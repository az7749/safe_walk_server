import argparse
import csv
import json
import os
import xml.etree.ElementTree as ET
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import psycopg2

DB_CONFIG = {
    "host": "localhost",
    "dbname": "night_safe_walk",
    "user": "postgres",
    "password": "0000",
    "port": 5432,
}

BASE_DIR = Path(__file__).resolve().parent.parent
ENV_PATH = BASE_DIR / ".env"
PROCESSED_DIR = BASE_DIR / "data" / "processed"
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

API_URL = "https://www.safe182.go.kr/api/lcm/safeMap.do"

SAFE182_ESNTL_ID_ENV = "SAFE182_ESNTL_ID"
SAFE182_AUTH_KEY_ENV = "SAFE182_AUTH_KEY"

DEFAULT_PAGE_UNIT = 100
DEFAULT_METHOD = "GET"

# Cheongju bounding box. API uses X=longitude, Y=latitude.
CHEONGJU_BBOX = {
    "minX": 127.25,
    "minY": 36.45,
    "maxX": 127.70,
    "maxY": 36.85,
}

CATEGORY_MAP = {
    "09": {
        "name": "아동안전지킴이집",
        "facility_type": "child_safety_house",
        "weight_score": 8,
        "insert_to_facilities": True,
    },
    "17": {
        "name": "청소년지원시설",
        "facility_type": "youth_support_center",
        "weight_score": 6,
        "insert_to_facilities": True,
    },
    "18": {
        "name": "원스톱지원센터",
        "facility_type": "one_stop_support_center",
        "weight_score": 10,
        "insert_to_facilities": True,
    },
    "20": {
        "name": "우범지역및공폐허가",
        "facility_type": "risk_area",
        "weight_score": 0,
        "insert_to_facilities": False,
    },
    "22": {
        "name": "아동보호시설",
        "facility_type": "child_protection_center",
        "weight_score": 8,
        "insert_to_facilities": True,
    },
    "23": {
        "name": "노인보호시설",
        "facility_type": "senior_protection_center",
        "weight_score": 5,
        "insert_to_facilities": True,
    },
}


def load_local_env() -> None:
    if not ENV_PATH.exists():
        return

    for line in ENV_PATH.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue

        key, value = stripped.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def get_required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise EnvironmentError(f".env에 {name} 값을 추가해야 합니다.")
    return value


def fetch_page(
    esntl_id: str,
    auth_key: str,
    category_codes: list[str],
    page_index: int,
    page_unit: int,
    method: str,
) -> dict:
    params: list[tuple[str, str | int | float]] = [
        ("esntlId", esntl_id),
        ("authKey", auth_key),
        ("pageIndex", page_index),
        ("pageUnit", page_unit),
        ("xmlUseYN", "N"),
        ("minX", CHEONGJU_BBOX["minX"]),
        ("minY", CHEONGJU_BBOX["minY"]),
        ("maxX", CHEONGJU_BBOX["maxX"]),
        ("maxY", CHEONGJU_BBOX["maxY"]),
    ]

    for category_code in category_codes:
        params.append(("clArray", category_code))

    encoded_params = urlencode(params)

    if method == "POST":
        request = Request(
            API_URL,
            data=encoded_params.encode("utf-8"),
            method="POST",
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "User-Agent": "Mozilla/5.0",
            },
        )
    else:
        request = Request(
            f"{API_URL}?{encoded_params}",
            method="GET",
            headers={"User-Agent": "Mozilla/5.0"},
        )

    with urlopen(request, timeout=30) as response:
        raw_payload = response.read()

    payload = raw_payload.decode("utf-8", errors="replace").strip()

    try:
        data = json.loads(payload)
    except json.JSONDecodeError:
        if payload.startswith("<"):
            root = ET.fromstring(payload)
            result = get_xml_text(root, ".//result")
            message = get_xml_text(root, ".//msg") or get_xml_text(root, ".//message")
            if result:
                raise RuntimeError(f"Safe182 API 오류: {result} {message}") from None

        preview = payload[:500].replace("\n", " ")
        raise RuntimeError(f"Safe182 API 응답을 JSON으로 읽을 수 없습니다. 응답 앞부분: {preview}") from None

    if str(data.get("result")) != "00":
        raise RuntimeError(f"Safe182 API 오류: {data.get('result')} {data.get('msg')}")

    return data


def get_xml_text(root: ET.Element, path: str) -> str:
    element = root.find(path)
    if element is None or element.text is None:
        return ""
    return element.text.strip()


def normalize_item(item: dict) -> dict | None:
    try:
        lat = float(item.get("lcinfoLa", ""))
        lng = float(item.get("lcinfoLo", ""))
    except ValueError:
        return None

    if not (33.0 <= lat <= 39.5 and 124.0 <= lng <= 132.0):
        return None

    address_text = " ".join(
        [
            str(item.get("adres", "")),
            str(item.get("etcAdres", "")),
            str(item.get("bsshNm", "")),
        ]
    )
    if "청주" not in address_text:
        return None

    category_code = str(item.get("cl", "")).zfill(2)
    category = CATEGORY_MAP.get(category_code, {})

    return {
        "일련번호": str(item.get("lcSn", "")).strip(),
        "시설명": str(item.get("bsshNm", "")).strip(),
        "분류코드": category_code,
        "분류명": str(item.get("clNm") or category.get("name", "")).strip(),
        "우편번호": str(item.get("zip", "")).strip(),
        "주소": str(item.get("adres", "")).strip(),
        "기타주소": str(item.get("etcAdres", "")).strip(),
        "전화번호": str(item.get("telno", "")).strip(),
        "위도": lat,
        "경도": lng,
        "시설물타입": category.get("facility_type", ""),
        "가중치": category.get("weight_score", 0),
    }


def fetch_all_items(
    category_codes: list[str],
    page_unit: int,
    max_pages: int | None,
    method: str,
) -> list[dict]:
    load_local_env()
    esntl_id = get_required_env(SAFE182_ESNTL_ID_ENV)
    auth_key = get_required_env(SAFE182_AUTH_KEY_ENV)

    all_items: list[dict] = []
    page_index = 1
    total_count = None

    while True:
        data = fetch_page(
            esntl_id=esntl_id,
            auth_key=auth_key,
            category_codes=category_codes,
            page_index=page_index,
            page_unit=page_unit,
            method=method,
        )

        if total_count is None:
            total_count = int(data.get("totalCount", 0))

        page_items = []
        for item in data.get("list", []):
            normalized = normalize_item(item)
            if normalized is not None:
                page_items.append(normalized)

        all_items.extend(page_items)
        print(f"{page_index}페이지 수집: {len(page_items)}건, 누적 {len(all_items)}건")

        if page_index * page_unit >= total_count:
            break
        if max_pages is not None and page_index >= max_pages:
            break

        page_index += 1

    return all_items


def get_processed_csv_path(category_codes: list[str]) -> Path:
    category_part = "_".join(category_codes)
    return PROCESSED_DIR / f"safe182_safety_map_cheongju_{category_part}.csv"


def save_processed_csv(rows: list[dict], category_codes: list[str]) -> Path:
    csv_path = get_processed_csv_path(category_codes)
    fieldnames = [
        "일련번호",
        "시설명",
        "분류코드",
        "분류명",
        "우편번호",
        "주소",
        "기타주소",
        "전화번호",
        "위도",
        "경도",
        "시설물타입",
        "가중치",
    ]

    with csv_path.open("w", encoding="utf-8-sig", newline="") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    return csv_path


def insert_data(rows: list[dict], replace: bool) -> int:
    insertable_rows = [row for row in rows if row["시설물타입"] and int(row["가중치"]) > 0]
    facility_types = sorted({row["시설물타입"] for row in insertable_rows})

    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    try:
        if replace and facility_types:
            cur.execute(
                "DELETE FROM safety_facilities WHERE TRIM(type) = ANY(%s)",
                (facility_types,),
            )

        inserted_count = 0
        for row in insertable_rows:
            cur.execute(
                """
                INSERT INTO safety_facilities (type, weight_score, geom)
                VALUES (
                    %s,
                    %s,
                    ST_SetSRID(ST_MakePoint(%s, %s), 4326)
                )
                """,
                (
                    row["시설물타입"],
                    int(row["가중치"]),
                    row["경도"],
                    row["위도"],
                ),
            )
            inserted_count += 1

        conn.commit()
        return inserted_count

    except Exception:
        conn.rollback()
        raise

    finally:
        cur.close()
        conn.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Safe182 안전지도 정보를 청주시 기준으로 수집하고 safety_facilities에 적재합니다."
    )
    parser.add_argument(
        "--categories",
        nargs="+",
        default=["09"],
        choices=sorted(CATEGORY_MAP.keys()),
        help="수집할 Safe182 분류코드입니다. 기본값은 아동안전지킴이집(09)입니다.",
    )
    parser.add_argument(
        "--page-unit",
        type=int,
        default=DEFAULT_PAGE_UNIT,
        help="API 한 페이지에서 요청할 데이터 개수입니다.",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=None,
        help="테스트용 최대 페이지 수입니다.",
    )
    parser.add_argument(
        "--method",
        choices=["GET", "POST"],
        default=DEFAULT_METHOD,
        help="API 호출 방식입니다. 기본값은 문서 예시와 맞춘 GET입니다.",
    )
    parser.add_argument(
        "--replace",
        action="store_true",
        help="같은 시설물 타입의 기존 데이터를 삭제한 뒤 새로 적재합니다.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="CSV만 저장하고 DB에는 적재하지 않습니다.",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    print("Safe182 안전지도 데이터 수집을 시작합니다.")
    print(f"분류코드: {args.categories}")
    print(f"페이지당 요청 건수: {args.page_unit}")
    print(f"API 호출 방식: {args.method}")
    print(f"DB 적재 생략: {args.dry_run}")

    rows = fetch_all_items(
        category_codes=args.categories,
        page_unit=args.page_unit,
        max_pages=args.max_pages,
        method=args.method,
    )
    print(f"수집 완료: {len(rows)}건")

    csv_path = save_processed_csv(rows, category_codes=args.categories)
    print(f"가공 CSV 저장 완료: {csv_path}")

    if args.dry_run:
        print("DB 적재 생략 모드로 완료했습니다.")
        return

    inserted_count = insert_data(rows, replace=args.replace)
    print(f"DB 적재 완료: {inserted_count}건")


if __name__ == "__main__":
    main()
