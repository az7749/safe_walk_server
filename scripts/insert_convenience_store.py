import argparse
import csv
import math
import time
import xml.etree.ElementTree as ET
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import urlopen

import psycopg2

DB_CONFIG = {
    "host": "localhost",
    "dbname": "night_safe_walk",
    "user": "postgres",
    "password": "0000",
    "port": 5432,
}

BASE_DIR = Path(__file__).resolve().parent.parent
PROCESSED_DIR = BASE_DIR / "data" / "processed"
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

SERVICE_KEY = "***REMOVED***"
BASE_URL = "https://www.safemap.go.kr/openapi2/IF_0039"

FACILITY_TYPE = "convenience_store"
WEIGHT_SCORE = 2

DEFAULT_NUM_OF_ROWS = 1000
REQUEST_INTERVAL_SECONDS = 0.1

CHEONGJU_SGG_CODES = {"43111", "43112", "43113", "43114"}


def get_text(parent: ET.Element | None, tag_name: str) -> str:
    if parent is None:
        return ""

    element = parent.find(tag_name)
    if element is None or element.text is None:
        return ""

    return element.text.strip()


def web_mercator_to_wgs84(x: float, y: float) -> tuple[float, float]:
    lng = (x / 20037508.34) * 180
    lat = (y / 20037508.34) * 180
    lat = (180 / math.pi) * (
        2 * math.atan(math.exp(lat * math.pi / 180)) - math.pi / 2
    )
    return lng, lat


def fetch_page(page_no: int, num_of_rows: int) -> ET.Element:
    params = {
        "serviceKey": SERVICE_KEY,
        "pageNo": page_no,
        "numOfRows": num_of_rows,
        "returnType": "xml",
    }
    url = f"{BASE_URL}?{urlencode(params)}"

    with urlopen(url, timeout=30) as response:
        xml_text = response.read().decode("utf-8")

    root = ET.fromstring(xml_text)
    result_code = get_text(root.find("header"), "resultCode")

    if result_code != "00":
        result_msg = get_text(root.find("header"), "resultMsg")
        raise RuntimeError(f"생활안전지도 API 오류: {result_code} {result_msg}")

    return root


def parse_total_count(root: ET.Element) -> int:
    total_count_text = get_text(root.find("body"), "totalCount")
    if not total_count_text:
        return 0

    return int(total_count_text)


def is_cheongju_item(item: ET.Element) -> bool:
    return get_text(item, "sgg_cd") in CHEONGJU_SGG_CODES


def parse_items(root: ET.Element, only_cheongju: bool) -> list[dict]:
    parsed_items = []

    for item in root.findall(".//item"):
        if only_cheongju and not is_cheongju_item(item):
            continue

        try:
            x = float(get_text(item, "x"))
            y = float(get_text(item, "y"))
        except ValueError:
            continue

        if x == 0 or y == 0:
            continue

        lng, lat = web_mercator_to_wgs84(x, y)

        parsed_items.append(
            {
                "objt_id": get_text(item, "objt_id"),
                "fclty_cd": get_text(item, "fclty_cd"),
                "data_yr": get_text(item, "data_yr"),
                "fclty_nm": get_text(item, "fclty_nm"),
                "telno": get_text(item, "telno"),
                "adres": get_text(item, "adres"),
                "rn_adres": get_text(item, "rn_adres"),
                "ctprvn_cd": get_text(item, "ctprvn_cd"),
                "sgg_cd": get_text(item, "sgg_cd"),
                "emd_cd": get_text(item, "emd_cd"),
                "fclty_ty": get_text(item, "fclty_ty"),
                "x": x,
                "y": y,
                "lng": lng,
                "lat": lat,
            }
        )

    return parsed_items


def fetch_all_items(
    num_of_rows: int,
    max_pages: int | None,
    only_cheongju: bool,
) -> list[dict]:
    first_root = fetch_page(page_no=1, num_of_rows=num_of_rows)
    total_count = parse_total_count(first_root)
    total_pages = math.ceil(total_count / num_of_rows) if total_count else 1

    if max_pages is not None:
        total_pages = min(total_pages, max_pages)

    all_items = parse_items(first_root, only_cheongju=only_cheongju)
    print(f"1/{total_pages} 페이지 수집: 유효 데이터 {len(all_items)}건")

    for page_no in range(2, total_pages + 1):
        time.sleep(REQUEST_INTERVAL_SECONDS)
        root = fetch_page(page_no=page_no, num_of_rows=num_of_rows)
        page_items = parse_items(root, only_cheongju=only_cheongju)
        all_items.extend(page_items)
        print(f"{page_no}/{total_pages} 페이지 수집: {len(page_items)}건, 누적 {len(all_items)}건")

    return all_items


def get_processed_csv_path(items: list[dict], only_cheongju: bool) -> Path:
    data_years = sorted({item["data_yr"] for item in items if item["data_yr"]})
    data_year = data_years[-1] if data_years else "unknown"
    area = "cheongju" if only_cheongju else "nationwide"
    return PROCESSED_DIR / f"convenience_store_{area}_processed_{data_year}.csv"


def save_processed_csv(items: list[dict], only_cheongju: bool) -> Path:
    csv_path = get_processed_csv_path(items, only_cheongju=only_cheongju)
    csv_columns = [
        ("objt_id", "일련번호"),
        ("fclty_cd", "시설코드"),
        ("data_yr", "데이터연도"),
        ("fclty_nm", "시설명"),
        ("fclty_ty", "시설유형"),
        ("telno", "전화번호"),
        ("adres", "주소"),
        ("rn_adres", "도로명주소"),
        ("ctprvn_cd", "시도코드"),
        ("sgg_cd", "시군구코드"),
        ("emd_cd", "읍면동코드"),
        ("x", "원본X좌표"),
        ("y", "원본Y좌표"),
        ("lng", "경도"),
        ("lat", "위도"),
    ]
    fieldnames = [label for _, label in csv_columns]

    with csv_path.open("w", newline="", encoding="utf-8-sig") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        for item in items:
            writer.writerow({label: item[key] for key, label in csv_columns})

    return csv_path


def insert_data(items: list[dict], replace: bool) -> int:
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    try:
        if replace:
            cur.execute(
                "DELETE FROM safety_facilities WHERE TRIM(type) = %s",
                (FACILITY_TYPE,),
            )

        inserted_count = 0
        for item in items:
            cur.execute(
                """
                INSERT INTO safety_facilities (type, weight_score, geom)
                VALUES (
                    %s,
                    %s,
                    ST_Transform(
                        ST_SetSRID(ST_MakePoint(%s, %s), 3857),
                        4326
                    )
                )
                """,
                (
                    FACILITY_TYPE,
                    WEIGHT_SCORE,
                    item["x"],
                    item["y"],
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
        description="생활안전지도 편의점 데이터를 CSV로 저장하고 safety_facilities에 적재합니다."
    )
    parser.add_argument(
        "--num-of-rows",
        type=int,
        default=DEFAULT_NUM_OF_ROWS,
        help="API 한 페이지에서 요청할 데이터 개수입니다.",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=None,
        help="테스트용으로 수집할 최대 페이지 수를 제한합니다.",
    )
    parser.add_argument(
        "--cheongju-only",
        action="store_true",
        help="시군구코드 기준으로 청주시 데이터만 적재합니다. 기본값입니다.",
    )
    parser.add_argument(
        "--nationwide",
        action="store_true",
        help="전국 데이터를 대상으로 합니다. 실수 방지를 위해 --confirm-nationwide도 함께 필요합니다.",
    )
    parser.add_argument(
        "--confirm-nationwide",
        action="store_true",
        help="전국 데이터 적재를 명시적으로 확인합니다.",
    )
    parser.add_argument(
        "--replace",
        action="store_true",
        help="기존 편의점 데이터를 삭제한 뒤 새로 적재합니다.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="API 수집과 CSV 저장만 하고 DB에는 적재하지 않습니다.",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    only_cheongju = not args.nationwide

    if args.nationwide and not args.confirm_nationwide:
        raise RuntimeError(
            "전국 데이터는 5만 건 이상일 수 있습니다. "
            "정말 전국으로 실행하려면 --nationwide --confirm-nationwide를 함께 사용하세요."
        )

    print("생활안전지도 편의점 데이터 적재를 시작합니다.")
    print(f"페이지당 요청 건수: {args.num_of_rows}")
    print(f"최대 페이지 수: {args.max_pages}")
    print(f"청주시만 적재: {only_cheongju}")
    print(f"기존 데이터 교체: {args.replace}")
    print(f"DB 적재 생략: {args.dry_run}")

    items = fetch_all_items(
        num_of_rows=args.num_of_rows,
        max_pages=args.max_pages,
        only_cheongju=only_cheongju,
    )
    print(f"파싱 완료: {len(items)}건")

    csv_path = save_processed_csv(items, only_cheongju=only_cheongju)
    print(f"가공 CSV 저장 완료: {csv_path}")

    if args.dry_run:
        print("DB 적재 생략 모드로 완료했습니다.")
        return

    inserted_count = insert_data(items, replace=args.replace)
    print(f"DB 적재 완료: {inserted_count}건")


if __name__ == "__main__":
    main()
