import json
import os
import time
from pathlib import Path
from urllib.parse import quote
from urllib.request import Request, urlopen

import pandas as pd
import psycopg2

DB_CONFIG = {
    "host": "localhost",
    "dbname": "night_safe_walk",
    "user": "postgres",
    "password": "0000",
    "port": 5432,
}

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"
ENV_PATH = BASE_DIR / ".env"
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

CSV_PREFIX = "policestations_jeongug"
FACILITY_TYPE = "police_station"
WEIGHT_SCORE = 30

SIDO_COL = "시도청"
POLICE_COL = "경찰서"
OFFICE_COL = "관서명"
DIVISION_COL = "구분"
ADDRESS_COL = "주소"
LAT_COL = "위도"
LNG_COL = "경도"
GEOCODE_STATUS_COL = "geocode_status"
GEOCODE_SOURCE_COL = "geocode_source"
ADDRESS_USED_COL = "geocode_address"

AREA_KEYWORDS = ["청주시", "청주흥덕", "청주상당", "청주청원"]

NAVER_GEOCODE_URL = "https://maps.apigw.ntruss.com/map-geocode/v2/geocode"
NAVER_CLIENT_ID_ENV = "NAVER_MAP_CLIENT_ID"
NAVER_CLIENT_SECRET_ENV = "NAVER_MAP_CLIENT_SECRET"


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


def get_latest_csv(prefix: str) -> Path:
    files = sorted(RAW_DIR.glob(f"{prefix}_*.csv"))
    if not files:
        raise FileNotFoundError(f"{prefix} 파일을 찾을 수 없습니다.")
    return files[-1]


def get_processed_csv_path(csv_path: Path) -> Path:
    date_part = csv_path.stem.split("_")[-1]
    return PROCESSED_DIR / f"policestations_cheongju_processed_{date_part}.csv"


CSV_PATH = get_latest_csv(CSV_PREFIX)


def load_csv(csv_path: Path) -> pd.DataFrame:
    encodings = ["utf-8", "cp949", "euc-kr"]

    for enc in encodings:
        try:
            df = pd.read_csv(csv_path, encoding=enc)
            print(f"CSV 읽기 성공: {enc}")
            return df
        except Exception:
            continue

    raise ValueError("CSV 인코딩을 읽을 수 없습니다.")


def filter_target_area(df: pd.DataFrame) -> pd.DataFrame:
    required_cols = [SIDO_COL, POLICE_COL, OFFICE_COL, DIVISION_COL, ADDRESS_COL]
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise KeyError(f"필수 컬럼이 없습니다: {missing_cols}")

    police_series = df[POLICE_COL].fillna("").astype(str)
    office_series = df[OFFICE_COL].fillna("").astype(str)
    address_series = df[ADDRESS_COL].fillna("").astype(str)

    mask = False
    for keyword in AREA_KEYWORDS:
        keyword_mask = (
            police_series.str.contains(keyword, regex=False)
            | office_series.str.contains(keyword, regex=False)
            | address_series.str.contains(keyword, regex=False)
        )
        mask = mask | keyword_mask

    filtered = df[mask].copy()
    if filtered.empty:
        raise ValueError("청주 지역 경찰서 데이터가 없습니다. 필터 조건을 확인하세요.")

    return filtered


def geocode_address(
    address: str,
    client_id: str,
    client_secret: str,
) -> tuple[float | None, float | None]:
    encoded_query = quote(address)
    request = Request(
        f"{NAVER_GEOCODE_URL}?query={encoded_query}",
        headers={
            "x-ncp-apigw-api-key-id": client_id,
            "x-ncp-apigw-api-key": client_secret,
        },
    )

    with urlopen(request, timeout=10) as response:
        payload = json.loads(response.read().decode("utf-8"))

    addresses = payload.get("addresses", [])
    if not addresses:
        return None, None

    first = addresses[0]
    return float(first["y"]), float(first["x"])


def fill_coordinates_with_geocoding(df: pd.DataFrame) -> pd.DataFrame:
    load_local_env()

    client_id = os.getenv(NAVER_CLIENT_ID_ENV)
    client_secret = os.getenv(NAVER_CLIENT_SECRET_ENV)

    if not client_id or not client_secret:
        raise EnvironmentError(
            f"{NAVER_CLIENT_ID_ENV} and {NAVER_CLIENT_SECRET_ENV} must be set "
            "for police station geocoding."
        )

    address_cache: dict[str, tuple[float | None, float | None]] = {}
    latitudes = []
    longitudes = []
    statuses = []
    sources = []
    used_addresses = []
    total_rows = len(df)

    for index, (_, row) in enumerate(df.iterrows(), start=1):
        address = "" if pd.isna(row.get(ADDRESS_COL, "")) else str(row[ADDRESS_COL]).strip()
        used_addresses.append(address)
        sources.append("address")

        if not address:
            latitudes.append(None)
            longitudes.append(None)
            statuses.append("missing_address")
            continue

        if address not in address_cache:
            try:
                address_cache[address] = geocode_address(
                    address=address,
                    client_id=client_id,
                    client_secret=client_secret,
                )
                time.sleep(0.05)
            except Exception:
                address_cache[address] = (None, None)

        lat, lng = address_cache[address]
        latitudes.append(lat)
        longitudes.append(lng)
        statuses.append(
            "success" if lat is not None and lng is not None else "not_found"
        )

        if index % 20 == 0 or index == total_rows:
            success_count = statuses.count("success")
            missing_count = statuses.count("missing_address")
            not_found_count = statuses.count("not_found")
            print(
                f"[변환 진행률] {index}/{total_rows} "
                f"(성공: {success_count}, 주소없음: {missing_count}, 미일치: {not_found_count})"
            )

    df = df.copy()
    df[LAT_COL] = latitudes
    df[LNG_COL] = longitudes
    df[GEOCODE_STATUS_COL] = statuses
    df[GEOCODE_SOURCE_COL] = sources
    df[ADDRESS_USED_COL] = used_addresses
    return df


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    if LAT_COL not in df.columns or LNG_COL not in df.columns:
        raise KeyError(f"위도/경도 컬럼명을 확인하세요. 현재 컬럼: {list(df.columns)}")

    df[LAT_COL] = pd.to_numeric(df[LAT_COL], errors="coerce")
    df[LNG_COL] = pd.to_numeric(df[LNG_COL], errors="coerce")

    df = df.dropna(subset=[LAT_COL, LNG_COL]).copy()
    df = df[
        (df[LAT_COL] >= 33.0)
        & (df[LAT_COL] <= 39.5)
        & (df[LNG_COL] >= 124.0)
        & (df[LNG_COL] <= 132.0)
    ].copy()

    return df


def insert_data(df: pd.DataFrame):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    inserted_count = 0

    try:
        for _, row in df.iterrows():
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
                    FACILITY_TYPE,
                    WEIGHT_SCORE,
                    row[LNG_COL],
                    row[LAT_COL],
                ),
            )
            inserted_count += 1

        conn.commit()
        print(f"적재 완료: {inserted_count}건")

    except Exception as e:
        conn.rollback()
        print("적재 중 오류 발생:", e)
        raise

    finally:
        cur.close()
        conn.close()


def main():
    print("경찰서 CSV 적재 시작")
    print(f"원본 파일: {CSV_PATH}")

    df = load_csv(CSV_PATH)
    print(f"원본 행 수: {len(df)}")

    df = filter_target_area(df)
    print(f"청주 지역 필터 후 행 수: {len(df)}")

    df = fill_coordinates_with_geocoding(df)
    print(
        "지오코딩 상태 집계:\n"
        f"{df[GEOCODE_STATUS_COL].value_counts(dropna=False).to_string()}"
    )

    processed_path = get_processed_csv_path(CSV_PATH)
    df.to_csv(processed_path, index=False, encoding="utf-8-sig")
    print(f"processed CSV 저장 완료: {processed_path}")

    cleaned_df = clean_dataframe(df)
    print(f"좌표 정제 후 행 수: {len(cleaned_df)}")

    insert_data(cleaned_df)
    print("경찰서 적재 완료")


if __name__ == "__main__":
    main()
