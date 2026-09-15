import csv
from pathlib import Path

import psycopg2

DB_CONFIG = {
    "host": "localhost",
    "dbname": "night_safe_walk",
    "user": "postgres",
    "password": "0000",
    "port": 5432,
}

BASE_DIR = Path(__file__).resolve().parent.parent
RAW_DIR = BASE_DIR / "data" / "raw"
PROCESSED_DIR = BASE_DIR / "data" / "processed"
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

CSV_PATH = RAW_DIR / "fire_stations_korea_20240901.csv"
PROCESSED_CSV_PATH = PROCESSED_DIR / "fire_stations_cheongju_processed_20240901.csv"

FACILITY_TYPE = "fire_station"
WEIGHT_SCORE = 15

NAME_COL = "소방서 및 안전센터명"
ADDRESS_COL = "주소"
HEADQUARTERS_COL = "상위 본부명"
PHONE_COL = "전화번호"
LAT_COL = "X좌표"
LNG_COL = "Y좌표"
TYPE_COL = "유형"
REGISTERED_AT_COL = "등록일"


def load_csv(csv_path: Path) -> list[dict]:
    for encoding in ("utf-8-sig", "cp949", "euc-kr"):
        try:
            with csv_path.open(encoding=encoding, newline="") as csv_file:
                rows = list(csv.DictReader(csv_file))
            print(f"CSV 읽기 성공: {encoding}")
            return rows
        except UnicodeDecodeError:
            continue

    raise ValueError("CSV 인코딩을 확인할 수 없습니다.")


def is_cheongju_row(row: dict) -> bool:
    text = " ".join(
        [
            row.get(NAME_COL, ""),
            row.get(ADDRESS_COL, ""),
            row.get(HEADQUARTERS_COL, ""),
        ]
    )
    return "청주" in text


def clean_rows(rows: list[dict]) -> list[dict]:
    cleaned_rows = []

    for row in rows:
        if not is_cheongju_row(row):
            continue

        try:
            lat = float(row.get(LAT_COL, ""))
            lng = float(row.get(LNG_COL, ""))
        except ValueError:
            continue

        if not (33.0 <= lat <= 39.5 and 124.0 <= lng <= 132.0):
            continue

        cleaned_rows.append(
            {
                "시설명": row.get(NAME_COL, "").strip(),
                "주소": row.get(ADDRESS_COL, "").strip(),
                "상위본부명": row.get(HEADQUARTERS_COL, "").strip(),
                "전화번호": row.get(PHONE_COL, "").strip(),
                "유형": row.get(TYPE_COL, "").strip(),
                "등록일": row.get(REGISTERED_AT_COL, "").strip(),
                "위도": lat,
                "경도": lng,
            }
        )

    return cleaned_rows


def save_processed_csv(rows: list[dict]) -> None:
    fieldnames = ["시설명", "주소", "상위본부명", "전화번호", "유형", "등록일", "위도", "경도"]

    with PROCESSED_CSV_PATH.open("w", encoding="utf-8-sig", newline="") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"가공 CSV 저장 완료: {PROCESSED_CSV_PATH}")


def insert_data(rows: list[dict], replace: bool) -> int:
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    try:
        if replace:
            cur.execute(
                "DELETE FROM safety_facilities WHERE TRIM(type) = %s",
                (FACILITY_TYPE,),
            )

        inserted_count = 0
        for row in rows:
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


def main():
    print("소방서/119안전센터 데이터 처리를 시작합니다.")
    rows = load_csv(CSV_PATH)
    print(f"원본 데이터: {len(rows)}건")

    cleaned_rows = clean_rows(rows)
    print(f"청주시 좌표 유효 데이터: {len(cleaned_rows)}건")

    save_processed_csv(cleaned_rows)

    inserted_count = insert_data(cleaned_rows, replace=True)
    print(f"DB 적재 완료: {inserted_count}건")


if __name__ == "__main__":
    main()
