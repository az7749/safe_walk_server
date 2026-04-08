import pandas as pd
import psycopg2
from pathlib import Path

DB_CONFIG = {
    # "host": "localhost",
    'host': '192.168.35.20',
    "dbname": "night_safe_walk",
    "user": "postgres",
    "password": "0000",
    "port": 5432,
}

DATA_DIR = Path(__file__).resolve().parent.parent / "data"
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"

PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

def get_processed_csv_path(csv_path: Path) -> Path:
    date_part = csv_path.stem.split("_")[-1]
    return PROCESSED_DIR / f"securitylight_cheongju_processed_{date_part}.csv"

def get_latest_csv(prefix: str) -> Path:
    files = sorted(RAW_DIR.glob(f"{prefix}_*.csv"))
    if not files:
        raise FileNotFoundError(f"{prefix} 파일이 없습니다.")
    return files[-1]

CSV_PATH = get_latest_csv("securitylight_cheongju")
print(CSV_PATH)

FACILITY_TYPE = "security_light"
WEIGHT_SCORE = 3

LAT_COL = "위도"
LNG_COL = "경도"

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

def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    if LAT_COL not in df.columns or LNG_COL not in df.columns:
        raise KeyError(f"위도/경도 컬럼명을 확인하세요. 현재 컬럼: {list(df.columns)}")

    df[LAT_COL] = pd.to_numeric(df[LAT_COL], errors="coerce")
    df[LNG_COL] = pd.to_numeric(df[LNG_COL], errors="coerce")

    # 위도/경도 없는 행 제거
    df = df.dropna(subset=[LAT_COL, LNG_COL]).copy()

    # 한국 범위 대충 필터
    df = df[
        (df[LAT_COL] >= 33.0) & (df[LAT_COL] <= 39.5) &
        (df[LNG_COL] >= 124.0) & (df[LNG_COL] <= 132.0)
    ].copy()

    return df


def insert_data(df: pd.DataFrame):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    inserted_count = 0

    try:
        for _, row in df.iterrows():
            cur.execute("""
                INSERT INTO safety_facilities (type, weight_score, geom)
                VALUES (
                    %s,
                    %s,
                    ST_SetSRID(ST_MakePoint(%s, %s), 4326)
                )
            """, (
                FACILITY_TYPE,
                WEIGHT_SCORE,
                row[LNG_COL],   # 경도
                row[LAT_COL],   # 위도
            ))
            inserted_count += 1

        conn.commit()
        print(f"삽입 완료: {inserted_count}건")

    except Exception as e:
        conn.rollback()
        print("삽입 중 오류 발생:", e)
        raise

    finally:
        cur.close()
        conn.close()


def main():
    print("보안등 CSV 적재 시작")
    print(f"사용 파일: {CSV_PATH}")

    df = load_csv(CSV_PATH)
    print(f"원본 행 수: {len(df)}")

    df = clean_dataframe(df)
    print(f"정제 후 행 수: {len(df)}")

    processed_path = get_processed_csv_path(CSV_PATH)
    df.to_csv(processed_path, index=False, encoding="utf-8-sig")
    print(f"가공 완료 CSV 저장: {processed_path}")

    insert_data(df)
    print("작업 완료")


if __name__ == "__main__":
    main()