import pandas as pd
import psycopg2
from pathlib import Path

DB_CONFIG = {
    "host": "localhost",
    "dbname": "night_safe_walk",
    "user": "postgres",
    "password": "0000",
    "port": 5432,
}

DATA_DIR = Path(__file__).resolve().parent.parent / "data" / "raw"

CSV_PREFIX = "cctv_chungbuk"
FACILITY_TYPE = "cctv"
WEIGHT_SCORE = 4

LAT_COL = "la"
LNG_COL = "lo"
AREA_FILTER_COLS = ["administ", "instl_dtl"]
AREA_KEYWORDS = ["청주", "청주시"]


def get_latest_csv(prefix: str) -> Path:
    files = sorted(DATA_DIR.glob(f"{prefix}_*.csv"))
    if not files:
        raise FileNotFoundError(f"No CSV file found for prefix: {prefix}")
    return files[-1]


CSV_PATH = get_latest_csv(CSV_PREFIX)


def load_csv(csv_path: Path) -> pd.DataFrame:
    encodings = ["utf-8", "cp949", "euc-kr"]

    for enc in encodings:
        try:
            df = pd.read_csv(csv_path, encoding=enc)
            print(f"CSV loaded with encoding: {enc}")
            return df
        except Exception:
            continue

    raise ValueError("Could not read the CSV file with supported encodings.")


def filter_target_area(df: pd.DataFrame) -> pd.DataFrame:
    missing_cols = [col for col in AREA_FILTER_COLS if col not in df.columns]
    if missing_cols:
        raise KeyError(f"Missing area filter columns: {missing_cols}")

    mask = False
    for col in AREA_FILTER_COLS:
        col_mask = df[col].fillna("").astype(str).str.contains(
            "|".join(AREA_KEYWORDS),
            regex=True,
        )
        mask = mask | col_mask

    filtered = df[mask].copy()
    if filtered.empty:
        raise ValueError(
            "No rows matched the target area keywords. "
            "Check whether the raw CCTV file contains Cheongju data."
        )

    return filtered


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    if LAT_COL not in df.columns or LNG_COL not in df.columns:
        raise KeyError(
            f"Latitude/longitude columns not found. Current columns: {list(df.columns)}"
        )

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
        print(f"Insert completed: {inserted_count} rows")

    except Exception as e:
        conn.rollback()
        print("Insert failed:", e)
        raise

    finally:
        cur.close()
        conn.close()


def main():
    print("CCTV CSV import started")
    print(f"Source file: {CSV_PATH}")

    df = load_csv(CSV_PATH)
    print(f"Original rows: {len(df)}")

    df = filter_target_area(df)
    print(f"Rows after area filter: {len(df)}")

    df = clean_dataframe(df)
    print(f"Rows after coordinate cleanup: {len(df)}")

    insert_data(df)
    print("Done")


if __name__ == "__main__":
    main()
