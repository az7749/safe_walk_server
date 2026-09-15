import time

import psycopg2

DB_CONFIG = {
    "host": "localhost",
    "dbname": "night_safe_walk",
    "user": "postgres",
    "password": "0000",
    "port": 5432,
}

ROAD_TABLE = "road_edges_cheongju"
BASE_SCORE = 40
FACILITY_SCORE_CAP = 45
FACILITY_RADIUS_M = 50

FACILITY_TYPES = (
    "street_light",
    "security_light",
    "police_station",
    "convenience_store",
    "fire_station",
    "child_safety_house",
)


def log(message: str) -> None:
    print(message, flush=True)


def run_step(label: str, func) -> None:
    started_at = time.time()
    log(f"[start] {label}")
    func()
    elapsed = time.time() - started_at
    log(f"[done] {label} ({elapsed:.1f}s)")


def ensure_score_columns(cur) -> None:
    cur.execute(f"ALTER TABLE {ROAD_TABLE} ADD COLUMN IF NOT EXISTS edge_id BIGSERIAL;")
    cur.execute(f"ALTER TABLE {ROAD_TABLE} ADD COLUMN IF NOT EXISTS length_m DOUBLE PRECISION;")
    cur.execute(f"ALTER TABLE {ROAD_TABLE} ADD COLUMN IF NOT EXISTS safety_score INTEGER;")
    cur.execute(f"ALTER TABLE {ROAD_TABLE} ADD COLUMN IF NOT EXISTS facility_score DOUBLE PRECISION;")
    cur.execute(f"ALTER TABLE {ROAD_TABLE} ADD COLUMN IF NOT EXISTS crime_penalty DOUBLE PRECISION;")
    cur.execute(f"ALTER TABLE {ROAD_TABLE} ADD COLUMN IF NOT EXISTS cost DOUBLE PRECISION;")


def update_length(cur) -> None:
    cur.execute(f"""
        UPDATE {ROAD_TABLE}
        SET length_m = ST_Length(geom)
        WHERE length_m IS NULL;
    """)


def create_spatial_indexes(cur) -> None:
    cur.execute(f"""
        CREATE INDEX IF NOT EXISTS {ROAD_TABLE}_geom_idx
        ON {ROAD_TABLE}
        USING GIST (geom);
    """)
    cur.execute(f"ANALYZE {ROAD_TABLE};")


def create_tmp_facilities(cur) -> None:
    cur.execute("DROP TABLE IF EXISTS tmp_safety_facilities_3857;")
    cur.execute("""
        CREATE TEMP TABLE tmp_safety_facilities_3857 AS
        SELECT
            facility_id,
            TRIM(type) AS type,
            weight_score,
            ST_Transform(geom, 3857) AS geom
        FROM safety_facilities
        WHERE TRIM(type) = ANY(%s);
    """, (list(FACILITY_TYPES),))
    cur.execute("""
        CREATE INDEX tmp_safety_facilities_3857_geom_idx
        ON tmp_safety_facilities_3857
        USING GIST (geom);
    """)
    cur.execute("ANALYZE tmp_safety_facilities_3857;")


def update_facility_score(cur) -> None:
    cur.execute(f"""
        UPDATE {ROAD_TABLE} AS road
        SET facility_score = COALESCE(score_data.score, 0),
            safety_score = LEAST(
                100,
                {BASE_SCORE}
                + LEAST({FACILITY_SCORE_CAP}, COALESCE(score_data.score, 0))
                - COALESCE(crime_penalty, 0)
            )::integer
        FROM (
            SELECT
                road.edge_id,
                SUM(facility.weight_score) AS score
            FROM {ROAD_TABLE} AS road
            LEFT JOIN tmp_safety_facilities_3857 AS facility
              ON ST_DWithin(
                road.geom,
                facility.geom,
                {FACILITY_RADIUS_M}
              )
            GROUP BY road.edge_id
        ) AS score_data
        WHERE road.edge_id = score_data.edge_id;
    """)


def update_cost(cur) -> None:
    cur.execute(f"""
        UPDATE {ROAD_TABLE}
        SET cost = length_m * (1 + ((100 - safety_score)::double precision / 100));
    """)


def print_score_summary(cur) -> None:
    cur.execute(f"""
        SELECT
            COUNT(*),
            MIN(safety_score),
            MAX(safety_score),
            AVG(safety_score),
            MIN(facility_score),
            MAX(facility_score),
            AVG(facility_score)
        FROM {ROAD_TABLE};
    """)
    (
        count,
        min_score,
        max_score,
        avg_score,
        min_facility_score,
        max_facility_score,
        avg_facility_score,
    ) = cur.fetchone()

    log(f"road_count={count}")
    log(f"safety_score min={min_score}, max={max_score}, avg={avg_score:.2f}")
    log(
        "facility_score "
        f"min={min_facility_score:.2f}, max={max_facility_score:.2f}, avg={avg_facility_score:.2f}"
    )


def print_grade_summary(cur) -> None:
    cur.execute(f"""
        SELECT
            CASE
                WHEN safety_score < 40 THEN 'danger'
                WHEN safety_score < 60 THEN 'normal'
                WHEN safety_score < 80 THEN 'safe'
                ELSE 'very_safe'
            END AS grade,
            COUNT(*)
        FROM {ROAD_TABLE}
        GROUP BY grade
        ORDER BY
            CASE
                WHEN CASE
                    WHEN safety_score < 40 THEN 'danger'
                    WHEN safety_score < 60 THEN 'normal'
                    WHEN safety_score < 80 THEN 'safe'
                    ELSE 'very_safe'
                END = 'danger' THEN 1
                WHEN CASE
                    WHEN safety_score < 40 THEN 'danger'
                    WHEN safety_score < 60 THEN 'normal'
                    WHEN safety_score < 80 THEN 'safe'
                    ELSE 'very_safe'
                END = 'normal' THEN 2
                WHEN CASE
                    WHEN safety_score < 40 THEN 'danger'
                    WHEN safety_score < 60 THEN 'normal'
                    WHEN safety_score < 80 THEN 'safe'
                    ELSE 'very_safe'
                END = 'safe' THEN 3
                ELSE 4
            END;
    """)

    log("grade_summary:")
    for grade, count in cur.fetchall():
        log(f"- {grade}: {count}")


def main() -> None:
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    try:
        run_step("ensure columns", lambda: ensure_score_columns(cur))
        run_step("update road length", lambda: update_length(cur))
        run_step("create road index", lambda: create_spatial_indexes(cur))
        run_step("create temp facility table", lambda: create_tmp_facilities(cur))
        run_step("update facility score", lambda: update_facility_score(cur))
        run_step("update cost", lambda: update_cost(cur))
        conn.commit()

        log("road safety score calculation completed")
        print_score_summary(cur)
        print_grade_summary(cur)

    except Exception as e:
        conn.rollback()
        log(f"calculation failed: {e}")
        raise

    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
