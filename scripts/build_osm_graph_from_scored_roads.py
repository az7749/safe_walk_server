import psycopg2

DB_CONFIG = {
    "host": "localhost",
    "dbname": "night_safe_walk",
    "user": "postgres",
    "password": "0000",
    "port": 5432,
}

SOURCE_ROAD_TABLE = "road_edges_cheongju"
NODE_TABLE = "osm_nodes"
EDGE_TABLE = "osm_edges"
NODE_PRECISION_M = 0.01


def log(message: str) -> None:
    print(message, flush=True)


def rebuild_nodes(cur) -> None:
    cur.execute(f"DELETE FROM {EDGE_TABLE};")
    cur.execute(f"DELETE FROM {NODE_TABLE};")

    cur.execute(f"""
        WITH endpoint_keys AS (
            SELECT
                ROUND(ST_X(point_geom) / {NODE_PRECISION_M}) * {NODE_PRECISION_M} AS x,
                ROUND(ST_Y(point_geom) / {NODE_PRECISION_M}) * {NODE_PRECISION_M} AS y
            FROM (
                SELECT ST_StartPoint(geom) AS point_geom
                FROM {SOURCE_ROAD_TABLE}
                WHERE geom IS NOT NULL
                UNION ALL
                SELECT ST_EndPoint(geom) AS point_geom
                FROM {SOURCE_ROAD_TABLE}
                WHERE geom IS NOT NULL
            ) AS endpoints
            GROUP BY
                ROUND(ST_X(point_geom) / {NODE_PRECISION_M}) * {NODE_PRECISION_M},
                ROUND(ST_Y(point_geom) / {NODE_PRECISION_M}) * {NODE_PRECISION_M}
        ),
        numbered_nodes AS (
            SELECT
                ROW_NUMBER() OVER (ORDER BY x, y) AS node_id,
                ST_Transform(ST_SetSRID(ST_MakePoint(x, y), 3857), 4326) AS geom
            FROM endpoint_keys
        )
        INSERT INTO {NODE_TABLE} (node_id, geom)
        SELECT node_id, geom
        FROM numbered_nodes;
    """)
    log(f"inserted nodes: {cur.rowcount}")


def rebuild_edges(cur) -> None:
    cur.execute(f"""
        WITH node_lookup AS (
            SELECT
                node_id,
                ROUND(ST_X(ST_Transform(geom, 3857)) / {NODE_PRECISION_M}) * {NODE_PRECISION_M} AS x,
                ROUND(ST_Y(ST_Transform(geom, 3857)) / {NODE_PRECISION_M}) * {NODE_PRECISION_M} AS y
            FROM {NODE_TABLE}
        ),
        road_keys AS (
            SELECT
                road.edge_id,
                road.length_m,
                ST_Transform(road.geom, 4326) AS geom,
                COALESCE(road.safety_score, 0) AS safety_score,
                COALESCE(road.cost, road.length_m) AS cost,
                ROUND(ST_X(ST_StartPoint(road.geom)) / {NODE_PRECISION_M}) * {NODE_PRECISION_M} AS source_x,
                ROUND(ST_Y(ST_StartPoint(road.geom)) / {NODE_PRECISION_M}) * {NODE_PRECISION_M} AS source_y,
                ROUND(ST_X(ST_EndPoint(road.geom)) / {NODE_PRECISION_M}) * {NODE_PRECISION_M} AS target_x,
                ROUND(ST_Y(ST_EndPoint(road.geom)) / {NODE_PRECISION_M}) * {NODE_PRECISION_M} AS target_y
            FROM {SOURCE_ROAD_TABLE} AS road
            WHERE road.geom IS NOT NULL
        )
        INSERT INTO {EDGE_TABLE} (
            edge_id,
            source_node_id,
            target_node_id,
            length_m,
            geom,
            safety_score,
            cost
        )
        SELECT
            road.edge_id,
            source_node.node_id AS source_node_id,
            target_node.node_id AS target_node_id,
            road.length_m,
            road.geom,
            road.safety_score,
            road.cost
        FROM road_keys AS road
        JOIN node_lookup AS source_node
          ON source_node.x = road.source_x
         AND source_node.y = road.source_y
        JOIN node_lookup AS target_node
          ON target_node.x = road.target_x
         AND target_node.y = road.target_y;
    """)
    log(f"inserted edges: {cur.rowcount}")


def create_indexes(cur) -> None:
    cur.execute(f"CREATE INDEX IF NOT EXISTS {NODE_TABLE}_geom_idx ON {NODE_TABLE} USING GIST (geom);")
    cur.execute(f"CREATE INDEX IF NOT EXISTS {EDGE_TABLE}_geom_idx ON {EDGE_TABLE} USING GIST (geom);")
    cur.execute(f"CREATE INDEX IF NOT EXISTS {EDGE_TABLE}_source_node_idx ON {EDGE_TABLE} (source_node_id);")
    cur.execute(f"CREATE INDEX IF NOT EXISTS {EDGE_TABLE}_target_node_idx ON {EDGE_TABLE} (target_node_id);")
    cur.execute(f"ANALYZE {NODE_TABLE};")
    cur.execute(f"ANALYZE {EDGE_TABLE};")


def print_summary(cur) -> None:
    cur.execute(f"SELECT COUNT(*) FROM {NODE_TABLE};")
    node_count = cur.fetchone()[0]
    cur.execute(f"SELECT COUNT(*) FROM {EDGE_TABLE};")
    edge_count = cur.fetchone()[0]
    cur.execute(f"SELECT MIN(safety_score), MAX(safety_score), AVG(safety_score), MIN(cost), MAX(cost), AVG(cost) FROM {EDGE_TABLE};")
    min_score, max_score, avg_score, min_cost, max_cost, avg_cost = cur.fetchone()

    log(f"node_count={node_count}")
    log(f"edge_count={edge_count}")
    log(f"safety_score min={min_score}, max={max_score}, avg={avg_score:.2f}")
    log(f"cost min={min_cost:.2f}, max={max_cost:.2f}, avg={avg_cost:.2f}")


def main() -> None:
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    try:
        log("rebuilding osm graph from scored roads")
        rebuild_nodes(cur)
        rebuild_edges(cur)
        create_indexes(cur)
        conn.commit()
        log("osm graph rebuild completed")
        print_summary(cur)

    except Exception as e:
        conn.rollback()
        log(f"osm graph rebuild failed: {e}")
        raise

    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
