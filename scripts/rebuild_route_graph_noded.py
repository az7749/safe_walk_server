import psycopg2

DB_CONFIG = {
    "host": "localhost",
    "dbname": "night_safe_walk",
    "user": "postgres",
    "password": "0000",
    "port": 5432,
}

SOURCE_ROAD_TABLE = "road_edges_cheongju"
NODE_TABLE = "route_nodes_noded"
EDGE_TABLE = "route_edges_noded"
NODE_PRECISION_M = 0.01
SAFETY_COST_WEIGHT = 0.3


def log(message):
    print(message, flush=True)


def rebuild_noded_edges(cur):
    cur.execute(f"DROP TABLE IF EXISTS {EDGE_TABLE};")

    cur.execute(f"""
        CREATE TABLE {EDGE_TABLE} AS
        WITH merged AS (
            SELECT ST_Node(ST_UnaryUnion(ST_Collect(geom))) AS geom
            FROM {SOURCE_ROAD_TABLE}
            WHERE geom IS NOT NULL
        ),
        dumped AS (
            SELECT (ST_Dump(geom)).geom AS geom
            FROM merged
        ),
        valid_segments AS (
            SELECT geom
            FROM dumped
            WHERE ST_GeometryType(geom) = 'ST_LineString'
              AND ST_NPoints(geom) >= 2
              AND ST_Length(ST_Transform(geom, 3857)) >= 0.5
        ),
        scored_segments AS (
            SELECT
                ROW_NUMBER() OVER ()::bigint AS edge_id,
                segment.geom,
                ST_Length(ST_Transform(segment.geom, 3857)) AS length_m,
                COALESCE(nearest.safety_score, 40) AS safety_score
            FROM valid_segments AS segment
            JOIN LATERAL (
                SELECT road.safety_score
                FROM {SOURCE_ROAD_TABLE} AS road
                ORDER BY road.geom <-> ST_PointOnSurface(segment.geom)
                LIMIT 1
            ) AS nearest ON TRUE
        )
        SELECT
            edge_id,
            NULL::bigint AS source_node_id,
            NULL::bigint AS target_node_id,
            length_m,
            geom,
            safety_score,
            length_m * (
                1 + (((100 - safety_score)::double precision / 100) * {SAFETY_COST_WEIGHT})
            ) AS cost
        FROM scored_segments;
    """)

    log(f"created {EDGE_TABLE}: {cur.rowcount}")


def rebuild_nodes(cur):
    cur.execute(f"DROP TABLE IF EXISTS {NODE_TABLE};")

    cur.execute(f"""
        CREATE TABLE {NODE_TABLE} AS
        WITH endpoints AS (
            SELECT ST_StartPoint(geom) AS geom
            FROM {EDGE_TABLE}
            UNION ALL
            SELECT ST_EndPoint(geom) AS geom
            FROM {EDGE_TABLE}
        ),
        endpoint_keys AS (
            SELECT
                ROUND(ST_X(ST_Transform(geom, 3857)) / {NODE_PRECISION_M}) * {NODE_PRECISION_M} AS x,
                ROUND(ST_Y(ST_Transform(geom, 3857)) / {NODE_PRECISION_M}) * {NODE_PRECISION_M} AS y
            FROM endpoints
            GROUP BY
                ROUND(ST_X(ST_Transform(geom, 3857)) / {NODE_PRECISION_M}) * {NODE_PRECISION_M},
                ROUND(ST_Y(ST_Transform(geom, 3857)) / {NODE_PRECISION_M}) * {NODE_PRECISION_M}
        )
        SELECT
            ROW_NUMBER() OVER (ORDER BY x, y)::bigint AS node_id,
            ST_Transform(ST_SetSRID(ST_MakePoint(x, y), 3857), 4326) AS geom,
            x,
            y
        FROM endpoint_keys;
    """)

    log(f"created {NODE_TABLE}: {cur.rowcount}")


def assign_edge_nodes(cur):
    cur.execute(f"""
        WITH node_lookup AS (
            SELECT node_id, x, y
            FROM {NODE_TABLE}
        ),
        edge_keys AS (
            SELECT
                edge_id,
                ROUND(ST_X(ST_Transform(ST_StartPoint(geom), 3857)) / {NODE_PRECISION_M}) * {NODE_PRECISION_M} AS source_x,
                ROUND(ST_Y(ST_Transform(ST_StartPoint(geom), 3857)) / {NODE_PRECISION_M}) * {NODE_PRECISION_M} AS source_y,
                ROUND(ST_X(ST_Transform(ST_EndPoint(geom), 3857)) / {NODE_PRECISION_M}) * {NODE_PRECISION_M} AS target_x,
                ROUND(ST_Y(ST_Transform(ST_EndPoint(geom), 3857)) / {NODE_PRECISION_M}) * {NODE_PRECISION_M} AS target_y
            FROM {EDGE_TABLE}
        ),
        matched AS (
            SELECT
                edge.edge_id,
                source_node.node_id AS source_node_id,
                target_node.node_id AS target_node_id
            FROM edge_keys AS edge
            JOIN node_lookup AS source_node
              ON source_node.x = edge.source_x
             AND source_node.y = edge.source_y
            JOIN node_lookup AS target_node
              ON target_node.x = edge.target_x
             AND target_node.y = edge.target_y
        )
        UPDATE {EDGE_TABLE} AS edge
        SET
            source_node_id = matched.source_node_id,
            target_node_id = matched.target_node_id
        FROM matched
        WHERE edge.edge_id = matched.edge_id;
    """)

    log(f"assigned edge nodes: {cur.rowcount}")


def create_indexes(cur):
    cur.execute(f"ALTER TABLE {EDGE_TABLE} ADD PRIMARY KEY (edge_id);")
    cur.execute(f"ALTER TABLE {NODE_TABLE} ADD PRIMARY KEY (node_id);")
    cur.execute(f"CREATE INDEX {EDGE_TABLE}_geom_idx ON {EDGE_TABLE} USING GIST (geom);")
    cur.execute(f"CREATE INDEX {EDGE_TABLE}_source_idx ON {EDGE_TABLE} (source_node_id);")
    cur.execute(f"CREATE INDEX {EDGE_TABLE}_target_idx ON {EDGE_TABLE} (target_node_id);")
    cur.execute(f"CREATE INDEX {NODE_TABLE}_geom_idx ON {NODE_TABLE} USING GIST (geom);")
    cur.execute(f"ANALYZE {EDGE_TABLE};")
    cur.execute(f"ANALYZE {NODE_TABLE};")


def print_summary(cur):
    cur.execute(f"SELECT COUNT(*) FROM {EDGE_TABLE};")
    edge_count = cur.fetchone()[0]
    cur.execute(f"SELECT COUNT(*) FROM {NODE_TABLE};")
    node_count = cur.fetchone()[0]
    cur.execute(f"""
        WITH node_degree AS (
            SELECT node_id, COUNT(*) AS degree
            FROM (
                SELECT source_node_id AS node_id FROM {EDGE_TABLE}
                UNION ALL
                SELECT target_node_id AS node_id FROM {EDGE_TABLE}
            ) AS nodes
            GROUP BY node_id
        )
        SELECT
            COUNT(*) FILTER (WHERE degree = 1),
            COUNT(*) FILTER (WHERE degree >= 3)
        FROM node_degree;
    """)
    dead_ends, intersections = cur.fetchone()

    log(f"edge_count={edge_count}")
    log(f"node_count={node_count}")
    log(f"dead_end_count={dead_ends}")
    log(f"intersection_count={intersections}")


def main():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    try:
        log("rebuilding noded route graph")
        rebuild_noded_edges(cur)
        rebuild_nodes(cur)
        assign_edge_nodes(cur)
        create_indexes(cur)
        conn.commit()
        print_summary(cur)
        log("noded route graph rebuild completed")

    except Exception as exc:
        conn.rollback()
        log(f"noded route graph rebuild failed: {exc}")
        raise

    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
