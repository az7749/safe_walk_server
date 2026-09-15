import argparse
import json
import os
from collections import Counter, defaultdict
from io import BytesIO
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import psycopg2
from psycopg2.extras import execute_values
from PIL import Image

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

ROAD_TABLE = "road_edges_cheongju"
WMS_SRS = "EPSG:3857"

WMS_CONFIGS = {
    "general": {
        "url": "https://www.safemap.go.kr/openapi2/IF_0087_WMS",
        "layer": "A2SM_CRMNLHSPOT_TOT",
        "style": "A2SM_CrmnlHspot_Tot_Tot",
        "image_path": PROCESSED_DIR / "crime_wms_total_cheongju.png",
        "metadata_path": PROCESSED_DIR / "crime_wms_total_cheongju_metadata.json",
        "weights": {"red": 50, "orange": 30, "yellow": 15},
    },
    "child": {
        "url": "https://www.safemap.go.kr/openapi2/IF_0081_WMS",
        "layer": "A2SM_ODBLRCRMNLHSPOT_KID",
        "style": "A2SM_OdblrCrmnlHspot_Kid",
        "image_path": PROCESSED_DIR / "crime_wms_child_cheongju.png",
        "metadata_path": PROCESSED_DIR / "crime_wms_child_cheongju_metadata.json",
        "weights": {"red": 30, "orange": 20, "yellow": 10},
    },
}

BASE_SCORE = 40
FACILITY_SCORE_CAP = 45
DEFAULT_IMAGE_WIDTH = 2048
DEFAULT_TILE_SIZE = 512
DEFAULT_SAMPLE_COUNT = 15
SAFEMAP_SERVICE_KEY_ENV = "SAFEMAP_SERVICE_KEY"


def log(message: str) -> None:
    print(message, flush=True)


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


def get_safemap_service_key() -> str:
    load_local_env()
    service_key = os.getenv(SAFEMAP_SERVICE_KEY_ENV)
    if not service_key:
        raise EnvironmentError(f"Add {SAFEMAP_SERVICE_KEY_ENV}=your_key to .env")
    return service_key


def get_road_bbox(cur) -> tuple[float, float, float, float]:
    cur.execute(f"""
        SELECT ST_XMin(ext), ST_YMin(ext), ST_XMax(ext), ST_YMax(ext)
        FROM (
            SELECT ST_Extent(geom)::box2d AS ext
            FROM {ROAD_TABLE}
        ) AS bounds;
    """)
    bbox = cur.fetchone()
    if bbox is None or any(value is None for value in bbox):
        raise RuntimeError(f"{ROAD_TABLE} has no road geometry.")
    return tuple(float(value) for value in bbox)


def expanded_bbox(
    bbox: tuple[float, float, float, float],
    padding_m: float,
) -> tuple[float, float, float, float]:
    minx, miny, maxx, maxy = bbox
    return minx - padding_m, miny - padding_m, maxx + padding_m, maxy + padding_m


def image_size_for_bbox(
    bbox: tuple[float, float, float, float],
    image_width: int,
) -> tuple[int, int]:
    minx, miny, maxx, maxy = bbox
    ratio = (maxy - miny) / (maxx - minx)
    image_height = max(1, round(image_width * ratio))
    return image_width, image_height


def build_wms_url(
    bbox: tuple[float, float, float, float],
    width: int,
    height: int,
    service_key: str,
    config: dict,
) -> str:
    params = {
        "serviceKey": service_key,
        "service": "WMS",
        "version": "1.1.1",
        "request": "GetMap",
        "layers": config["layer"],
        "styles": config["style"],
        "srs": WMS_SRS,
        "bbox": ",".join(str(value) for value in bbox),
        "format": "image/png",
        "width": width,
        "height": height,
        "transparent": "TRUE",
    }
    return f"{config['url']}?{urlencode(params)}"


def fetch_wms_image(url: str) -> bytes:
    request = Request(url, headers={"User-Agent": "Mozilla/5.0"})

    try:
        with urlopen(request, timeout=60) as response:
            content = response.read()
            content_type = response.headers.get("Content-Type", "")
    except HTTPError as error:
        body = error.read()[:500].decode("utf-8", errors="replace").replace("\n", " ")
        raise RuntimeError(f"WMS HTTP error: {error.code}, body={body}") from None

    if not content.startswith(b"\x89PNG"):
        preview = content[:500].decode("utf-8", errors="replace").replace("\n", " ")
        raise RuntimeError(f"WMS response is not PNG. content_type={content_type}, preview={preview}")

    return content


def fetch_wms_mosaic(
    bbox: tuple[float, float, float, float],
    width: int,
    height: int,
    tile_size: int,
    print_url: bool,
    service_key: str,
    config: dict,
) -> bytes:
    minx, miny, maxx, maxy = bbox
    tile_cols = max(1, (width + tile_size - 1) // tile_size)
    tile_rows = max(1, (height + tile_size - 1) // tile_size)
    mosaic = Image.new("RGBA", (width, height), (0, 0, 0, 0))

    for row in range(tile_rows):
        for col in range(tile_cols):
            left = col * tile_size
            upper = row * tile_size
            right = min(width, left + tile_size)
            lower = min(height, upper + tile_size)

            tile_width = right - left
            tile_height = lower - upper

            tile_minx = minx + (maxx - minx) * (left / width)
            tile_maxx = minx + (maxx - minx) * (right / width)
            tile_maxy = maxy - (maxy - miny) * (upper / height)
            tile_miny = maxy - (maxy - miny) * (lower / height)
            tile_bbox = (tile_minx, tile_miny, tile_maxx, tile_maxy)

            url = build_wms_url(tile_bbox, tile_width, tile_height, service_key, config)
            if print_url and row == 0 and col == 0:
                log(f"{config['layer']} first tile URL: {url}")

            log(f"{config['layer']} fetch tile {row + 1}/{tile_rows}, {col + 1}/{tile_cols}")
            tile_bytes = fetch_wms_image(url)
            tile_image = Image.open(BytesIO(tile_bytes)).convert("RGBA")
            mosaic.paste(tile_image, (left, upper))

    output = BytesIO()
    mosaic.save(output, format="PNG")
    return output.getvalue()


def save_wms_assets(
    image_bytes: bytes,
    bbox: tuple[float, float, float, float],
    width: int,
    height: int,
    config: dict,
) -> None:
    config["image_path"].write_bytes(image_bytes)
    metadata = {
        "srs": WMS_SRS,
        "bbox": bbox,
        "width": width,
        "height": height,
        "layer": config["layer"],
        "style": config["style"],
    }
    config["metadata_path"].write_text(
        json.dumps(metadata, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def load_wms_assets(config: dict) -> tuple[Image.Image, dict]:
    if not config["image_path"].exists() or not config["metadata_path"].exists():
        raise FileNotFoundError(f"Missing WMS image/metadata: {config['image_path']}")

    image = Image.open(config["image_path"]).convert("RGBA")
    metadata = json.loads(config["metadata_path"].read_text(encoding="utf-8"))
    return image, metadata


def classify_pixel(pixel: tuple[int, int, int, int]) -> str | None:
    red, green, blue, alpha = pixel

    if alpha < 40:
        return None

    if red >= 180 and green <= 110 and blue <= 110:
        return "red"
    if red >= 200 and 90 <= green <= 180 and blue <= 120:
        return "orange"
    if red >= 180 and green >= 170 and blue <= 150:
        return "yellow"

    return None


def point_to_pixel(
    x: float,
    y: float,
    bbox: tuple[float, float, float, float],
    width: int,
    height: int,
) -> tuple[int, int] | None:
    minx, miny, maxx, maxy = bbox
    if x < minx or x > maxx or y < miny or y > maxy:
        return None

    pixel_x = int((x - minx) / (maxx - minx) * width)
    pixel_y = int((maxy - y) / (maxy - miny) * height)

    pixel_x = max(0, min(width - 1, pixel_x))
    pixel_y = max(0, min(height - 1, pixel_y))
    return pixel_x, pixel_y


def print_top_opaque_colors(image: Image.Image, label: str, limit: int = 12) -> None:
    counter: Counter[tuple[int, int, int, int]] = Counter()
    width, height = image.size
    step_x = max(1, width // 512)
    step_y = max(1, height // 512)

    for y in range(0, height, step_y):
        for x in range(0, width, step_x):
            pixel = image.getpixel((x, y))
            if pixel[3] >= 40:
                counter[pixel] += 1

    log(f"{label} top opaque colors:")
    for pixel, count in counter.most_common(limit):
        log(f"- rgba={pixel}, sample_count={count}, class={classify_pixel(pixel)}")


def fetch_sample_points(cur, sample_count: int, max_edges: int | None = None):
    limit_sql = "LIMIT %s" if max_edges is not None else ""
    params: list[int] = []
    if max_edges is not None:
        params.append(max_edges)

    cur.execute(f"""
        WITH selected_roads AS (
            SELECT edge_id, facility_score, geom
            FROM {ROAD_TABLE}
            WHERE geom IS NOT NULL
            ORDER BY edge_id
            {limit_sql}
        )
        SELECT
            road.edge_id,
            road.facility_score,
            ST_X(ST_LineInterpolatePoint(road.geom, (sample_index + 0.5) / %s)) AS x,
            ST_Y(ST_LineInterpolatePoint(road.geom, (sample_index + 0.5) / %s)) AS y
        FROM selected_roads AS road
        CROSS JOIN generate_series(0, %s - 1) AS sample_index
        ORDER BY road.edge_id, sample_index;
    """, params + [sample_count, sample_count, sample_count])

    return cur.fetchall()


def pixel_risk_class(
    image: Image.Image,
    metadata: dict,
    x: float,
    y: float,
) -> str | None:
    bbox = tuple(float(value) for value in metadata["bbox"])
    width = int(metadata["width"])
    height = int(metadata["height"])
    pixel_position = point_to_pixel(float(x), float(y), bbox, width, height)
    if pixel_position is None:
        return None
    return classify_pixel(image.getpixel(pixel_position))


def calculate_penalties(
    cur,
    assets: dict[str, tuple[Image.Image, dict]],
    sample_count: int,
    max_edges: int | None,
) -> list[tuple[float, float, float, int, int]]:
    counts_by_kind: dict[str, dict[int, Counter[str]]] = {
        kind: defaultdict(Counter) for kind in assets
    }
    facility_scores_by_edge: dict[int, float] = {}

    for edge_id, facility_score, x, y in fetch_sample_points(cur, sample_count, max_edges=max_edges):
        facility_scores_by_edge[edge_id] = float(facility_score or 0)

        for kind, (image, metadata) in assets.items():
            risk_class = pixel_risk_class(image, metadata, float(x), float(y))
            counts_by_kind[kind][edge_id][risk_class or "none"] += 1

    updates = []
    for edge_id, facility_score in facility_scores_by_edge.items():
        penalties: dict[str, float] = {}

        for kind, counts_by_edge in counts_by_kind.items():
            counts = counts_by_edge[edge_id]
            total = sum(counts.values()) or 1
            weights = WMS_CONFIGS[kind]["weights"]
            penalties[kind] = (
                weights["red"] * (counts["red"] / total)
                + weights["orange"] * (counts["orange"] / total)
                + weights["yellow"] * (counts["yellow"] / total)
            )

        general_penalty = penalties.get("general", 0.0)
        child_penalty = penalties.get("child", 0.0)
        total_penalty = general_penalty + child_penalty
        capped_facility_score = min(FACILITY_SCORE_CAP, facility_score)
        final_score = round(max(0, min(100, BASE_SCORE + capped_facility_score - total_penalty)))
        updates.append((general_penalty, child_penalty, total_penalty, final_score, edge_id))

    return updates


def ensure_penalty_columns(cur) -> None:
    cur.execute(f"ALTER TABLE {ROAD_TABLE} ADD COLUMN IF NOT EXISTS general_crime_penalty DOUBLE PRECISION;")
    cur.execute(f"ALTER TABLE {ROAD_TABLE} ADD COLUMN IF NOT EXISTS child_crime_penalty DOUBLE PRECISION;")


def create_penalty_update_table(cur, updates: list[tuple[float, float, float, int, int]]) -> None:
    cur.execute("""
        CREATE TEMP TABLE tmp_crime_penalty_updates (
            edge_id BIGINT PRIMARY KEY,
            general_crime_penalty DOUBLE PRECISION,
            child_crime_penalty DOUBLE PRECISION,
            crime_penalty DOUBLE PRECISION,
            safety_score INTEGER
        ) ON COMMIT DROP;
    """)
    execute_values(
        cur,
        """
        INSERT INTO tmp_crime_penalty_updates (
            general_crime_penalty,
            child_crime_penalty,
            crime_penalty,
            safety_score,
            edge_id
        )
        VALUES %s
        """,
        updates,
        page_size=5000,
    )
    cur.execute("ANALYZE tmp_crime_penalty_updates;")


def sync_risk_zones(cur) -> None:
    cur.execute("DELETE FROM risk_zones WHERE TRIM(type) = 'crime_wms';")
    cur.execute(f"""
        INSERT INTO risk_zones (
            zone_id,
            type,
            penalty_score,
            geom
        )
        SELECT
            road.edge_id AS zone_id,
            'crime_wms' AS type,
            ROUND(updates.crime_penalty)::integer AS penalty_score,
            ST_Transform(ST_Buffer(road.geom, 20), 4326) AS geom
        FROM {ROAD_TABLE} AS road
        JOIN tmp_crime_penalty_updates AS updates
          ON road.edge_id = updates.edge_id
        WHERE updates.crime_penalty > 0;
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS risk_zones_geom_idx ON risk_zones USING GIST (geom);")
    cur.execute("ANALYZE risk_zones;")


def update_road_scores_from_risk_zones(cur) -> None:
    cur.execute(f"""
        UPDATE {ROAD_TABLE} AS road
        SET general_crime_penalty = updates.general_crime_penalty,
            child_crime_penalty = updates.child_crime_penalty,
            crime_penalty = COALESCE(zone.penalty_score, 0),
            safety_score = GREATEST(
                0,
                LEAST(
                    100,
                    {BASE_SCORE}
                    + LEAST({FACILITY_SCORE_CAP}, COALESCE(road.facility_score, 0))
                    - COALESCE(zone.penalty_score, 0)
                )
            )::integer,
            cost = road.length_m * (
                1 + (
                    (
                        100 - GREATEST(
                            0,
                            LEAST(
                                100,
                                {BASE_SCORE}
                                + LEAST({FACILITY_SCORE_CAP}, COALESCE(road.facility_score, 0))
                                - COALESCE(zone.penalty_score, 0)
                            )
                        )
                    )::double precision / 100
                )
            )
        FROM tmp_crime_penalty_updates AS updates
        LEFT JOIN risk_zones AS zone
          ON zone.zone_id = updates.edge_id
         AND TRIM(zone.type) = 'crime_wms'
        WHERE road.edge_id = updates.edge_id;
    """)


def print_summary(cur) -> None:
    cur.execute(f"""
        SELECT
            COUNT(*),
            MIN(general_crime_penalty),
            MAX(general_crime_penalty),
            AVG(general_crime_penalty),
            MIN(child_crime_penalty),
            MAX(child_crime_penalty),
            AVG(child_crime_penalty),
            MIN(crime_penalty),
            MAX(crime_penalty),
            AVG(crime_penalty),
            MIN(safety_score),
            MAX(safety_score),
            AVG(safety_score)
        FROM {ROAD_TABLE};
    """)
    (
        count,
        min_general,
        max_general,
        avg_general,
        min_child,
        max_child,
        avg_child,
        min_total,
        max_total,
        avg_total,
        min_score,
        max_score,
        avg_score,
    ) = cur.fetchone()

    log(f"road_count={count}")
    log(f"general_crime_penalty min={min_general:.2f}, max={max_general:.2f}, avg={avg_general:.2f}")
    log(f"child_crime_penalty min={min_child:.2f}, max={max_child:.2f}, avg={avg_child:.2f}")
    log(f"crime_penalty total min={min_total:.2f}, max={max_total:.2f}, avg={avg_total:.2f}")
    log(f"final safety_score min={min_score}, max={max_score}, avg={avg_score:.2f}")


def fetch_and_save_wms_assets(args: argparse.Namespace, cur, service_key: str) -> None:
    bbox = expanded_bbox(get_road_bbox(cur), args.bbox_padding_m)
    width, height = image_size_for_bbox(bbox, args.image_width)

    for kind in args.kinds:
        config = WMS_CONFIGS[kind]
        log(f"fetching {kind} WMS width={width}, height={height}, bbox={bbox}")
        url = build_wms_url(bbox, width, height, service_key, config)
        if args.print_url:
            log(f"{kind} WMS URL: {url}")

        try:
            image_bytes = fetch_wms_image(url)
        except RuntimeError as error:
            log(f"{kind} single WMS request failed: {error}")
            log(f"{kind} retrying as tiled WMS mosaic, tile_size={args.tile_size}")
            image_bytes = fetch_wms_mosaic(
                bbox=bbox,
                width=width,
                height=height,
                tile_size=args.tile_size,
                print_url=args.print_url,
                service_key=service_key,
                config=config,
            )

        save_wms_assets(image_bytes, bbox, width, height, config)
        log(f"{kind} WMS image saved: {config['image_path']}")
        log(f"{kind} WMS metadata saved: {config['metadata_path']}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Calculate road crime penalties from SafeMap WMS images."
    )
    parser.add_argument(
        "--kinds",
        nargs="+",
        choices=sorted(WMS_CONFIGS.keys()),
        default=["general", "child"],
        help="WMS kinds to use.",
    )
    parser.add_argument("--fetch-wms", action="store_true", help="Download WMS images before calculating.")
    parser.add_argument("--image-width", type=int, default=DEFAULT_IMAGE_WIDTH)
    parser.add_argument("--tile-size", type=int, default=DEFAULT_TILE_SIZE)
    parser.add_argument("--bbox-padding-m", type=float, default=100.0)
    parser.add_argument("--sample-count", type=int, default=DEFAULT_SAMPLE_COUNT)
    parser.add_argument("--max-edges", type=int, default=None, help="Limit roads for testing.")
    parser.add_argument("--print-url", action="store_true", help="Print WMS request URLs before fetching.")
    parser.add_argument("--dry-run", action="store_true", help="Calculate but do not update DB.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    try:
        if args.fetch_wms:
            service_key = get_safemap_service_key()
            fetch_and_save_wms_assets(args, cur, service_key)

        assets = {}
        for kind in args.kinds:
            image, metadata = load_wms_assets(WMS_CONFIGS[kind])
            print_top_opaque_colors(image, label=kind)
            assets[kind] = (image, metadata)

        updates = calculate_penalties(
            cur,
            assets=assets,
            sample_count=args.sample_count,
            max_edges=args.max_edges,
        )
        log(f"calculated penalties: {len(updates)} roads")

        if args.dry_run:
            conn.rollback()
            log("dry-run completed. DB not updated.")
            return

        ensure_penalty_columns(cur)
        create_penalty_update_table(cur, updates)
        sync_risk_zones(cur)
        update_road_scores_from_risk_zones(cur)
        conn.commit()
        log("risk_zones sync and crime penalty update completed")
        print_summary(cur)

    except Exception as e:
        conn.rollback()
        log(f"crime penalty calculation failed: {e}")
        raise

    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
