from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS
import heapq
import json
import math
import os
import psycopg2
import re
import shutil
import uuid
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from werkzeug.security import generate_password_hash, check_password_hash
from werkzeug.utils import secure_filename

app = Flask(__name__)
# 주석: Flutter 지도 화면에서 현재 화면 범위 기준으로 재조회할 수 있도록 CORS 허용
CORS(app)

UPLOAD_ROOT = os.path.join(os.path.dirname(__file__), 'uploads')
REPORT_UPLOAD_ROOT = os.path.join(UPLOAD_ROOT, 'reports')
ALLOWED_REPORT_IMAGE_EXTENSIONS = {'.jpg', '.jpeg', '.png', '.webp'}
app.config['MAX_CONTENT_LENGTH'] = 10 * 1024 * 1024

DB_CONFIG = {
    'host': 'localhost',
    # 'host': '192.168.35.20',
    'dbname': 'night_safe_walk',
    'user': 'postgres',
    'password': '0000',
    'port': 5432
}

ROUTE_MODE_CONFIG = {
    "fast": {
        "safety_weight": 0.0,
        "report_weight": 0.0,
        "label": "빠른길",
    },
    "safe": {
        "safety_weight": 1.0,
        "report_weight": 4.0,
        "label": "안전한길",
    },
}
SNAP_DISTANCE_WEIGHT = 20
REPORT_ROUTE_MATCH_RADIUS_M = 20
ROUTE_EDGE_TABLE = "route_edges_noded"
ROUTE_NODE_TABLE = "route_nodes_noded"
NAVER_LOCAL_SEARCH_URL = "https://openapi.naver.com/v1/search/local.json"
NAVER_REVERSE_GEOCODE_URL = (
    "https://maps.apigw.ntruss.com/map-reversegeocode/v2/gc"
)


def load_env_file():
    env_path = os.path.join(os.path.dirname(__file__), ".env")

    if not os.path.exists(env_path):
        return

    with open(env_path, "r", encoding="utf-8") as env_file:
        for line in env_file:
            line = line.strip()

            if not line or line.startswith("#") or "=" not in line:
                continue

            key, value = line.split("=", 1)
            os.environ.setdefault(key.strip(), value.strip())


load_env_file()


def get_db_connection():
    conn = psycopg2.connect(
        host=DB_CONFIG['host'],
        dbname=DB_CONFIG['dbname'],
        user=DB_CONFIG['user'],
        password=DB_CONFIG['password'],
        port=DB_CONFIG['port']
    )
    return conn


def strip_html_tags(value):
    return re.sub(r"<[^>]+>", "", value or "")


def parse_naver_local_coordinate(value):
    if value in (None, ""):
        return None

    try:
        return int(value) / 10000000
    except (TypeError, ValueError):
        return None


def build_naver_reverse_address(result):
    region = result.get('region') or {}
    land = result.get('land') or {}
    parts = []

    for key in ('area1', 'area2', 'area3', 'area4'):
        name = str((region.get(key) or {}).get('name') or '').strip()
        if name and name not in parts:
            parts.append(name)

    land_name = str(land.get('name') or '').strip()
    if land_name:
        parts.append(land_name)

    number1 = str(land.get('number1') or '').strip()
    number2 = str(land.get('number2') or '').strip()
    if number1:
        parts.append(f'{number1}-{number2}' if number2 else number1)

    return ' '.join(parts)


def get_report_image_extension(filename):
    safe_name = secure_filename(filename or '')
    extension = os.path.splitext(safe_name)[1].lower()
    return extension if extension in ALLOWED_REPORT_IMAGE_EXTENSIONS else None


def build_report_image_url(relative_path):
    if not relative_path:
        return None
    return f"/uploads/{relative_path.strip().replace(os.sep, '/')}"


def is_admin_user(cur, user_id):
    if user_id is None:
        return False

    cur.execute("""
        SELECT 1
        FROM users
        WHERE user_id = %s
          AND TRIM(role) = 'admin'
    """, (user_id,))
    return cur.fetchone() is not None


def normalize_mobile_phone(value):
    digits = re.sub(r'\D', '', value or '')

    if not re.fullmatch(r'01[016789]\d{7,8}', digits):
        return None

    if len(digits) == 10:
        return f'{digits[:3]}-{digits[3:6]}-{digits[6:]}'
    return f'{digits[:3]}-{digits[3:7]}-{digits[7:]}'


from admin_management import create_admin_management

app.register_blueprint(create_admin_management(
    get_db_connection, is_admin_user, normalize_mobile_phone,
))


@app.route('/')
def home():
    return 'PostgreSQL Flask server is running!'


@app.route('/search/places', methods=['GET'])
def search_places():
    query = request.args.get('query', '').strip()
    display = request.args.get('display', default=5, type=int)

    if not query:
        return jsonify({
            'success': False,
            'message': '검색어를 입력해주세요.'
        }), 400

    display = max(1, min(display, 10))

    client_id = os.getenv('NAVER_SEARCH_CLIENT_ID')
    client_secret = os.getenv('NAVER_SEARCH_CLIENT_SECRET')

    if not client_id or not client_secret:
        return jsonify({
            'success': False,
            'message': '네이버 검색 API 키가 설정되어 있지 않습니다.'
        }), 500

    params = urlencode({
        'query': query,
        'display': display,
        'start': 1,
        'sort': 'random',
    })
    url = f'{NAVER_LOCAL_SEARCH_URL}?{params}'
    naver_request = Request(
        url,
        headers={
            'X-Naver-Client-Id': client_id,
            'X-Naver-Client-Secret': client_secret,
        },
    )

    try:
        with urlopen(naver_request, timeout=10) as response:
            payload = response.read().decode('utf-8')

        data = json.loads(payload)
        places = []

        for index, item in enumerate(data.get('items', []), start=1):
            lng = parse_naver_local_coordinate(item.get('mapx'))
            lat = parse_naver_local_coordinate(item.get('mapy'))

            places.append({
                'id': f'naver_{index}_{item.get("mapx", "")}_{item.get("mapy", "")}',
                'title': strip_html_tags(item.get('title')),
                'category': item.get('category', ''),
                'address': item.get('address', ''),
                'roadAddress': item.get('roadAddress', ''),
                'lat': lat,
                'lng': lng,
                'link': item.get('link', ''),
            })

        return jsonify({
            'success': True,
            'places': places,
        }), 200

    except HTTPError as error:
        body = error.read().decode('utf-8', errors='replace')
        return jsonify({
            'success': False,
            'message': f'네이버 검색 API 오류: {error.code}',
            'detail': body,
        }), 502

    except (URLError, TimeoutError) as error:
        return jsonify({
            'success': False,
            'message': f'네이버 검색 API 연결 오류: {str(error)}'
        }), 502

    except Exception as error:
        return jsonify({
            'success': False,
            'message': f'장소 검색 오류: {str(error)}'
        }), 500


@app.route('/reverse-geocode', methods=['GET'])
def reverse_geocode():
    lat = request.args.get('lat', type=float)
    lng = request.args.get('lng', type=float)

    if lat is None or lng is None:
        return jsonify({
            'success': False,
            'message': 'lat, lng 좌표가 필요합니다.'
        }), 400

    if not (-90 <= lat <= 90 and -180 <= lng <= 180):
        return jsonify({
            'success': False,
            'message': '좌표 범위가 올바르지 않습니다.'
        }), 400

    client_id = os.getenv('NAVER_MAP_CLIENT_ID')
    client_secret = os.getenv('NAVER_MAP_CLIENT_SECRET')
    if not client_id or not client_secret:
        return jsonify({
            'success': False,
            'message': '네이버 지도 API 키가 설정되어 있지 않습니다.'
        }), 500

    params = urlencode({
        'request': 'coordsToaddr',
        'coords': f'{lng},{lat}',
        'sourcecrs': 'epsg:4326',
        'orders': 'roadaddr,addr',
        'output': 'json',
    })
    naver_request = Request(
        f'{NAVER_REVERSE_GEOCODE_URL}?{params}',
        headers={
            'x-ncp-apigw-api-key-id': client_id,
            'x-ncp-apigw-api-key': client_secret,
        },
    )

    try:
        with urlopen(naver_request, timeout=10) as response:
            data = json.loads(response.read().decode('utf-8'))

        results = data.get('results') or []
        road_result = next(
            (item for item in results if item.get('name') == 'roadaddr'),
            None,
        )
        address_result = road_result or next(
            (item for item in results if item.get('name') == 'addr'),
            None,
        )
        address = build_naver_reverse_address(address_result or {})

        if not address:
            return jsonify({
                'success': False,
                'message': '해당 위치의 주소를 찾을 수 없습니다.'
            }), 404

        return jsonify({
            'success': True,
            'address': address,
            'address_type': (address_result or {}).get('name'),
        }), 200
    except HTTPError as error:
        body = error.read().decode('utf-8', errors='replace')
        return jsonify({
            'success': False,
            'message': f'네이버 역지오코딩 API 오류: {error.code}',
            'detail': body,
        }), 502
    except (URLError, TimeoutError) as error:
        return jsonify({
            'success': False,
            'message': f'네이버 역지오코딩 API 연결 오류: {str(error)}'
        }), 502
    except Exception as error:
        return jsonify({
            'success': False,
            'message': f'주소 변환 오류: {str(error)}'
        }), 500

@app.route('/check-userid', methods=['POST'])
def check_userid():
    data = request.get_json()

    if not data:
        return jsonify({
            'success': False,
            'message': '요청 데이터가 없습니다.'
        }), 400

    user_id = data.get('user_id')

    if not user_id:
        return jsonify({
            'success': False,
            'message': '아이디를 입력해주세요.'
        }), 400

    conn = None
    cur = None

    try:
        conn = get_db_connection()
        cur = conn.cursor()

        cur.execute(
            "SELECT id FROM users WHERE user_id = %s",
            (user_id,)
        )
        user = cur.fetchone()

        if user:
            return jsonify({
                'success': True,
                'available': False,
                'message': '이미 존재하는 아이디입니다.'
            }), 200

        return jsonify({
            'success': True,
            'available': True,
            'message': '사용 가능한 아이디입니다.'
        }), 200

    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'서버 오류: {str(e)}'
        }), 500

    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

@app.route('/signup', methods=['POST'])
def signup():
    data = request.get_json()
    print(data)
    if not data:
        return jsonify({
            'success': False,
            'message': '요청 데이터가 없습니다.'
        }), 400

    login_id = data.get('login_id')
    name = data.get('name')
    phone = normalize_mobile_phone(data.get('phone'))
    birth_date = data.get('birth_date')
    gender = data.get('gender')
    password = data.get('password')

    if not login_id or not name or not phone or not birth_date or not gender or not password:
        return jsonify({
            'success': False,
            'message': '모든 항목을 입력해주세요.'
        }), 400
    hashed_password = generate_password_hash(password)

    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute('SELECT * FROM users WHERE login_id = %s', (login_id,))
        existing_user = cur.fetchone()

        if existing_user:
            return jsonify({
                'success': False,
                'message': '이미 존재하는 아이디입니다.'
            }), 409

        cur.execute(
            'INSERT INTO users (login_id, name, phone, birth_date, gender, password) VALUES (%s, %s, %s, %s, %s, %s)',
            (login_id, name, phone, birth_date, gender, hashed_password)
        )

        conn.commit()

        return jsonify({
            'success': True,
            'message': '회원가입이 완료되었습니다.'
        }), 201

    except Exception as e:
        conn.rollback()
        return jsonify({
            'success': False,
            'message': f'회원가입 중 오류 발생: {str(e)}'
        }), 500

    finally:
        cur.close()
        conn.close()


@app.route('/login', methods=['POST'])
def login():
    data = request.get_json()
    print("login data:", data)
    
    if not data:
        return jsonify({
            'success': False,
            'message': '요청 데이터가 없습니다.'
        }), 400

    login_id = data.get('login_id')
    password = data.get('password')

    if not login_id or not password:
        return jsonify({
            'success': False,
            'message': '아이디와 비밀번호를 입력해주세요.'
        }), 400

    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
                SELECT
                    user_id,
                    login_id,
                    password,
                    name,
                    COALESCE(TRIM(role), 'user')
                FROM users
                WHERE login_id = %s
            """,
            (login_id,)
        )
        user = cur.fetchone()
        print("db user:", user)

        if user is None:
            return jsonify({
                'success': False,
                'message': '아이디 또는 비밀번호를 다시 확인하세요.'
            }), 401

        db_user_id, db_login_id, db_password, db_name, db_role = user

        if not check_password_hash(db_password, password):
            return jsonify({
                'success': False,
                'message': '아이디 또는 비밀번호를 다시 확인하세요.'
            }), 401

        return jsonify({
            'success': True,
            'message': '로그인 성공',
            'user': {
                'id': db_user_id,
                'login_id': db_login_id,
                'name': db_name,
                'role': db_role,
            }
        }), 200

    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'로그인 중 오류 발생: {str(e)}'
        }), 500

    finally:
        cur.close()
        conn.close()


@app.route('/password-reset', methods=['POST'])
def reset_password():
    data = request.get_json()

    if not data:
        return jsonify({
            'success': False,
            'message': '요청 데이터가 없습니다.'
        }), 400

    login_id = data.get('login_id')
    name = data.get('name')
    phone = normalize_mobile_phone(data.get('phone'))
    new_password = data.get('new_password')

    if not login_id or not name or not phone or not new_password:
        return jsonify({
            'success': False,
            'message': '아이디, 이름, 전화번호, 새 비밀번호를 입력해주세요.'
        }), 400

    if len(new_password) < 8:
        return jsonify({
            'success': False,
            'message': '새 비밀번호는 8자 이상이어야 합니다.'
        }), 400

    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute("""
            SELECT user_id
            FROM users
            WHERE login_id = %s
              AND name = %s
              AND REGEXP_REPLACE(phone, '\\D', '', 'g') = %s
        """, (login_id, name, re.sub(r'\D', '', phone)))
        user = cur.fetchone()

        if user is None:
            return jsonify({
                'success': False,
                'message': '일치하는 사용자 정보를 찾을 수 없습니다.'
            }), 404

        cur.execute("""
            UPDATE users
            SET password = %s
            WHERE user_id = %s
        """, (generate_password_hash(new_password), user[0]))

        conn.commit()

        return jsonify({
            'success': True,
            'message': '비밀번호가 변경되었습니다.'
        }), 200

    except Exception as e:
        conn.rollback()
        return jsonify({
            'success': False,
            'message': f'비밀번호 변경 중 오류 발생: {str(e)}'
        }), 500

    finally:
        cur.close()
        conn.close()


@app.route('/users/<int:user_id>', methods=['GET'])
def get_user_profile(user_id):
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute("""
            SELECT
                user_id,
                login_id,
                name,
                phone,
                birth_date,
                gender
            FROM users
            WHERE user_id = %s
        """, (user_id,))
        user = cur.fetchone()

        if user is None:
            return jsonify({
                'success': False,
                'message': '사용자 정보를 찾을 수 없습니다.'
            }), 404

        return jsonify({
            'success': True,
            'user': {
                'user_id': user[0],
                'login_id': user[1],
                'name': user[2],
                'phone': user[3],
                'birth_date': user[4].isoformat() if user[4] else '',
                'gender': user[5] or '',
            }
        }), 200

    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'사용자 조회 중 오류 발생: {str(e)}'
        }), 500

    finally:
        cur.close()
        conn.close()


@app.route('/users/<int:user_id>', methods=['PUT'])
def update_user_profile(user_id):
    data = request.get_json()

    if not data:
        return jsonify({
            'success': False,
            'message': '요청 데이터가 없습니다.'
        }), 400

    name = data.get('name')
    phone = normalize_mobile_phone(data.get('phone'))
    birth_date = data.get('birth_date')
    gender = data.get('gender')

    if not name or not phone or not birth_date or not gender:
        return jsonify({
            'success': False,
            'message': '이름, 전화번호, 생년월일, 성별을 입력해주세요.'
        }), 400

    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute("""
            UPDATE users
            SET
                name = %s,
                phone = %s,
                birth_date = %s,
                gender = %s
            WHERE user_id = %s
        """, (name, phone, birth_date, gender, user_id))

        if cur.rowcount == 0:
            conn.rollback()
            return jsonify({
                'success': False,
                'message': '사용자 정보를 찾을 수 없습니다.'
            }), 404

        conn.commit()

        return jsonify({
            'success': True,
            'message': '내 정보가 수정되었습니다.'
        }), 200

    except Exception as e:
        conn.rollback()
        return jsonify({
            'success': False,
            'message': f'사용자 수정 중 오류 발생: {str(e)}'
        }), 500

    finally:
        cur.close()
        conn.close()


@app.route('/users/<int:user_id>/password', methods=['PUT'])
def change_user_password(user_id):
    data = request.get_json()

    if not data:
        return jsonify({
            'success': False,
            'message': '요청 데이터가 없습니다.'
        }), 400

    current_password = data.get('current_password')
    new_password = data.get('new_password')

    if not current_password or not new_password:
        return jsonify({
            'success': False,
            'message': '현재 비밀번호와 새 비밀번호를 입력해주세요.'
        }), 400

    if len(new_password) < 8:
        return jsonify({
            'success': False,
            'message': '새 비밀번호는 8자 이상이어야 합니다.'
        }), 400

    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute(
            'SELECT password FROM users WHERE user_id = %s',
            (user_id,)
        )
        user = cur.fetchone()

        if user is None:
            return jsonify({
                'success': False,
                'message': '사용자 정보를 찾을 수 없습니다.'
            }), 404

        if not check_password_hash(user[0], current_password):
            return jsonify({
                'success': False,
                'message': '현재 비밀번호가 일치하지 않습니다.'
            }), 401

        cur.execute(
            'UPDATE users SET password = %s WHERE user_id = %s',
            (generate_password_hash(new_password), user_id)
        )
        conn.commit()

        return jsonify({
            'success': True,
            'message': '비밀번호가 변경되었습니다.'
        }), 200

    except Exception as e:
        conn.rollback()
        return jsonify({
            'success': False,
            'message': f'비밀번호 변경 중 오류 발생: {str(e)}'
        }), 500

    finally:
        cur.close()
        conn.close()


@app.route('/users/<int:user_id>/alarm-settings', methods=['GET'])
def get_alarm_settings(user_id):
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute('SELECT 1 FROM users WHERE user_id = %s', (user_id,))
        if cur.fetchone() is None:
            return jsonify({
                'success': False,
                'message': '사용자 정보를 찾을 수 없습니다.'
            }), 404

        cur.execute("""
            INSERT INTO alarm_settings (
                user_id,
                risk_zone_alert,
                push_alert,
                vibration_alert
            )
            VALUES (%s, TRUE, TRUE, TRUE)
            ON CONFLICT (user_id) DO NOTHING
        """, (user_id,))
        conn.commit()

        cur.execute("""
            SELECT risk_zone_alert, push_alert, vibration_alert
            FROM alarm_settings
            WHERE user_id = %s
        """, (user_id,))
        settings = cur.fetchone()

        return jsonify({
            'success': True,
            'settings': {
                'risk_zone_alert': settings[0],
                'push_alert': settings[1],
                'vibration_alert': settings[2],
            }
        }), 200

    except Exception as e:
        conn.rollback()
        return jsonify({
            'success': False,
            'message': f'알림 설정 조회 오류: {str(e)}'
        }), 500

    finally:
        cur.close()
        conn.close()


@app.route('/users/<int:user_id>/alarm-settings', methods=['PUT'])
def update_alarm_settings(user_id):
    data = request.get_json(silent=True) or {}
    required_fields = ('risk_zone_alert', 'push_alert', 'vibration_alert')

    if any(type(data.get(field)) is not bool for field in required_fields):
        return jsonify({
            'success': False,
            'message': '알림 설정값은 true 또는 false여야 합니다.'
        }), 400

    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute('SELECT 1 FROM users WHERE user_id = %s', (user_id,))
        if cur.fetchone() is None:
            return jsonify({
                'success': False,
                'message': '사용자 정보를 찾을 수 없습니다.'
            }), 404

        cur.execute("""
            INSERT INTO alarm_settings (
                user_id,
                risk_zone_alert,
                push_alert,
                vibration_alert
            )
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (user_id) DO UPDATE SET
                risk_zone_alert = EXCLUDED.risk_zone_alert,
                push_alert = EXCLUDED.push_alert,
                vibration_alert = EXCLUDED.vibration_alert
        """, (
            user_id,
            data['risk_zone_alert'],
            data['push_alert'],
            data['vibration_alert'],
        ))
        conn.commit()

        return jsonify({
            'success': True,
            'message': '알림 설정이 저장되었습니다.'
        }), 200

    except Exception as e:
        conn.rollback()
        return jsonify({
            'success': False,
            'message': f'알림 설정 저장 오류: {str(e)}'
        }), 500

    finally:
        cur.close()
        conn.close()


@app.route('/users/<int:user_id>/alarm-logs', methods=['GET'])
def get_alarm_logs(user_id):
    limit = request.args.get('limit', default=100, type=int)
    limit = max(1, min(limit, 300))
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute('SELECT 1 FROM users WHERE user_id = %s', (user_id,))
        if cur.fetchone() is None:
            return jsonify({
                'success': False,
                'message': '사용자 정보를 찾을 수 없습니다.'
            }), 404

        cur.execute("""
            SELECT
                log_id,
                TRIM(alarm_type),
                TRIM(content),
                is_read,
                created_at
            FROM alarm_logs
            WHERE user_id = %s
            ORDER BY created_at DESC, log_id DESC
            LIMIT %s
        """, (user_id, limit))

        logs = [
            {
                'log_id': row[0],
                'alarm_type': row[1],
                'content': row[2],
                'is_read': row[3],
                'created_at': row[4].isoformat(),
            }
            for row in cur.fetchall()
        ]
        unread_count = sum(1 for log in logs if not log['is_read'])

        return jsonify({
            'success': True,
            'unread_count': unread_count,
            'logs': logs,
        }), 200

    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'알림 내역 조회 오류: {str(e)}'
        }), 500

    finally:
        cur.close()
        conn.close()


@app.route(
    '/users/<int:user_id>/alarm-logs/<int:log_id>/read',
    methods=['PATCH']
)
def read_alarm_log(user_id, log_id):
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute("""
            UPDATE alarm_logs
            SET is_read = TRUE
            WHERE log_id = %s AND user_id = %s
        """, (log_id, user_id))

        if cur.rowcount == 0:
            conn.rollback()
            return jsonify({
                'success': False,
                'message': '알림 내역을 찾을 수 없습니다.'
            }), 404

        conn.commit()
        return jsonify({
            'success': True,
            'message': '알림을 읽음 처리했습니다.'
        }), 200

    except Exception as e:
        conn.rollback()
        return jsonify({
            'success': False,
            'message': f'알림 읽음 처리 오류: {str(e)}'
        }), 500

    finally:
        cur.close()
        conn.close()


@app.route('/users/<int:user_id>/alarm-logs/read-all', methods=['PATCH'])
def read_all_alarm_logs(user_id):
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute('SELECT 1 FROM users WHERE user_id = %s', (user_id,))
        if cur.fetchone() is None:
            return jsonify({
                'success': False,
                'message': '사용자 정보를 찾을 수 없습니다.'
            }), 404

        cur.execute("""
            UPDATE alarm_logs
            SET is_read = TRUE
            WHERE user_id = %s AND is_read = FALSE
        """, (user_id,))
        updated_count = cur.rowcount
        conn.commit()

        return jsonify({
            'success': True,
            'updated_count': updated_count,
            'message': '모든 알림을 읽음 처리했습니다.'
        }), 200

    except Exception as e:
        conn.rollback()
        return jsonify({
            'success': False,
            'message': f'전체 알림 읽음 처리 오류: {str(e)}'
        }), 500

    finally:
        cur.close()
        conn.close()


@app.route(
    '/users/<int:user_id>/alarm-logs/<int:log_id>',
    methods=['DELETE']
)
def delete_alarm_log(user_id, log_id):
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute("""
            DELETE FROM alarm_logs
            WHERE log_id = %s AND user_id = %s
        """, (log_id, user_id))

        if cur.rowcount == 0:
            conn.rollback()
            return jsonify({
                'success': False,
                'message': '알림 내역을 찾을 수 없습니다.'
            }), 404

        conn.commit()
        return jsonify({
            'success': True,
            'message': '알림 내역을 삭제했습니다.'
        }), 200

    except Exception as e:
        conn.rollback()
        return jsonify({
            'success': False,
            'message': f'알림 내역 삭제 오류: {str(e)}'
        }), 500

    finally:
        cur.close()
        conn.close()


@app.route('/users/<int:user_id>/alarm-logs', methods=['DELETE'])
def delete_all_alarm_logs(user_id):
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute('SELECT 1 FROM users WHERE user_id = %s', (user_id,))
        if cur.fetchone() is None:
            return jsonify({
                'success': False,
                'message': '사용자 정보를 찾을 수 없습니다.'
            }), 404

        cur.execute(
            'DELETE FROM alarm_logs WHERE user_id = %s',
            (user_id,),
        )
        deleted_count = cur.rowcount
        conn.commit()

        return jsonify({
            'success': True,
            'deleted_count': deleted_count,
            'message': '알림 내역을 모두 삭제했습니다.'
        }), 200

    except Exception as e:
        conn.rollback()
        return jsonify({
            'success': False,
            'message': f'전체 알림 내역 삭제 오류: {str(e)}'
        }), 500

    finally:
        cur.close()
        conn.close()


@app.route('/users/<int:user_id>/alarm-logs/risk-zone', methods=['POST'])
def create_risk_zone_alarm_log(user_id):
    data = request.get_json(silent=True) or {}
    road_id = data.get('road_id')

    try:
        road_id = int(road_id)
    except (TypeError, ValueError):
        return jsonify({
            'success': False,
            'message': '도로 정보가 필요합니다.'
        }), 400

    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute("""
            SELECT setting.risk_zone_alert, road.safety_score
            FROM users AS app_user
            JOIN alarm_settings AS setting
              ON setting.user_id = app_user.user_id
            JOIN osm_edges AS road
              ON road.edge_id = %s
            WHERE app_user.user_id = %s
        """, (road_id, user_id))
        row = cur.fetchone()

        if row is None:
            return jsonify({
                'success': False,
                'message': '사용자 또는 도로 정보를 찾을 수 없습니다.'
            }), 404

        risk_zone_alert, safety_score = row
        if not risk_zone_alert:
            return jsonify({
                'success': True,
                'logged': False,
                'message': '위험구역 알림이 꺼져 있습니다.'
            }), 200

        if safety_score is None or float(safety_score) >= 50:
            return jsonify({
                'success': False,
                'message': '위험 등급 도로가 아닙니다.'
            }), 409

        cur.execute("""
            SELECT 1
            FROM alarm_logs
            WHERE user_id = %s
              AND TRIM(alarm_type) = 'risk_zone'
              AND created_at >= CURRENT_TIMESTAMP - INTERVAL '45 seconds'
            LIMIT 1
        """, (user_id,))
        if cur.fetchone() is not None:
            return jsonify({
                'success': True,
                'logged': False,
                'message': '최근 위험구역 알림 기록이 있습니다.'
            }), 200

        content = '위험구역에 진입했습니다. 주변을 살피고 안전에 유의해주세요.'
        cur.execute("""
            INSERT INTO alarm_logs (user_id, alarm_type, content, is_read)
            VALUES (%s, 'risk_zone', %s, FALSE)
            RETURNING log_id, created_at
        """, (user_id, content))
        log_id, created_at = cur.fetchone()
        conn.commit()

        return jsonify({
            'success': True,
            'logged': True,
            'log': {
                'log_id': log_id,
                'alarm_type': 'risk_zone',
                'content': content,
                'created_at': created_at.isoformat(),
            }
        }), 201

    except Exception as e:
        conn.rollback()
        return jsonify({
            'success': False,
            'message': f'위험구역 알림 기록 오류: {str(e)}'
        }), 500

    finally:
        cur.close()
        conn.close()


@app.route('/users/<int:user_id>/emergency-contacts', methods=['GET'])
def get_emergency_contacts(user_id):
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute('SELECT 1 FROM users WHERE user_id = %s', (user_id,))
        if cur.fetchone() is None:
            return jsonify({
                'success': False,
                'message': '사용자 정보를 찾을 수 없습니다.'
            }), 404

        cur.execute("""
            SELECT contact_id, TRIM(name), TRIM(phone)
            FROM emergency_contacts
            WHERE user_id = %s
            ORDER BY contact_id
        """, (user_id,))

        contacts = [
            {
                'contact_id': row[0],
                'name': row[1],
                'phone': row[2],
            }
            for row in cur.fetchall()
        ]
        return jsonify({
            'success': True,
            'contacts': contacts,
        }), 200

    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'비상 연락처 조회 오류: {str(e)}'
        }), 500

    finally:
        cur.close()
        conn.close()


@app.route('/users/<int:user_id>/emergency-contacts', methods=['POST'])
def create_emergency_contact(user_id):
    data = request.get_json(silent=True) or {}
    name = (data.get('name') or '').strip()
    phone = normalize_mobile_phone(data.get('phone'))

    if not name or len(name) > 20:
        return jsonify({
            'success': False,
            'message': '이름은 1자 이상 20자 이하로 입력해주세요.'
        }), 400

    if phone is None:
        return jsonify({
            'success': False,
            'message': '올바른 휴대전화 번호를 입력해주세요.'
        }), 400

    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute(
            'SELECT user_id FROM users WHERE user_id = %s FOR UPDATE',
            (user_id,)
        )
        if cur.fetchone() is None:
            conn.rollback()
            return jsonify({
                'success': False,
                'message': '사용자 정보를 찾을 수 없습니다.'
            }), 404

        cur.execute(
            'SELECT COUNT(*) FROM emergency_contacts WHERE user_id = %s',
            (user_id,)
        )
        if cur.fetchone()[0] >= 3:
            conn.rollback()
            return jsonify({
                'success': False,
                'message': '비상 연락처는 최대 3개까지 등록할 수 있습니다.'
            }), 409

        cur.execute("""
            SELECT 1
            FROM emergency_contacts
            WHERE user_id = %s
              AND REGEXP_REPLACE(phone, '\\D', '', 'g') = %s
        """, (user_id, re.sub(r'\D', '', phone)))
        if cur.fetchone() is not None:
            conn.rollback()
            return jsonify({
                'success': False,
                'message': '이미 등록된 전화번호입니다.'
            }), 409

        cur.execute("""
            INSERT INTO emergency_contacts (user_id, name, phone)
            VALUES (%s, %s, %s)
            RETURNING contact_id
        """, (user_id, name, phone))
        contact_id = cur.fetchone()[0]
        conn.commit()

        return jsonify({
            'success': True,
            'message': '비상 연락처가 등록되었습니다.',
            'contact': {
                'contact_id': contact_id,
                'name': name,
                'phone': phone,
            }
        }), 201

    except Exception as e:
        conn.rollback()
        return jsonify({
            'success': False,
            'message': f'비상 연락처 등록 오류: {str(e)}'
        }), 500

    finally:
        cur.close()
        conn.close()


@app.route(
    '/users/<int:user_id>/emergency-contacts/<int:contact_id>',
    methods=['PUT']
)
def update_emergency_contact(user_id, contact_id):
    data = request.get_json(silent=True) or {}
    name = (data.get('name') or '').strip()
    phone = normalize_mobile_phone(data.get('phone'))

    if not name or len(name) > 20:
        return jsonify({
            'success': False,
            'message': '이름은 1자 이상 20자 이하로 입력해주세요.'
        }), 400

    if phone is None:
        return jsonify({
            'success': False,
            'message': '올바른 휴대전화 번호를 입력해주세요.'
        }), 400

    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute("""
            SELECT 1
            FROM emergency_contacts
            WHERE contact_id = %s AND user_id = %s
            FOR UPDATE
        """, (contact_id, user_id))
        if cur.fetchone() is None:
            conn.rollback()
            return jsonify({
                'success': False,
                'message': '비상 연락처를 찾을 수 없습니다.'
            }), 404

        cur.execute("""
            SELECT 1
            FROM emergency_contacts
            WHERE user_id = %s
              AND contact_id <> %s
              AND REGEXP_REPLACE(phone, '\\D', '', 'g') = %s
        """, (user_id, contact_id, re.sub(r'\D', '', phone)))
        if cur.fetchone() is not None:
            conn.rollback()
            return jsonify({
                'success': False,
                'message': '이미 등록된 전화번호입니다.'
            }), 409

        cur.execute("""
            UPDATE emergency_contacts
            SET name = %s, phone = %s
            WHERE contact_id = %s AND user_id = %s
        """, (name, phone, contact_id, user_id))
        conn.commit()

        return jsonify({
            'success': True,
            'message': '비상 연락처가 수정되었습니다.'
        }), 200

    except Exception as e:
        conn.rollback()
        return jsonify({
            'success': False,
            'message': f'비상 연락처 수정 오류: {str(e)}'
        }), 500

    finally:
        cur.close()
        conn.close()


@app.route(
    '/users/<int:user_id>/emergency-contacts/<int:contact_id>',
    methods=['DELETE']
)
def delete_emergency_contact(user_id, contact_id):
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute("""
            DELETE FROM emergency_contacts
            WHERE contact_id = %s AND user_id = %s
        """, (contact_id, user_id))

        if cur.rowcount == 0:
            conn.rollback()
            return jsonify({
                'success': False,
                'message': '비상 연락처를 찾을 수 없습니다.'
            }), 404

        conn.commit()
        return jsonify({
            'success': True,
            'message': '비상 연락처가 삭제되었습니다.'
        }), 200

    except Exception as e:
        conn.rollback()
        return jsonify({
            'success': False,
            'message': f'비상 연락처 삭제 오류: {str(e)}'
        }), 500

    finally:
        cur.close()
        conn.close()


@app.route('/users/<int:user_id>/sos-requests', methods=['POST'])
def create_sos_request(user_id):
    data = request.get_json(silent=True) or {}

    try:
        latitude = float(data.get('latitude'))
        longitude = float(data.get('longitude'))
    except (TypeError, ValueError):
        return jsonify({
            'success': False,
            'message': '현재 위치 좌표가 올바르지 않습니다.'
        }), 400

    if not (-90 <= latitude <= 90 and -180 <= longitude <= 180):
        return jsonify({
            'success': False,
            'message': '현재 위치 좌표가 올바르지 않습니다.'
        }), 400

    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute(
            'SELECT TRIM(name) FROM users WHERE user_id = %s FOR UPDATE',
            (user_id,)
        )
        user = cur.fetchone()
        if user is None:
            conn.rollback()
            return jsonify({
                'success': False,
                'message': '사용자 정보를 찾을 수 없습니다.'
            }), 404

        cur.execute("""
            SELECT
                contact.contact_id,
                TRIM(contact.name),
                TRIM(contact.phone),
                recipient.user_id
            FROM emergency_contacts AS contact
            LEFT JOIN users AS recipient
              ON REGEXP_REPLACE(recipient.phone, '\\D', '', 'g') =
                 REGEXP_REPLACE(contact.phone, '\\D', '', 'g')
             AND recipient.user_id <> %s
            WHERE contact.user_id = %s
            ORDER BY contact.contact_id
        """, (user_id, user_id))
        contacts = cur.fetchall()
        if not contacts:
            conn.rollback()
            return jsonify({
                'success': False,
                'message': 'SOS를 보낼 비상 연락처를 먼저 등록해주세요.'
            }), 409

        recipient_ids = sorted({
            recipient_id
            for _, _, _, recipient_id in contacts
            if recipient_id is not None
        })
        if not recipient_ids:
            conn.rollback()
            return jsonify({
                'success': False,
                'message': '등록된 회원과 연결된 비상 연락처가 없습니다.'
            }), 409

        cur.execute("""
            INSERT INTO sos_requests (user_id, geom, status)
            VALUES (
                %s,
                ST_SetSRID(ST_MakePoint(%s, %s), 4326),
                'APP_QUEUED'
            )
            RETURNING sos_id, created_at
        """, (user_id, longitude, latitude))
        sos_id, created_at = cur.fetchone()

        for contact_id, _, _, recipient_id in contacts:
            cur.execute("""
                INSERT INTO sos_request_detail (
                    contact_id,
                    sos_id,
                    send_status,
                    sent_at,
                    read_at
                )
                VALUES (
                    %s,
                    %s,
                    %s,
                    CASE WHEN %s IS NULL THEN NULL ELSE CURRENT_TIMESTAMP END,
                    NULL
                )
            """, (
                contact_id,
                sos_id,
                'APP_QUEUED' if recipient_id is not None else 'NO_APP_USER',
                recipient_id,
            ))

        map_url = f'https://maps.google.com/?q={latitude:.7f},{longitude:.7f}'
        message = (
            f'{user[0]}님이 긴급 SOS를 요청했습니다. '
            f'현재 위치: {latitude:.6f}, {longitude:.6f} {map_url}'
        )
        for recipient_id in recipient_ids:
            cur.execute("""
                INSERT INTO alarm_logs (
                    user_id,
                    alarm_type,
                    content,
                    is_read
                )
                VALUES (%s, 'sos', %s, FALSE)
            """, (recipient_id, message))
        conn.commit()

        return jsonify({
            'success': True,
            'message': 'SOS 요청이 생성되었습니다.',
            'sos': {
                'sos_id': sos_id,
                'status': 'APP_QUEUED',
                'created_at': created_at.isoformat(),
                'recipient_count': len(recipient_ids),
                'unmatched_contact_count': sum(
                    1 for _, _, _, recipient_id in contacts
                    if recipient_id is None
                ),
            }
        }), 201

    except Exception as e:
        conn.rollback()
        return jsonify({
            'success': False,
            'message': f'SOS 요청 생성 오류: {str(e)}'
        }), 500

    finally:
        cur.close()
        conn.close()


@app.route('/users/<int:user_id>/alarm-logs/sos/pending', methods=['GET'])
def get_pending_sos_alarm_logs(user_id):
    after_log_id = request.args.get('after_log_id', default=0, type=int)
    after_log_id = max(0, after_log_id)
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute('SELECT 1 FROM users WHERE user_id = %s', (user_id,))
        if cur.fetchone() is None:
            return jsonify({
                'success': False,
                'message': '사용자 정보를 찾을 수 없습니다.'
            }), 404

        cur.execute("""
            SELECT log_id, TRIM(content), created_at
            FROM alarm_logs
            WHERE user_id = %s
              AND TRIM(alarm_type) = 'sos'
              AND log_id > %s
              AND created_at >= CURRENT_TIMESTAMP - INTERVAL '24 hours'
            ORDER BY log_id
            LIMIT 20
        """, (user_id, after_log_id))
        logs = [
            {
                'log_id': row[0],
                'content': row[1],
                'created_at': row[2].isoformat(),
            }
            for row in cur.fetchall()
        ]

        return jsonify({
            'success': True,
            'logs': logs,
        }), 200

    except Exception as e:
        conn.rollback()
        return jsonify({
            'success': False,
            'message': f'SOS 알림 조회 오류: {str(e)}'
        }), 500

    finally:
        cur.close()
        conn.close()


@app.route('/facilities', methods=['GET'])
def get_facilities():
    min_lat = request.args.get('min_lat', type=float)
    max_lat = request.args.get('max_lat', type=float)
    min_lng = request.args.get('min_lng', type=float)
    max_lng = request.args.get('max_lng', type=float)
    limit = request.args.get('limit', default=200, type=int)

    if None in (min_lat, max_lat, min_lng, max_lng):
        return jsonify({
            'success': False,
            'message': 'min_lat, max_lat, min_lng, max_lng are required.'
        }), 400

    if min_lat > max_lat or min_lng > max_lng:
        return jsonify({
            'success': False,
            'message': 'Invalid bounds: min values must be smaller than max values.'
        }), 400

    limit = max(1, min(limit, 500))

    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute("""
            SELECT
                facility_id,
                TRIM(type) AS type,
                weight_score,
                ST_Y(geom) AS lat,
                ST_X(geom) AS lng
            FROM safety_facilities
            WHERE TRIM(type) IN ('cctv', 'police_station')
              AND ST_Y(geom) BETWEEN %s AND %s
              AND ST_X(geom) BETWEEN %s AND %s
            ORDER BY TRIM(type), facility_id
            LIMIT %s
        """, (min_lat, max_lat, min_lng, max_lng, limit))

        rows = cur.fetchall()
        facilities = []
        for row in rows:
            facilities.append({
                'facility_id': row[0],
                'type': row[1],
                'weight_score': row[2],
                'lat': row[3],
                'lng': row[4],
            })

        return jsonify({
            'success': True,
            'facilities': facilities,
        }), 200

    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'시설물 조회 오류: {str(e)}'
        }), 500

    finally:
        cur.close()
        conn.close()


@app.route('/facilities/nearby', methods=['GET'])
def get_nearby_reportable_facilities():
    lat = request.args.get('lat', type=float)
    lng = request.args.get('lng', type=float)
    radius_m = request.args.get('radius_m', default=30.0, type=float)

    if lat is None or lng is None:
        return jsonify({
            'success': False,
            'message': 'lat and lng are required.'
        }), 400

    if not (-90 <= lat <= 90 and -180 <= lng <= 180):
        return jsonify({
            'success': False,
            'message': 'Invalid latitude or longitude.'
        }), 400

    radius_m = max(1.0, min(radius_m, 100.0))
    reportable_types = ['street_light', 'security_light']

    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute("""
            WITH current_position AS (
                SELECT ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography AS geom
            )
            SELECT
                facility.facility_id,
                TRIM(facility.type) AS type,
                ST_Y(facility.geom) AS lat,
                ST_X(facility.geom) AS lng,
                ST_Distance(
                    facility.geom::geography,
                    current_position.geom
                ) AS distance_m
            FROM safety_facilities AS facility
            CROSS JOIN current_position
            WHERE TRIM(facility.type) = ANY(%s)
              AND ST_DWithin(
                    facility.geom::geography,
                    current_position.geom,
                    %s
                  )
            ORDER BY distance_m, facility.facility_id
            LIMIT 100
        """, (lng, lat, reportable_types, radius_m))

        facilities = [
            {
                'facility_id': row[0],
                'type': row[1],
                'lat': row[2],
                'lng': row[3],
                'distance_m': round(float(row[4]), 2),
            }
            for row in cur.fetchall()
        ]

        return jsonify({
            'success': True,
            'radius_m': radius_m,
            'facilities': facilities,
        }), 200

    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'주변 시설물 조회 오류: {str(e)}'
        }), 500

    finally:
        cur.close()
        conn.close()


@app.route('/uploads/<path:filename>', methods=['GET'])
def get_uploaded_file(filename):
    return send_from_directory(UPLOAD_ROOT, filename)


@app.route('/reports', methods=['POST'])
def create_facility_report():
    user_id = request.form.get('user_id', type=int)
    facility_id = request.form.get('facility_id', type=int)
    report_type = (request.form.get('report_type') or '').strip()
    description = (request.form.get('description') or '').strip()
    image = request.files.get('image')

    if user_id is None or facility_id is None:
        return jsonify({
            'success': False,
            'message': 'user_id and facility_id are required.'
        }), 400

    if not report_type:
        return jsonify({
            'success': False,
            'message': '고장 유형을 선택해주세요.'
        }), 400

    if image is None or not image.filename:
        return jsonify({
            'success': False,
            'message': '고장 사진을 촬영해주세요.'
        }), 400

    extension = get_report_image_extension(image.filename)
    if extension is None:
        return jsonify({
            'success': False,
            'message': 'JPG, PNG, WEBP 형식의 사진만 업로드할 수 있습니다.'
        }), 400

    conn = get_db_connection()
    cur = conn.cursor()
    report_directory = None

    try:
        cur.execute("""
            INSERT INTO user_reports (
                user_id,
                facility_id,
                report_type,
                geom,
                status,
                description
            )
            SELECT
                %s,
                facility.facility_id,
                %s,
                facility.geom,
                'received',
                %s
            FROM safety_facilities AS facility
            WHERE facility.facility_id = %s
              AND TRIM(facility.type) IN ('street_light', 'security_light')
            RETURNING report_id
        """, (user_id, report_type, description or None, facility_id))

        inserted = cur.fetchone()
        if inserted is None:
            conn.rollback()
            return jsonify({
                'success': False,
                'message': '신고할 시설물을 찾을 수 없습니다.'
            }), 404

        report_id = inserted[0]
        report_directory = os.path.join(REPORT_UPLOAD_ROOT, str(report_id))
        os.makedirs(report_directory, exist_ok=True)

        filename = f'{uuid.uuid4().hex}{extension}'
        absolute_path = os.path.join(report_directory, filename)
        relative_path = os.path.join('reports', str(report_id), filename)
        image.save(absolute_path)

        cur.execute("""
            INSERT INTO reports_images (report_id, path)
            VALUES (%s, %s)
            RETURNING image_id
        """, (report_id, relative_path))
        image_id = cur.fetchone()[0]

        conn.commit()

        return jsonify({
            'success': True,
            'message': '고장 신고가 접수되었습니다.',
            'report': {
                'report_id': report_id,
                'facility_id': facility_id,
                'report_type': report_type,
                'description': description,
                'status': 'received',
                'image_id': image_id,
                'image_url': build_report_image_url(relative_path),
            }
        }), 201

    except psycopg2.errors.ForeignKeyViolation:
        conn.rollback()
        if report_directory and os.path.isdir(report_directory):
            shutil.rmtree(report_directory)
        return jsonify({
            'success': False,
            'message': '로그인 사용자 또는 시설물 정보를 확인할 수 없습니다.'
        }), 400

    except Exception as e:
        conn.rollback()
        if report_directory and os.path.isdir(report_directory):
            shutil.rmtree(report_directory)
        return jsonify({
            'success': False,
            'message': f'고장 신고 등록 오류: {str(e)}'
        }), 500

    finally:
        cur.close()
        conn.close()


@app.route('/users/<int:user_id>/reports', methods=['GET'])
def get_user_reports(user_id):
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute("""
            SELECT
                report.report_id,
                report.facility_id,
                COALESCE(TRIM(facility.type), 'unknown') AS facility_type,
                TRIM(report.report_type) AS report_type,
                TRIM(report.description) AS description,
                TRIM(report.status) AS status,
                report.created_at,
                report.completed_at,
                ST_Y(report.geom) AS lat,
                ST_X(report.geom) AS lng,
                image.path
            FROM user_reports AS report
            LEFT JOIN safety_facilities AS facility
              ON facility.facility_id = report.facility_id
            LEFT JOIN LATERAL (
                SELECT path
                FROM reports_images
                WHERE report_id = report.report_id
                ORDER BY image_id
                LIMIT 1
            ) AS image ON TRUE
            WHERE report.user_id = %s
            ORDER BY report.created_at DESC, report.report_id DESC
        """, (user_id,))

        reports = []
        for row in cur.fetchall():
            reports.append({
                'report_id': row[0],
                'facility_id': row[1],
                'facility_type': row[2],
                'report_type': row[3],
                'description': row[4] or '',
                'status': row[5],
                'created_at': row[6].isoformat() if row[6] else None,
                'completed_at': row[7].isoformat() if row[7] else None,
                'lat': row[8],
                'lng': row[9],
                'image_url': build_report_image_url(row[10]),
            })

        return jsonify({
            'success': True,
            'reports': reports,
        }), 200

    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'신고 내역 조회 오류: {str(e)}'
        }), 500

    finally:
        cur.close()
        conn.close()


@app.route('/admin/reports', methods=['GET'])
def get_admin_reports():
    admin_user_id = request.args.get('admin_user_id', type=int)
    status_group = (request.args.get('status') or 'received').strip().lower()
    status_filters = {
        'received': ['received', 'checking'],
        'approved': ['approved'],
        'history': ['rejected', 'completed'],
        'all': ['received', 'checking', 'approved', 'rejected', 'completed'],
    }

    if status_group not in status_filters:
        return jsonify({
            'success': False,
            'message': '올바르지 않은 신고 상태입니다.'
        }), 400

    conn = get_db_connection()
    cur = conn.cursor()

    try:
        if not is_admin_user(cur, admin_user_id):
            return jsonify({
                'success': False,
                'message': '관리자 권한이 필요합니다.'
            }), 403

        cur.execute("""
            SELECT
                report.report_id,
                report.user_id,
                report.facility_id,
                COALESCE(TRIM(facility.type), 'unknown') AS facility_type,
                TRIM(report.report_type) AS report_type,
                TRIM(report.description) AS description,
                TRIM(report.status) AS status,
                report.created_at,
                report.completed_at,
                image.path
            FROM user_reports AS report
            LEFT JOIN safety_facilities AS facility
              ON facility.facility_id = report.facility_id
            LEFT JOIN LATERAL (
                SELECT path
                FROM reports_images
                WHERE report_id = report.report_id
                ORDER BY image_id
                LIMIT 1
            ) AS image ON TRUE
            WHERE TRIM(report.status) = ANY(%s)
            ORDER BY report.created_at DESC, report.report_id DESC
        """, (status_filters[status_group],))

        reports = []
        for row in cur.fetchall():
            reports.append({
                'report_id': row[0],
                'user_id': row[1],
                'facility_id': row[2],
                'facility_type': row[3],
                'report_type': row[4],
                'description': row[5] or '',
                'status': row[6],
                'created_at': row[7].isoformat() if row[7] else None,
                'completed_at': row[8].isoformat() if row[8] else None,
                'image_url': build_report_image_url(row[9]),
            })

        return jsonify({
            'success': True,
            'reports': reports,
        }), 200

    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'관리자 신고 목록 조회 오류: {str(e)}'
        }), 500

    finally:
        cur.close()
        conn.close()


@app.route('/admin/reports/<int:report_id>', methods=['PATCH'])
def update_admin_report_status(report_id):
    data = request.get_json(silent=True) or {}
    admin_user_id = data.get('admin_user_id')
    action = (data.get('action') or '').strip().lower()
    allowed_transitions = {
        'approved': {'received', 'checking'},
        'rejected': {'received', 'checking'},
        'completed': {'approved'},
    }

    try:
        admin_user_id = int(admin_user_id)
    except (TypeError, ValueError):
        return jsonify({
            'success': False,
            'message': '관리자 정보가 필요합니다.'
        }), 400

    if action not in allowed_transitions:
        return jsonify({
            'success': False,
            'message': '올바르지 않은 처리 유형입니다.'
        }), 400

    conn = get_db_connection()
    cur = conn.cursor()

    try:
        if not is_admin_user(cur, admin_user_id):
            return jsonify({
                'success': False,
                'message': '관리자 권한이 필요합니다.'
            }), 403

        cur.execute("""
            SELECT TRIM(status)
            FROM user_reports
            WHERE report_id = %s
            FOR UPDATE
        """, (report_id,))
        report = cur.fetchone()

        if report is None:
            conn.rollback()
            return jsonify({
                'success': False,
                'message': '신고를 찾을 수 없습니다.'
            }), 404

        current_status = report[0]
        if current_status not in allowed_transitions[action]:
            conn.rollback()
            return jsonify({
                'success': False,
                'message': f'{current_status} 상태에서는 해당 처리를 할 수 없습니다.'
            }), 409

        cur.execute("""
            INSERT INTO report_reviews (report_id, action)
            VALUES (%s, %s)
        """, (report_id, action))

        cur.execute("""
            UPDATE user_reports
            SET
                status = %s,
                completed_at = CASE
                    WHEN %s = 'completed' THEN CURRENT_TIMESTAMP
                    ELSE completed_at
                END
            WHERE report_id = %s
            RETURNING completed_at
        """, (action, action, report_id))
        completed_at = cur.fetchone()[0]
        conn.commit()

        return jsonify({
            'success': True,
            'message': '신고 상태가 변경되었습니다.',
            'report': {
                'report_id': report_id,
                'status': action,
                'completed_at': completed_at.isoformat() if completed_at else None,
            }
        }), 200

    except Exception as e:
        conn.rollback()
        return jsonify({
            'success': False,
            'message': f'신고 상태 변경 오류: {str(e)}'
        }), 500

    finally:
        cur.close()
        conn.close()


@app.route('/reports/map', methods=['GET'])
def get_report_map_markers():
    min_lat = request.args.get('min_lat', type=float)
    max_lat = request.args.get('max_lat', type=float)
    min_lng = request.args.get('min_lng', type=float)
    max_lng = request.args.get('max_lng', type=float)
    limit = request.args.get('limit', default=300, type=int)

    if None in (min_lat, max_lat, min_lng, max_lng):
        return jsonify({
            'success': False,
            'message': 'min_lat, max_lat, min_lng and max_lng are required.'
        }), 400

    if min_lat > max_lat or min_lng > max_lng:
        return jsonify({
            'success': False,
            'message': 'Invalid bounds.'
        }), 400

    limit = max(1, min(limit, 1000))
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute("""
            SELECT
                report.report_id,
                report.facility_id,
                TRIM(facility.type) AS facility_type,
                TRIM(report.report_type) AS report_type,
                TRIM(report.description) AS description,
                TRIM(report.status) AS status,
                report.created_at,
                ST_Y(report.geom) AS lat,
                ST_X(report.geom) AS lng,
                image.path
            FROM user_reports AS report
            JOIN safety_facilities AS facility
              ON facility.facility_id = report.facility_id
            LEFT JOIN LATERAL (
                SELECT path
                FROM reports_images
                WHERE report_id = report.report_id
                ORDER BY image_id
                LIMIT 1
            ) AS image ON TRUE
            WHERE TRIM(report.status) = 'approved'
              AND report.expired_at IS NULL
              AND ST_Y(report.geom) BETWEEN %s AND %s
              AND ST_X(report.geom) BETWEEN %s AND %s
            ORDER BY report.created_at DESC
            LIMIT %s
        """, (min_lat, max_lat, min_lng, max_lng, limit))

        reports = []
        for row in cur.fetchall():
            reports.append({
                'report_id': row[0],
                'facility_id': row[1],
                'facility_type': row[2],
                'report_type': row[3],
                'description': row[4] or '',
                'status': row[5],
                'created_at': row[6].isoformat() if row[6] else None,
                'lat': row[7],
                'lng': row[8],
                'image_url': build_report_image_url(row[9]),
            })

        return jsonify({
            'success': True,
            'reports': reports,
        }), 200

    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'신고 지도 조회 오류: {str(e)}'
        }), 500

    finally:
        cur.close()
        conn.close()


@app.route('/roads/sample', methods=['GET'])
def get_sample_roads():
    limit = request.args.get('limit', default=100, type=int)
    limit = max(1, min(limit, 500))

    # 청주시청 근처 도로 몇 개만 테스트용으로 내려준다.
    center_lng = 127.4890
    center_lat = 36.6424

    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute("""
            WITH center AS (
                SELECT ST_SetSRID(ST_MakePoint(%s, %s), 4326) AS geom
            )
            SELECT
                road.edge_id,
                road.safety_score,
                road.cost,
                ST_AsGeoJSON(
                    ST_SimplifyPreserveTopology(road.geom, 0.00001),
                    6
                ) AS geojson
            FROM osm_edges AS road, center
            ORDER BY ST_Transform(road.geom, 3857) <-> ST_Transform(center.geom, 3857)
            LIMIT %s
        """, (center_lng, center_lat, limit))

        roads = []
        for row in cur.fetchall():
            geometry = json.loads(row[3])
            coords = [
                {'lat': point[1], 'lng': point[0]}
                for point in geometry.get('coordinates', [])
            ]

            roads.append({
                'road_id': row[0],
                'safety_score': row[1],
                'cost': row[2],
                'coords': coords,
            })

        return jsonify({
            'success': True,
            'roads': roads,
        }), 200

    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'도로 조회 오류: {str(e)}'
        }), 500

    finally:
        cur.close()
        conn.close()


@app.route('/roads', methods=['GET'])
def get_roads():
    min_lat = request.args.get('min_lat', type=float)
    max_lat = request.args.get('max_lat', type=float)
    min_lng = request.args.get('min_lng', type=float)
    max_lng = request.args.get('max_lng', type=float)
    limit = request.args.get('limit', default=5000, type=int)

    if None in (min_lat, max_lat, min_lng, max_lng):
        return jsonify({
            'success': False,
            'message': 'min_lat, max_lat, min_lng, max_lng are required.'
        }), 400

    if min_lat > max_lat or min_lng > max_lng:
        return jsonify({
            'success': False,
            'message': 'Invalid bounds: min values must be smaller than max values.'
        }), 400

    limit = max(1, min(limit, 5000))

    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute("""
            WITH bounds AS (
                SELECT ST_MakeEnvelope(%s, %s, %s, %s, 4326) AS geom
            )
            SELECT
                road.edge_id,
                road.safety_score,
                road.cost,
                ST_AsGeoJSON(
                    ST_SimplifyPreserveTopology(road.geom, 0.00001),
                    6
                ) AS geojson
            FROM osm_edges AS road
            CROSS JOIN bounds
            WHERE road.geom && bounds.geom
              AND ST_Intersects(road.geom, bounds.geom)
            ORDER BY road.edge_id
            LIMIT %s
        """, (min_lng, min_lat, max_lng, max_lat, limit))

        roads = []
        for row in cur.fetchall():
            geometry = json.loads(row[3])
            coords = [
                {'lat': point[1], 'lng': point[0]}
                for point in geometry.get('coordinates', [])
            ]

            if len(coords) < 2:
                continue

            roads.append({
                'road_id': row[0],
                'safety_score': row[1],
                'cost': row[2],
                'coords': coords,
            })

        return jsonify({
            'success': True,
            'roads': roads,
        }), 200

    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'?꾨줈 議고쉶 ?ㅻ쪟: {str(e)}'
        }), 500

    finally:
        cur.close()
        conn.close()


@app.route('/roads/nearest-risk', methods=['GET'])
def get_nearest_risk_road():
    lat = request.args.get('lat', type=float)
    lng = request.args.get('lng', type=float)
    radius_m = request.args.get('radius_m', default=25, type=float)
    threshold = request.args.get('threshold', default=50, type=float)

    if lat is None or lng is None:
        return jsonify({
            'success': False,
            'message': 'lat, lng are required.'
        }), 400

    radius_m = max(1, min(radius_m, 100))
    threshold = max(0, min(threshold, 100))

    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute("""
            WITH current_point AS (
                SELECT ST_SetSRID(ST_MakePoint(%s, %s), 4326) AS geom
            ),
            nearest AS (
                SELECT
                    road.edge_id,
                    road.safety_score,
                    road.cost,
                    ST_Distance(
                        ST_Transform(road.geom, 3857),
                        ST_Transform(current_point.geom, 3857)
                    ) AS distance_m
                FROM osm_edges AS road
                CROSS JOIN current_point
                WHERE road.geom IS NOT NULL
                ORDER BY ST_Transform(road.geom, 3857)
                    <-> ST_Transform(current_point.geom, 3857)
                LIMIT 1
            )
            SELECT edge_id, safety_score, cost, distance_m
            FROM nearest
        """, (lng, lat))

        row = cur.fetchone()

        if row is None:
            return jsonify({
                'success': True,
                'is_risky': False,
                'road': None,
            }), 200

        safety_score = float(row[1] or 0)
        distance_m = float(row[3] or 0)
        is_risky = distance_m <= radius_m and safety_score < threshold

        if safety_score < 40:
            risk_level = 'high'
        elif safety_score < 50:
            risk_level = 'medium'
        else:
            risk_level = 'low'

        return jsonify({
            'success': True,
            'is_risky': is_risky,
            'road': {
                'road_id': row[0],
                'safety_score': safety_score,
                'cost': row[2],
                'distance_m': round(distance_m, 1),
                'risk_level': risk_level,
            },
        }), 200

    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'위험구역 조회 오류: {str(e)}'
        }), 500

    finally:
        cur.close()
        conn.close()


@app.route('/users/<int:user_id>/favorites', methods=['GET'])
def get_favorites(user_id):
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute("""
            SELECT
                favorite_id,
                TRIM(alias),
                ST_Y(geom),
                ST_X(geom)
            FROM favorites
            WHERE user_id = %s
            ORDER BY favorite_id DESC
        """, (user_id,))
        favorites = [
            {
                'favorite_id': row[0],
                'alias': row[1],
                'lat': row[2],
                'lng': row[3],
            }
            for row in cur.fetchall()
        ]
        return jsonify({'success': True, 'favorites': favorites}), 200
    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'즐겨찾기 조회 오류: {str(e)}'
        }), 500
    finally:
        cur.close()
        conn.close()


@app.route('/users/<int:user_id>/favorites', methods=['POST'])
def create_favorite(user_id):
    data = request.get_json() or {}
    alias = str(data.get('alias') or '').strip()
    lat = data.get('lat')
    lng = data.get('lng')

    if not alias or lat is None or lng is None:
        return jsonify({
            'success': False,
            'message': '장소 이름과 좌표를 입력해주세요.'
        }), 400

    if len(alias) > 50:
        return jsonify({
            'success': False,
            'message': '장소 이름은 50자 이내로 입력해주세요.'
        }), 400

    try:
        lat = float(lat)
        lng = float(lng)
    except (TypeError, ValueError):
        return jsonify({'success': False, 'message': '좌표 형식이 올바르지 않습니다.'}), 400

    if not (-90 <= lat <= 90 and -180 <= lng <= 180):
        return jsonify({'success': False, 'message': '좌표 범위가 올바르지 않습니다.'}), 400

    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute("""
            INSERT INTO favorites (user_id, alias, geom)
            VALUES (
                %s,
                %s,
                ST_SetSRID(ST_MakePoint(%s, %s), 4326)
            )
            RETURNING favorite_id
        """, (user_id, alias, lng, lat))
        favorite_id = cur.fetchone()[0]
        conn.commit()
        return jsonify({
            'success': True,
            'message': '즐겨찾기에 추가했습니다.',
            'favorite_id': favorite_id,
        }), 201
    except Exception as e:
        conn.rollback()
        return jsonify({
            'success': False,
            'message': f'즐겨찾기 추가 오류: {str(e)}'
        }), 500
    finally:
        cur.close()
        conn.close()


@app.route('/users/<int:user_id>/favorites/<int:favorite_id>', methods=['DELETE'])
def delete_favorite(user_id, favorite_id):
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute("""
            DELETE FROM favorites
            WHERE favorite_id = %s AND user_id = %s
        """, (favorite_id, user_id))
        if cur.rowcount == 0:
            conn.rollback()
            return jsonify({
                'success': False,
                'message': '즐겨찾기를 찾을 수 없습니다.'
            }), 404

        conn.commit()
        return jsonify({'success': True, 'message': '즐겨찾기를 삭제했습니다.'}), 200
    except Exception as e:
        conn.rollback()
        return jsonify({
            'success': False,
            'message': f'즐겨찾기 삭제 오류: {str(e)}'
        }), 500
    finally:
        cur.close()
        conn.close()


def find_nearest_road_snap_candidates(cur, lat, lng, limit=12):
    cur.execute(f"""
        WITH point AS (
            SELECT ST_Transform(ST_SetSRID(ST_MakePoint(%s, %s), 4326), 3857) AS geom
        ),
        snapped_roads AS (
            SELECT
                road.edge_id,
                road.source_node_id,
                road.target_node_id,
                road.geom,
                ST_ClosestPoint(road.geom, point.geom) AS snap_geom,
                ST_LineLocatePoint(road.geom, point.geom) AS snap_fraction,
                ST_Distance(
                    ST_ClosestPoint(road.geom, point.geom),
                    point.geom
                ) AS distance_m
            FROM {ROUTE_EDGE_TABLE} AS road
            CROSS JOIN point
            WHERE road.source_node_id IS NOT NULL
              AND road.target_node_id IS NOT NULL
              AND road.geom IS NOT NULL
            ORDER BY road.geom <-> point.geom
            LIMIT %s
        )
        SELECT
            edge_id,
            source_node_id,
            target_node_id,
            ST_Y(ST_Transform(snap_geom, 4326)) AS snap_lat,
            ST_X(ST_Transform(snap_geom, 4326)) AS snap_lng,
            distance_m,
            ST_Length(
                ST_LineSubstring(geom, 0, snap_fraction)
            ) AS source_connector_length_m,
            ST_AsGeoJSON(
                ST_Transform(
                    ST_Reverse(ST_LineSubstring(geom, 0, snap_fraction)),
                    4326
                ),
                6
            ) AS source_connector_geojson,
            ST_Length(
                ST_LineSubstring(geom, snap_fraction, 1)
            ) AS target_connector_length_m,
            ST_AsGeoJSON(
                ST_Transform(ST_LineSubstring(geom, snap_fraction, 1), 4326),
                6
            ) AS target_connector_geojson
        FROM snapped_roads
    """, (lng, lat, limit))

    candidates = []

    for row in cur.fetchall():
        edge_id = row[0]
        source_node_id = row[1]
        target_node_id = row[2]
        snap_point = {'lat': row[3], 'lng': row[4]}
        distance_m = float(row[5])
        source_connector_length_m = float(row[6] or 0)
        source_connector_coords = geojson_to_latlng_list(row[7])
        target_connector_length_m = float(row[8] or 0)
        target_connector_coords = geojson_to_latlng_list(row[9])

        candidates.append({
            'edge_id': edge_id,
            'node_id': source_node_id,
            'snap_point': snap_point,
            'distance_m': distance_m,
            'connector_length_m': source_connector_length_m,
            'connector_coords': source_connector_coords,
        })
        candidates.append({
            'edge_id': edge_id,
            'node_id': target_node_id,
            'snap_point': snap_point,
            'distance_m': distance_m,
            'connector_length_m': target_connector_length_m,
            'connector_coords': target_connector_coords,
        })

    return candidates


def geojson_to_latlng_list(geojson_text):
    geometry = json.loads(geojson_text)
    geometry_type = geometry.get('type')
    coordinates = geometry.get('coordinates', [])

    if geometry_type == 'Point':
        return [{'lat': coordinates[1], 'lng': coordinates[0]}]

    return [
        {'lat': point[1], 'lng': point[0]}
        for point in coordinates
    ]


def load_route_graph(cur, safety_weight, report_weight):
    cur.execute(f"""
        WITH active_report_edges AS (
            SELECT DISTINCT nearest_edge.edge_id
            FROM user_reports AS report
            CROSS JOIN LATERAL (
                SELECT edge.edge_id
                FROM {ROUTE_EDGE_TABLE} AS edge
                WHERE edge.geom IS NOT NULL
                  AND ST_DWithin(
                      edge.geom,
                      ST_Transform(report.geom, ST_SRID(edge.geom)),
                      %s
                  )
                ORDER BY edge.geom <-> ST_Transform(
                    report.geom,
                    ST_SRID(edge.geom)
                )
                LIMIT 1
            ) AS nearest_edge
            WHERE TRIM(report.status) = 'approved'
              AND report.geom IS NOT NULL
        )
        SELECT
            edge.edge_id,
            edge.source_node_id,
            edge.target_node_id,
            edge.length_m,
            (
                edge.length_m * (
                    1 + (((100 - COALESCE(edge.safety_score, 40))::double precision / 100) * %s)
                ) * (
                    1 + (CASE WHEN active.edge_id IS NULL THEN 0 ELSE %s END)
                )
            ) AS cost,
            edge.safety_score,
            ST_AsGeoJSON(ST_Transform(edge.geom, 4326), 6) AS geojson,
            active.edge_id IS NOT NULL AS has_active_report
        FROM {ROUTE_EDGE_TABLE} AS edge
        LEFT JOIN active_report_edges AS active
          ON active.edge_id = edge.edge_id
        WHERE edge.source_node_id IS NOT NULL
          AND edge.target_node_id IS NOT NULL
          AND edge.geom IS NOT NULL
          AND edge.length_m IS NOT NULL
    """, (REPORT_ROUTE_MATCH_RADIUS_M, safety_weight, report_weight))

    graph = {}

    for row in cur.fetchall():
        edge_id = row[0]
        source_node_id = row[1]
        target_node_id = row[2]
        length_m = float(row[3] or 0)
        cost = float(row[4] or length_m or 1)
        safety_score = row[5]
        geometry = json.loads(row[6])
        has_active_report = bool(row[7])
        coordinates = geometry.get('coordinates', [])

        if len(coordinates) < 2:
            continue

        if cost <= 0:
            cost = max(length_m, 1)

        forward_coords = [
            {'lat': point[1], 'lng': point[0]}
            for point in coordinates
        ]
        reverse_coords = list(reversed(forward_coords))

        forward_edge = {
            'edge_id': edge_id,
            'to': target_node_id,
            'cost': cost,
            'length_m': length_m,
            'safety_score': safety_score,
            'has_active_report': has_active_report,
            'coords': forward_coords,
        }
        reverse_edge = {
            'edge_id': edge_id,
            'to': source_node_id,
            'cost': cost,
            'length_m': length_m,
            'safety_score': safety_score,
            'has_active_report': has_active_report,
            'coords': reverse_coords,
        }

        graph.setdefault(source_node_id, []).append(forward_edge)
        graph.setdefault(target_node_id, []).append(reverse_edge)

    return graph


def find_shortest_path(graph, start_node_id, end_node_id):
    distances = {start_node_id: 0.0}
    previous = {}
    queue = [(0.0, start_node_id)]
    visited = set()

    while queue:
        current_cost, current_node_id = heapq.heappop(queue)

        if current_node_id in visited:
            continue

        visited.add(current_node_id)

        if current_node_id == end_node_id:
            break

        for edge in graph.get(current_node_id, []):
            next_node_id = edge['to']
            next_cost = current_cost + edge['cost']

            if next_cost < distances.get(next_node_id, math.inf):
                distances[next_node_id] = next_cost
                previous[next_node_id] = (current_node_id, edge)
                heapq.heappush(queue, (next_cost, next_node_id))

    if end_node_id not in distances:
        return None

    route_edges = []
    node_id = end_node_id

    while node_id != start_node_id:
        previous_item = previous.get(node_id)

        if previous_item is None:
            return None

        prev_node_id, edge = previous_item
        route_edges.append(edge)
        node_id = prev_node_id

    route_edges.reverse()
    return route_edges, distances[end_node_id]


def build_component_index(graph):
    component_by_node = {}
    component_id = 0

    for start_node_id in graph:
        if start_node_id in component_by_node:
            continue

        stack = [start_node_id]
        component_by_node[start_node_id] = component_id

        while stack:
            node_id = stack.pop()

            for edge in graph.get(node_id, []):
                next_node_id = edge['to']

                if next_node_id not in component_by_node:
                    component_by_node[next_node_id] = component_id
                    stack.append(next_node_id)

        component_id += 1

    return component_by_node


def find_best_route_for_snaps(start_candidates, end_candidates, graph):
    component_by_node = build_component_index(graph)
    best_route = None
    best_total_cost = math.inf

    for start_candidate in start_candidates:
        start_component = component_by_node.get(start_candidate['node_id'])
        if start_component is None:
            continue

        for end_candidate in end_candidates:
            if component_by_node.get(end_candidate['node_id']) != start_component:
                continue

            result = find_shortest_path(
                graph,
                start_candidate['node_id'],
                end_candidate['node_id'],
            )

            if result is None:
                continue

            route_edges, route_cost = result
            total_cost = (
                (start_candidate['distance_m'] * SNAP_DISTANCE_WEIGHT) +
                start_candidate['connector_length_m'] +
                route_cost +
                end_candidate['connector_length_m'] +
                (end_candidate['distance_m'] * SNAP_DISTANCE_WEIGHT)
            )

            if total_cost < best_total_cost:
                best_total_cost = total_cost
                best_route = (
                    start_candidate,
                    end_candidate,
                    route_edges,
                    total_cost,
                )

    return best_route


def merge_route_coordinates(route_edges):
    path = []

    for edge in route_edges:
        coords = edge['coords']
        if not coords:
            continue

        if path and path[-1] == coords[0]:
            path.extend(coords[1:])
        else:
            path.extend(coords)

    return path


def merge_path_parts(*parts):
    path = []

    for coords in parts:
        for point in coords:
            if path and path[-1] == point:
                continue
            path.append(point)

    return path


@app.route('/route', methods=['GET'])
def get_route():
    start_lat = request.args.get('s_lat', type=float)
    start_lng = request.args.get('s_lng', type=float)
    end_lat = request.args.get('e_lat', type=float)
    end_lng = request.args.get('e_lng', type=float)
    route_mode = request.args.get('mode', default='safe').strip().lower()

    if None in (start_lat, start_lng, end_lat, end_lng):
        return jsonify({
            'success': False,
            'message': 's_lat, s_lng, e_lat, e_lng are required.'
        }), 400

    if route_mode not in ROUTE_MODE_CONFIG:
        return jsonify({
            'success': False,
            'message': 'mode는 fast 또는 safe만 사용할 수 있습니다.'
        }), 400

    conn = get_db_connection()
    cur = conn.cursor()

    try:
        start_candidates = find_nearest_road_snap_candidates(
            cur,
            start_lat,
            start_lng,
        )
        end_candidates = find_nearest_road_snap_candidates(
            cur,
            end_lat,
            end_lng,
        )

        if not start_candidates or not end_candidates:
            return jsonify({
                'success': False,
                'message': '가까운 도로 노드를 찾을 수 없습니다.'
            }), 404

        mode_config = ROUTE_MODE_CONFIG[route_mode]
        graph = load_route_graph(
            cur,
            mode_config['safety_weight'],
            mode_config['report_weight'],
        )
        route_result = find_best_route_for_snaps(
            start_candidates,
            end_candidates,
            graph,
        )

        if route_result is None:
            return jsonify({
                'success': False,
                'message': '출발지와 도착지 주변에서 서로 연결된 도로 노드를 찾을 수 없습니다.',
                'nearest_start_snap': start_candidates[0],
                'nearest_end_snap': end_candidates[0],
            }), 404

        start_snap, end_snap, route_edges, total_cost = route_result
        road_path = merge_route_coordinates(route_edges)
        path = merge_path_parts(
            start_snap['connector_coords'],
            road_path,
            list(reversed(end_snap['connector_coords'])),
        )
        total_distance_m = (
            start_snap['connector_length_m'] +
            sum(edge['length_m'] for edge in route_edges) +
            end_snap['connector_length_m']
        )
        safety_scores = [
            float(edge['safety_score'])
            for edge in route_edges
            if edge['safety_score'] is not None
        ]
        average_safety_score = (
            sum(safety_scores) / len(safety_scores)
            if safety_scores
            else None
        )
        active_report_edge_count = sum(
            1 for edge in route_edges if edge['has_active_report']
        )

        return jsonify({
            'success': True,
            'mode': route_mode,
            'mode_label': mode_config['label'],
            'start_node': {
                'node_id': start_snap['node_id'],
                'distance_m': start_snap['distance_m'],
            },
            'end_node': {
                'node_id': end_snap['node_id'],
                'distance_m': end_snap['distance_m'],
            },
            'distance_m': round(total_distance_m, 1),
            'cost': round(total_cost, 1),
            'average_safety_score': (
                round(average_safety_score, 1)
                if average_safety_score is not None
                else None
            ),
            'edge_count': len(route_edges),
            'active_report_edge_count': active_report_edge_count,
            'snapped_start_point': start_snap['snap_point'],
            'snapped_end_point': end_snap['snap_point'],
            'path': path,
        }), 200

    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'경로 계산 오류: {str(e)}'
        }), 500

    finally:
        cur.close()
        conn.close()


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
