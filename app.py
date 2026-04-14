from flask import Flask, request, jsonify
from flask_cors import CORS
import psycopg2
from werkzeug.security import generate_password_hash, check_password_hash

app = Flask(__name__)
# 주석: Flutter 지도 화면에서 현재 화면 범위 기준으로 재조회할 수 있도록 CORS 허용
CORS(app)

DB_CONFIG = {
    'host': 'localhost',
    # 'host': '192.168.35.20',
    'dbname': 'night_safe_walk',
    'user': 'postgres',
    'password': '0000',
    'port': 5432
}


def get_db_connection():
    conn = psycopg2.connect(
        host=DB_CONFIG['host'],
        dbname=DB_CONFIG['dbname'],
        user=DB_CONFIG['user'],
        password=DB_CONFIG['password'],
        port=DB_CONFIG['port']
    )
    return conn


@app.route('/')
def home():
    return 'PostgreSQL Flask server is running!'

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
    phone = data.get('phone')
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
            'SELECT user_id, login_id, password, name FROM users WHERE login_id = %s',
            (login_id,)
        )
        user = cur.fetchone()
        print("db user:", user)

        if user is None:
            return jsonify({
                'success': False,
                'message': '아이디 또는 비밀번호를 다시 확인하세요.'
            }), 401

        db_user_id, db_login_id, db_password, db_name = user

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
                'name': db_name
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

@app.route('/facilities', methods=['GET'])
def get_facilities():
    # 주석: 현재 화면 bounds 안에 있는 CCTV만 조회하는 API
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
            WHERE TRIM(type) = 'cctv'
              AND ST_Y(geom) BETWEEN %s AND %s
              AND ST_X(geom) BETWEEN %s AND %s
            ORDER BY facility_id
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


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
