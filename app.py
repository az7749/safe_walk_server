from flask import Flask, request, jsonify
from flask_cors import CORS
import psycopg2
from werkzeug.security import generate_password_hash, check_password_hash

app = Flask(__name__)

DB_CONFIG = {
    'host': 'localhost',
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
            (login_id, name, phone, birth_date, gender, password)
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

    if not data:
        return jsonify({
            'success': False,
            'message': '요청 데이터가 없습니다.'
        }), 400

    user_id = data.get('user_id')
    password = data.get('password')

    if not user_id or not password:
        return jsonify({
            'success': False,
            'message': '아이디와 비밀번호를 입력해주세요.'
        }), 400

    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute(
            'SELECT id, user_id, password, name FROM users WHERE user_id = %s',
            (user_id,)
        )
        user = cur.fetchone()

        if user is None:
            return jsonify({
                'success': False,
                'message': '존재하지 않는 아이디입니다.'
            }), 401

        db_id, db_user_id, db_password, db_name = user

        if not check_password_hash(db_password, password):
            return jsonify({
                'success': False,
                'message': '비밀번호가 올바르지 않습니다.'
            }), 401

        return jsonify({
            'success': True,
            'message': '로그인 성공',
            'user': {
                'id': db_id,
                'user_id': db_user_id,
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


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)