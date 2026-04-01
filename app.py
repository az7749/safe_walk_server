from flask import Flask, request, jsonify
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


@app.route('/signup', methods=['POST'])
def signup():
    data = request.get_json()

    if not data:
        return jsonify({
            'success': False,
            'message': '요청 데이터가 없습니다.'
        }), 400

    user_id = data.get('user_id')
    password = data.get('password')
    name = data.get('name')

    if not user_id or not password or not name:
        return jsonify({
            'success': False,
            'message': '이름, 아이디, 비밀번호를 모두 입력해주세요.'
        }), 400

    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute('SELECT * FROM users WHERE user_id = %s', (user_id,))
        existing_user = cur.fetchone()

        if existing_user:
            return jsonify({
                'success': False,
                'message': '이미 존재하는 아이디입니다.'
            }), 409

        hashed_password = generate_password_hash(password)

        cur.execute(
            'INSERT INTO users (user_id, password, name) VALUES (%s, %s, %s)',
            (user_id, hashed_password, name)
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