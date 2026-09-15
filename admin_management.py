import math

from flask import Blueprint, current_app, jsonify, request


def create_admin_management(get_connection, is_admin, normalize_phone):
    blueprint = Blueprint('admin_management', __name__)

    @blueprint.route('/admin/<resource>', methods=['GET'])
    @blueprint.route('/admin/<resource>/<int:item_id>', methods=['PATCH'])
    def manage(resource, item_id=None):
        if resource not in ('users', 'facilities'):
            return jsonify(success=False, message='Not found'), 404
        data = (request.get_json(silent=True) or {}) if item_id is not None else request.args
        if not hasattr(data, 'get'):
            return jsonify(success=False, message='요청 형식이 올바르지 않습니다.'), 400
        try:
            admin_id = int(data.get('admin_user_id'))
        except (TypeError, ValueError):
            return jsonify(success=False, message='관리자 정보가 필요합니다.'), 403

        conn = get_connection()
        try:
            with conn.cursor() as cur:
                if not is_admin(cur, admin_id):
                    return jsonify(success=False, message='관리자 권한이 필요합니다.'), 403
                if item_id is None:
                    try:
                        page = max(0, int(request.args.get('page', 0)))
                    except ValueError:
                        return jsonify(success=False, message='페이지가 올바르지 않습니다.'), 400
                    query = request.args.get('query', '').strip()[:100]
                    pattern = '%' + query.replace('\\', '\\\\').replace('%', '\\%').replace('_', '\\_') + '%'
                    if resource == 'users':
                        cur.execute('''
                            SELECT user_id AS id, TRIM(login_id) AS login_id,
                                   TRIM(name) AS name, TRIM(phone) AS phone,
                                   birth_date, TRIM(gender) AS gender,
                                   TRIM(role) AS role, created_at
                            FROM users
                            WHERE login_id ILIKE %s OR name ILIKE %s OR phone ILIKE %s
                            ORDER BY user_id LIMIT 51 OFFSET %s
                        ''', (pattern, pattern, pattern, page * 50))
                    else:
                        cur.execute('''
                            SELECT facility_id AS id, TRIM(type) AS type,
                                   ST_Y(geom) AS lat, ST_X(geom) AS lng
                            FROM safety_facilities
                            WHERE type ILIKE %s OR facility_id::text ILIKE %s
                            ORDER BY facility_id LIMIT 51 OFFSET %s
                        ''', (pattern, pattern, page * 50))
                    columns = [column.name for column in cur.description]
                    rows = cur.fetchall()
                    items = []
                    for row in rows[:50]:
                        items.append({key: value.isoformat() if hasattr(value, 'isoformat') else value
                                      for key, value in zip(columns, row)})
                    return jsonify(success=True, items=items, has_more=len(rows) > 50)

                if resource == 'users':
                    name = data.get('name')
                    phone_value = data.get('phone')
                    if not isinstance(name, str) or not name.strip() or len(name.strip()) > 50:
                        return jsonify(success=False, message='이름은 1~50자로 입력해주세요.'), 400
                    phone = normalize_phone(phone_value) if isinstance(phone_value, str) else None
                    if phone is None:
                        return jsonify(success=False, message='올바른 휴대전화 번호를 입력해주세요.'), 400
                    cur.execute('UPDATE users SET name = %s, phone = %s WHERE user_id = %s',
                                (name.strip(), phone, item_id))
                else:
                    facility_type = data.get('type')
                    if not isinstance(facility_type, str) or not facility_type.strip() or len(facility_type.strip()) > 50:
                        return jsonify(success=False, message='시설물 종류를 확인해주세요.'), 400
                    try:
                        lat, lng = float(data.get('lat')), float(data.get('lng'))
                        if not math.isfinite(lat) or not math.isfinite(lng) or not (-90 <= lat <= 90 and -180 <= lng <= 180):
                            raise ValueError()
                    except (ValueError, TypeError):
                        return jsonify(success=False, message='올바른 위도와 경도를 입력해주세요.'), 400
                    cur.execute('SELECT 1 FROM safety_facilities WHERE TRIM(type) = %s LIMIT 1',
                                (facility_type.strip(),))
                    if cur.fetchone() is None:
                        return jsonify(success=False, message='등록된 시설물 종류를 선택해주세요.'), 400
                    cur.execute('''UPDATE safety_facilities SET type = %s,
                        geom = ST_SetSRID(ST_MakePoint(%s, %s), 4326) WHERE facility_id = %s''',
                                (facility_type.strip(), lng, lat, item_id))
                if cur.rowcount == 0:
                    return jsonify(success=False, message='대상을 찾을 수 없습니다.'), 404
                conn.commit()
                return jsonify(success=True, message='정보가 수정되었습니다.')
        except Exception:
            conn.rollback()
            current_app.logger.exception('Admin management failed')
            return jsonify(success=False, message='관리자 요청을 처리하지 못했습니다.'), 500
        finally:
            conn.close()

    @blueprint.get('/admin/facility-types')
    def facility_types():
        admin_id = request.args.get('admin_user_id', type=int)
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                if not is_admin(cur, admin_id):
                    return jsonify(success=False, message='관리자 권한이 필요합니다.'), 403
                cur.execute('SELECT DISTINCT TRIM(type) FROM safety_facilities ORDER BY 1')
                return jsonify(success=True, types=[row[0] for row in cur.fetchall()])
        finally:
            conn.close()

    return blueprint
