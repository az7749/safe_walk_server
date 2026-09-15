import unittest

from flask import Flask
import app as server
from admin_management import create_admin_management


class AdminManagementTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.conn = server.get_db_connection()
        cls.conn.autocommit = True
        with cls.conn.cursor() as cur:
            # Session-local fixtures shadow production tables and never change them.
            cur.execute('''CREATE TEMP TABLE users (
                user_id BIGINT PRIMARY KEY, login_id TEXT, name TEXT, phone TEXT,
                birth_date DATE, gender TEXT, role TEXT, created_at TIMESTAMP)''')
            cur.execute('''CREATE TEMP TABLE safety_facilities (
                facility_id BIGINT PRIMARY KEY, type TEXT, geom geometry(Point,4326))''')
            cur.execute("INSERT INTO users VALUES (1,'admin','Admin','010-1111-1111',NULL,NULL,'admin',NOW()), (2,'member','Member','010-2222-2222',NULL,NULL,'user',NOW())")
            cur.execute("INSERT INTO safety_facilities VALUES (1,'street_light',ST_SetSRID(ST_MakePoint(127,36),4326)), (2,'security_light',ST_SetSRID(ST_MakePoint(127,36),4326))")
        class Connection:
            def cursor(self): return cls.conn.cursor()
            def commit(self): pass
            def rollback(self): pass
            def close(self): pass
        test_app = Flask(__name__)
        test_app.register_blueprint(create_admin_management(Connection, server.is_admin_user, server.normalize_mobile_phone))
        cls.client = test_app.test_client()

    @classmethod
    def tearDownClass(cls):
        cls.conn.close()

    def test_permissions(self):
        for path in ('users', 'facilities', 'facility-types'):
            self.assertEqual(self.client.get('/admin/' + path).status_code, 403)
            self.assertEqual(self.client.get('/admin/' + path + '?admin_user_id=2').status_code, 403)
        for path in ('users/2', 'facilities/1'):
            self.assertEqual(self.client.patch('/admin/' + path, json={'admin_user_id': 2}).status_code, 403)

    def test_member_save_and_search(self):
        response = self.client.patch('/admin/users/2', json={'admin_user_id': 1, 'name': 'Updated', 'phone': '01033334444', 'role': 'admin'})
        self.assertEqual(response.status_code, 200)
        items = self.client.get('/admin/users?admin_user_id=1&query=Updated').json['items']
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]['phone'], '010-3333-4444')
        self.assertEqual(items[0]['role'], 'user')
        self.assertNotIn('password', items[0])

    def test_facility_save(self):
        response = self.client.patch('/admin/facilities/1', json={'admin_user_id': 1, 'type': 'security_light', 'lat': 36.5, 'lng': 127.5})
        self.assertEqual(response.status_code, 200)
        item = self.client.get('/admin/facilities?admin_user_id=1&query=1').json['items'][0]
        self.assertEqual((item['type'], item['lat'], item['lng']), ('security_light', 36.5, 127.5))

    def test_validation_and_missing(self):
        self.assertEqual(self.client.patch('/admin/users/2', json={'admin_user_id': 1, 'name': '', 'phone': 'wrong'}).status_code, 400)
        self.assertEqual(self.client.patch('/admin/facilities/1', json={'admin_user_id': 1, 'type': 'street_light', 'lat': 91, 'lng': 127}).status_code, 400)
        self.assertEqual(self.client.patch('/admin/users/999', json={'admin_user_id': 1, 'name': 'Missing', 'phone': '01012345678'}).status_code, 404)
        self.assertEqual(self.client.get('/admin/users?admin_user_id=1&query=%25').json['items'], [])


if __name__ == '__main__':
    unittest.main()
