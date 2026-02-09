import json
import re
import yaml
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
from http.cookies import SimpleCookie

from database import Database
from manager import StorageManager, UserManager
from handlers import RequestHandler, MultipartParser, FileHelper
from auth import SessionManager

class StorageHTTPHandler(BaseHTTPRequestHandler):
    handler = None
    session_manager = None

    def get_session_id(self):
        cookie_header = self.headers.get('Cookie', '')
        cookie = SimpleCookie()
        cookie.load(cookie_header)
        if 'session_id' in cookie:
            return cookie['session_id'].value
        return None

    def get_current_user(self):
        session_id = self.get_session_id()
        if session_id:
            return self.session_manager.get_session(session_id)
        return None

    def require_auth(self):
        user = self.get_current_user()
        if not user:
            self.send_error_json('Требуется авторизация', 401)
            return None
        return user

    def require_admin(self):
        user = self.require_auth()
        if not user:
            return None
        if not user.get('admin'):
            self.send_error_json('Требуются права администратора', 403)
            return None
        return user

    def send_json_response(self, data, status=200, session_id=None):
        json_data = json.dumps(data, default=str, ensure_ascii=False)
        self.send_response(status)
        self.send_header('Content-Type', 'application/json; charset=utf-8')
        self.send_header('Access-Control-Allow-Origin', '*')

        if session_id:
            self.send_header('Set-Cookie', f'session_id={session_id}; Path=/; HttpOnly; SameSite=Strict')

        self.end_headers()
        self.wfile.write(json_data.encode('utf-8'))

    def send_error_json(self, message, status=500):
        self.send_json_response({'error': message}, status)

    def serve_file(self, filepath, content_type):
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                content = f.read()
            self.send_response(200)
            self.send_header('Content-Type', f'{content_type}; charset=utf-8')
            self.end_headers()
            self.wfile.write(content.encode('utf-8'))
        except FileNotFoundError:
            self.send_error_json('File not found', 404)

    def serve_document_file(self, path):
        match = re.match(r'/api/file/(bill|invoice|entry_control)/(\d+)', path)
        if not match:
            self.send_error_json('Invalid file path', 400)
            return

        file_type = match.group(1)
        file_id = int(match.group(2))

        try:
            result = self.handler.get_file(file_type, file_id)
            file_data = bytes(result['file'])
            filename = result.get('filename') or 'document'

            content_type = FileHelper.detect_content_type(file_data, filename)
            encoded_filename = FileHelper.encode_filename(filename)

            self.send_response(200)
            self.send_header('Content-Type', content_type)
            self.send_header('Content-Length', str(len(file_data)))

            if FileHelper.is_inline(content_type):
                self.send_header('Content-Disposition', f"inline; filename*=UTF-8''{encoded_filename}")
            else:
                self.send_header('Content-Disposition', f"attachment; filename*=UTF-8''{encoded_filename}")

            self.end_headers()
            self.wfile.write(file_data)
        except ValueError as e:
            self.send_error_json(str(e), 404)

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path
        query = parse_qs(parsed.query)

        routes = {
            '/': ('templates/index.html', 'text/html'),
            '/index.html': ('templates/index.html', 'text/html'),
            '/add': ('templates/add.html', 'text/html'),
            '/add.html': ('templates/add.html', 'text/html'),
            '/manage': ('templates/manage.html', 'text/html'),
            '/manage.html': ('templates/manage.html', 'text/html'),
            '/users': ('templates/users.html', 'text/html'),
            '/users.html': ('templates/users.html', 'text/html'),
            '/static/style.css': ('static/style.css', 'text/css'),
        }

        try:
            if path in routes:
                filepath, content_type = routes[path]
                self.serve_file(filepath, content_type)
            elif path == '/api/auth/check':
                session_id = self.get_session_id()
                self.send_json_response(self.handler.get_current_user(session_id))
            elif path == '/api/objects':
                self.send_json_response(self.handler.get_objects())
            elif path == '/api/search':
                search_type = query.get('type', ['name'])[0]
                search_value = query.get('value', [''])[0]
                self.send_json_response(self.handler.search_objects(search_type, search_value))
            elif path == '/api/object':
                object_id = query.get('id', [None])[0]
                if object_id:
                    self.send_json_response(self.handler.get_object(int(object_id)))
                else:
                    self.send_error_json('Missing object id', 400)
            elif path == '/api/object_details':
                object_id = query.get('id', [None])[0]
                if object_id:
                    self.send_json_response(self.handler.get_object_details(int(object_id)))
                else:
                    self.send_error_json('Missing object id', 400)
            elif path == '/api/sellers':
                self.send_json_response(self.handler.get_sellers())
            elif path == '/api/seller':
                seller_id = query.get('id', [None])[0]
                if seller_id:
                    self.send_json_response(self.handler.get_seller(int(seller_id)))
                else:
                    self.send_error_json('Missing seller id', 400)
            elif path == '/api/themes':
                self.send_json_response(self.handler.get_themes())
            elif path == '/api/theme':
                theme_id = query.get('id', [None])[0]
                if theme_id:
                    self.send_json_response(self.handler.get_theme(int(theme_id)))
                else:
                    self.send_error_json('Missing theme id', 400)
            elif path == '/api/receipt':
                receipt_id = query.get('id', [None])[0]
                if receipt_id:
                    self.send_json_response(self.handler.get_receipt(int(receipt_id)))
                else:
                    self.send_error_json('Missing receipt id', 400)
            elif path == '/api/writeoff':
                writeoff_id = query.get('id', [None])[0]
                if writeoff_id:
                    self.send_json_response(self.handler.get_writeoff(int(writeoff_id)))
                else:
                    self.send_error_json('Missing writeoff id', 400)
            elif path == '/api/users':
                if not self.require_admin():
                    return
                self.send_json_response(self.handler.get_users())
            elif path == '/api/user':
                if not self.require_admin():
                    return
                user_id = query.get('id', [None])[0]
                if user_id:
                    self.send_json_response(self.handler.get_user(int(user_id)))
                else:
                    self.send_error_json('Missing user id', 400)
            elif path.startswith('/api/file/'):
                self.serve_document_file(path)
            else:
                self.send_error_json('Not Found', 404)
        except ValueError as e:
            self.send_error_json(str(e), 404)
        except Exception as e:
            self.send_error_json(str(e), 500)

    def do_POST(self):
        path = urlparse(self.path).path
        fields, files = MultipartParser.parse(self.headers, self.rfile)

        try:
            if path == '/api/auth/login':
                username = fields.get('username', '')
                password = fields.get('password', '')
                result = self.handler.login(username, password)
                self.send_json_response(result, session_id=result['session_id'])
            elif path == '/api/auth/logout':
                session_id = self.get_session_id()
                self.handler.logout(session_id)
                self.send_json_response({'success': True})
            elif path == '/api/object':
                if not self.require_auth():
                    return
                self.send_json_response(self.handler.create_object(fields))
            elif path == '/api/seller':
                if not self.require_auth():
                    return
                self.send_json_response(self.handler.create_seller(fields))
            elif path == '/api/theme':
                if not self.require_auth():
                    return
                self.send_json_response(self.handler.create_theme(fields))
            elif path == '/api/receipt':
                if not self.require_auth():
                    return
                self.send_json_response(self.handler.create_receipt(fields, files))
            elif path == '/api/writeoff':
                if not self.require_auth():
                    return
                self.send_json_response(self.handler.create_writeoff(fields))
            elif path == '/api/user':
                if not self.require_admin():
                    return
                self.send_json_response(self.handler.create_user(fields))
            else:
                self.send_error_json('Not Found', 404)
        except ValueError as e:
            self.send_error_json(str(e), 400)
        except Exception as e:
            self.send_error_json(str(e), 500)

    def do_PUT(self):
        path = urlparse(self.path).path
        fields, _ = MultipartParser.parse(self.headers, self.rfile)

        try:
            if path == '/api/object':
                if not self.require_auth():
                    return
                self.send_json_response(self.handler.update_object(fields))
            elif path == '/api/seller':
                if not self.require_auth():
                    return
                self.send_json_response(self.handler.update_seller(fields))
            elif path == '/api/theme':
                if not self.require_auth():
                    return
                self.send_json_response(self.handler.update_theme(fields))
            elif path == '/api/receipt':
                if not self.require_auth():
                    return
                self.send_json_response(self.handler.update_receipt(fields))
            elif path == '/api/writeoff':
                if not self.require_auth():
                    return
                self.send_json_response(self.handler.update_writeoff(fields))
            elif path == '/api/user':
                if not self.require_admin():
                    return
                self.send_json_response(self.handler.update_user(fields))
            else:
                self.send_error_json('Not Found', 404)
        except ValueError as e:
            self.send_error_json(str(e), 400)
        except Exception as e:
            self.send_error_json(str(e), 500)

    def do_DELETE(self):
        parsed = urlparse(self.path)
        path = parsed.path
        query = parse_qs(parsed.query)

        try:
            if path == '/api/object':
                if not self.require_auth():
                    return
                object_id = query.get('id', [None])[0]
                if object_id:
                    self.send_json_response(self.handler.delete_object(int(object_id)))
                else:
                    self.send_error_json('Missing object id', 400)
            elif path == '/api/seller':
                if not self.require_auth():
                    return
                seller_id = query.get('id', [None])[0]
                if seller_id:
                    self.send_json_response(self.handler.delete_seller(int(seller_id)))
                else:
                    self.send_error_json('Missing seller id', 400)
            elif path == '/api/theme':
                if not self.require_auth():
                    return
                theme_id = query.get('id', [None])[0]
                if theme_id:
                    self.send_json_response(self.handler.delete_theme(int(theme_id)))
                else:
                    self.send_error_json('Missing theme id', 400)
            elif path == '/api/receipt':
                if not self.require_auth():
                    return
                receipt_id = query.get('id', [None])[0]
                if receipt_id:
                    self.send_json_response(self.handler.delete_receipt(int(receipt_id)))
                else:
                    self.send_error_json('Missing receipt id', 400)
            elif path == '/api/writeoff':
                if not self.require_auth():
                    return
                writeoff_id = query.get('id', [None])[0]
                if writeoff_id:
                    self.send_json_response(self.handler.delete_writeoff(int(writeoff_id)))
                else:
                    self.send_error_json('Missing writeoff id', 400)
            elif path == '/api/user':
                if not self.require_admin():
                    return
                user_id = query.get('id', [None])[0]
                if user_id:
                    self.send_json_response(self.handler.delete_user(int(user_id)))
                else:
                    self.send_error_json('Missing user id', 400)
            else:
                self.send_error_json('Not Found', 404)
        except ValueError as e:
            self.send_error_json(str(e), 400)
        except Exception as e:
            self.send_error_json(str(e), 500)

    def log_message(self, format, *args):
        pass

def load_config(config_path='config.yaml'):
    with open(config_path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)

def run_server(config_path='config.yaml'):
    config = load_config(config_path)
    server_config = config['server']

    db = Database(config_path)
    db.test_connection()

    manager = StorageManager(db)
    user_manager = UserManager(db)
    session_manager = SessionManager()

    StorageHTTPHandler.handler = RequestHandler(db, manager, user_manager, session_manager)
    StorageHTTPHandler.session_manager = session_manager

    server_address = (server_config['host'], server_config['port'])
    httpd = HTTPServer(server_address, StorageHTTPHandler)

    print(f"Server running at http://{server_config['host']}:{server_config['port']}")

    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nServer stopped")
        httpd.shutdown()
        db.close()

if __name__ == '__main__':
    run_server()