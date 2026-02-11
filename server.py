import json
import re
import os
import yaml
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
from http.cookies import SimpleCookie

from database import Database
from manager import StorageManager, UserManager, PricingManager
from handlers import RequestHandler, MultipartParser, FileHelper
from auth import SessionManager

class StorageHTTPHandler(BaseHTTPRequestHandler):
    handler = None
    session_manager = None
    config = None
    protocol_version = "HTTP/1.0"

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
        json_data = json.dumps(
            data, default=str, ensure_ascii=False
        )
        encoded = json_data.encode('utf-8')

        self.send_response(status)
        self.send_header('Content-Type',
                        'application/json; charset=utf-8')
        self.send_header('Content-Length', str(len(encoded)))
        self.send_header('Connection', 'close')

        if session_id:
            self.send_header(
                'Set-Cookie',
                f'session_id={session_id}; Path=/; '
                f'HttpOnly; SameSite=Strict'
            )

        self.end_headers()
        self.wfile.write(encoded)
        self.wfile.flush()

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

    def serve_binary_file(self, filepath, content_type):
        try:
            with open(filepath, 'rb') as f:
                content = f.read()
            self.send_response(200)
            self.send_header('Content-Type', content_type)
            self.send_header('Content-Length', str(len(content)))
            self.send_header('Cache-Control', 'public, max-age=86400')
            self.end_headers()
            self.wfile.write(content)
        except FileNotFoundError:
            self.send_error_json('File not found', 404)

    def serve_document_file(self, path):
        match = re.match(
            r'/api/file/(bill|invoice|entry_control|writeoff)/(\d+)',
            path
        )
        if not match:
            self.send_error_json('Invalid file path', 400)
            return

        file_type = match.group(1)
        file_id = int(match.group(2))

        try:
            result = self.handler.get_file(file_type, file_id)
            file_data = bytes(result['file'])
            filename = result.get('filename') or 'document'

            content_type = FileHelper.detect_content_type(
                file_data, filename
            )
            encoded_filename = FileHelper.encode_filename(filename)

            self.send_response(200)
            self.send_header('Content-Type', content_type)
            self.send_header('Content-Length', str(len(file_data)))

            if FileHelper.is_inline(content_type):
                self.send_header(
                    'Content-Disposition',
                    f"inline; filename*=UTF-8''{encoded_filename}"
                )
            else:
                self.send_header(
                    'Content-Disposition',
                    f"attachment; filename*=UTF-8''{encoded_filename}"
                )

            self.end_headers()
            self.wfile.write(file_data)
        except ValueError as e:
            self.send_error_json(str(e), 404)

    def get_logo_content_type(self, filepath):
        ext = filepath.lower().split('.')[-1] if '.' in filepath else ''
        content_types = {
            'png': 'image/png',
            'jpg': 'image/jpeg',
            'jpeg': 'image/jpeg',
            'gif': 'image/gif',
            'svg': 'image/svg+xml',
            'ico': 'image/x-icon',
            'webp': 'image/webp'
        }
        return content_types.get(ext, 'image/png')

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
            '/logs': ('templates/logs.html', 'text/html'),
            '/logs.html': ('templates/logs.html', 'text/html'),
        }

        try:
            if path in routes:
                filepath, content_type = routes[path]
                self.serve_file(filepath, content_type)
            elif path == '/favicon.ico' or path == '/static/logo':
                company_config = self.config.get('company', {})
                logo_path = company_config.get('logo', '')
                if logo_path and os.path.exists(logo_path):
                    content_type = self.get_logo_content_type(logo_path)
                    self.serve_binary_file(logo_path, content_type)
                else:
                    self.send_response(404)
                    self.end_headers()
            elif path == '/api/config':
                company_config = self.config.get('company', {})
                self.send_json_response({
                    'company_name': company_config.get('name', ''),
                    'has_logo': bool(company_config.get('logo', '') and os.path.exists(company_config.get('logo', '')))
                })
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
            elif path == '/api/pricing':
                pricing_id = query.get('id', [None])[0]
                receipt_id = query.get('receipt_id', [None])[0]
                if pricing_id:
                    self.send_json_response(self.handler.get_pricing(int(pricing_id)))
                elif receipt_id:
                    result = self.handler.get_pricing_by_receipt(int(receipt_id))
                    self.send_json_response(result if result else {})
                else:
                    self.send_json_response(self.handler.get_all_pricing())
                    
            elif path == '/api/logs':
                if not self.require_admin():
                    return
                search = query.get('search', [''])[0]
                limit = int(query.get('limit', [500])[0])
                offset = int(query.get('offset', [0])[0])
                if search:
                    logs = self.handler.search_logs(
                        search, limit, offset
                    )
                else:
                    logs = self.handler.get_logs(limit, offset)
                count = self.handler.get_logs_count()
                self.send_json_response({
                    'logs': logs, 'total': count
                })
        except ValueError as e:
            self.send_error_json(str(e), 404)
        except Exception as e:
                self.send_error_json(str(e), 500)

    def do_POST(self):
        path = urlparse(self.path).path
        content_length = int(self.headers.get('Content-Length', 0))
        body = self.rfile.read(content_length) if content_length > 0 else b''

        fields, files = MultipartParser.parse_body(
            self.headers, body
        )

        try:
            if path == '/api/auth/login':
                username = fields.get('username', '')
                password = fields.get('password', '')
                result = self.handler.login(username, password)
                self.send_json_response(
                    result, session_id=result['session_id']
                )
            elif path == '/api/auth/logout':
                session_id = self.get_session_id()
                self.handler.logout(session_id)
                self.send_json_response({'success': True})
            elif path == '/api/object':
                if not self.require_auth():
                    return
                self.send_json_response(
                    self.handler.create_object(fields)
                )
            elif path == '/api/seller':
                if not self.require_auth():
                    return
                self.send_json_response(
                    self.handler.create_seller(fields)
                )
            elif path == '/api/theme':
                if not self.require_auth():
                    return
                self.send_json_response(
                    self.handler.create_theme(fields)
                )
            elif path == '/api/receipt':
                if not self.require_auth():
                    return
                self.send_json_response(
                    self.handler.create_receipt(fields, files)
                )
            elif path == '/api/writeoff':
                if not self.require_auth():
                    return
                self.send_json_response(
                    self.handler.create_writeoff(fields, files)
                )
            elif path == '/api/pricing':
                if not self.require_auth():
                    return
                self.send_json_response(
                    self.handler.create_pricing(fields)
                )
            elif path == '/api/user':
                if not self.require_admin():
                    return
                self.send_json_response(
                    self.handler.create_user(fields)
                )
            else:
                self.send_error_json('Not Found', 404)
        except ValueError as e:
            self.send_error_json(str(e), 400)
        except Exception as e:
            self.send_error_json(str(e), 500)

    def do_PUT(self):
        path = urlparse(self.path).path

        content_length = int(
            self.headers.get('Content-Length', 0)
        )
        body = (
            self.rfile.read(content_length)
            if content_length > 0 else b''
        )
        fields, files = MultipartParser.parse_body(
            self.headers, body
        )

        session_id = self.get_session_id()

        try:
            if path == '/api/object':
                if not self.require_auth():
                    return
                self.send_json_response(
                    self.handler.update_object(
                        fields, session_id
                    )
                )
            elif path == '/api/seller':
                if not self.require_auth():
                    return
                self.send_json_response(
                    self.handler.update_seller(
                        fields, session_id
                    )
                )
            elif path == '/api/theme':
                if not self.require_auth():
                    return
                self.send_json_response(
                    self.handler.update_theme(
                        fields, session_id
                    )
                )
            elif path == '/api/receipt':
                if not self.require_auth():
                    return
                self.send_json_response(
                    self.handler.update_receipt(
                        fields, session_id
                    )
                )
            elif path == '/api/writeoff':
                if not self.require_auth():
                    return
                self.send_json_response(
                    self.handler.update_writeoff(
                        fields, files, session_id
                    )
                )
            elif path == '/api/pricing':
                if not self.require_auth():
                    return
                self.send_json_response(
                    self.handler.update_pricing(
                        fields, session_id
                    )
                )
            elif path == '/api/user':
                if not self.require_admin():
                    return
                self.send_json_response(
                    self.handler.update_user(
                        fields, session_id
                    )
                )
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

        content_length = int(
            self.headers.get('Content-Length', 0)
        )
        if content_length > 0:
            self.rfile.read(content_length)

        session_id = self.get_session_id()

        try:
            if path == '/api/object':
                if not self.require_auth():
                    return
                oid = query.get('id', [None])[0]
                if oid:
                    self.send_json_response(
                        self.handler.delete_object(
                            int(oid), session_id
                        )
                    )
                else:
                    self.send_error_json(
                        'Missing object id', 400
                    )
            elif path == '/api/seller':
                if not self.require_auth():
                    return
                sid = query.get('id', [None])[0]
                if sid:
                    self.send_json_response(
                        self.handler.delete_seller(
                            int(sid), session_id
                        )
                    )
                else:
                    self.send_error_json(
                        'Missing seller id', 400
                    )
            elif path == '/api/theme':
                if not self.require_auth():
                    return
                tid = query.get('id', [None])[0]
                if tid:
                    self.send_json_response(
                        self.handler.delete_theme(
                            int(tid), session_id
                        )
                    )
                else:
                    self.send_error_json(
                        'Missing theme id', 400
                    )
            elif path == '/api/receipt':
                if not self.require_auth():
                    return
                rid = query.get('id', [None])[0]
                if rid:
                    self.send_json_response(
                        self.handler.delete_receipt(
                            int(rid), session_id
                        )
                    )
                else:
                    self.send_error_json(
                        'Missing receipt id', 400
                    )
            elif path == '/api/writeoff':
                if not self.require_auth():
                    return
                wid = query.get('id', [None])[0]
                if wid:
                    self.send_json_response(
                        self.handler.delete_writeoff(
                            int(wid), session_id
                        )
                    )
                else:
                    self.send_error_json(
                        'Missing writeoff id', 400
                    )
            elif path == '/api/pricing':
                if not self.require_auth():
                    return
                pid = query.get('id', [None])[0]
                if pid:
                    self.send_json_response(
                        self.handler.delete_pricing(
                            int(pid), session_id
                        )
                    )
                else:
                    self.send_error_json(
                        'Missing pricing id', 400
                    )
            elif path == '/api/user':
                if not self.require_admin():
                    return
                uid = query.get('id', [None])[0]
                if uid:
                    self.send_json_response(
                        self.handler.delete_user(
                            int(uid), session_id
                        )
                    )
                else:
                    self.send_error_json(
                        'Missing user id', 400
                    )
            else:
                self.send_error_json('Not Found', 404)
        except Exception as e:
            error_msg = str(e)
            if 'CONTEXT' in error_msg:
                error_msg = error_msg.split('\n')[0]
            self.send_error_json(error_msg, 400)

    def log_message(self, format, *args):
        pass
        # print(f"[{self.log_date_time_string()}] {args[0]}")

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
    pricing_manager = PricingManager(db)

    StorageHTTPHandler.handler = RequestHandler(db, manager, user_manager, session_manager, pricing_manager)
    StorageHTTPHandler.session_manager = session_manager
    StorageHTTPHandler.config = config

    server_address = (server_config['host'], server_config['port'])
    httpd = HTTPServer(server_address, StorageHTTPHandler)

    company_name = config.get('company', {}).get('name', '')
    if company_name:
        print(f"Company: {company_name}")
        print(f"Database: {config.get('database', {}).get('host', '')}  {config.get('database', {}).get('name', '')}")

    print(f"Server running at http://{server_config['host']}:{server_config['port']}")

    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nServer stopped")
        httpd.shutdown()
        db.close()

if __name__ == '__main__':
    run_server()