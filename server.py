import json
import yaml
import re
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs, quote

from database import Database, StorageManager

class StorageHandler(BaseHTTPRequestHandler):
    db = None
    manager = None

    def send_json_response(self, data, status=200):
        json_data = json.dumps(data, default=str, ensure_ascii=False)
        self.send_response(status)
        self.send_header('Content-Type', 'application/json; charset=utf-8')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(json_data.encode('utf-8'))

    def send_error_json(self, message, status=500):
        self.send_json_response({'error': message}, status)

    def do_GET(self):
        parsed_path = urlparse(self.path)
        path = parsed_path.path
        query = parse_qs(parsed_path.query)

        try:
            if path == '/' or path == '/index.html':
                self.serve_file('templates/index.html', 'text/html')
            elif path == '/add' or path == '/add.html':
                self.serve_file('templates/add.html', 'text/html')
            elif path == '/static/style.css':
                self.serve_file('static/style.css', 'text/css')
            elif path == '/api/objects':
                self.serve_objects()
            elif path == '/api/object_details':
                object_id = query.get('id', [None])[0]
                if object_id:
                    self.serve_object_details(int(object_id))
                else:
                    self.send_error_json('Missing object id', 400)
            elif path == '/api/sellers':
                self.serve_sellers()
            elif path == '/api/themes':
                self.serve_themes()
            elif path.startswith('/api/file/'):
                self.serve_document_file(path)
            else:
                self.send_error_json('Not Found', 404)
        except Exception as e:
            self.send_error_json(str(e), 500)

    def do_POST(self):
        parsed_path = urlparse(self.path)
        path = parsed_path.path

        try:
            if path == '/api/object':
                self.handle_create_object()
            elif path == '/api/seller':
                self.handle_create_seller()
            elif path == '/api/theme':
                self.handle_create_theme()
            elif path == '/api/receipt':
                self.handle_create_receipt()
            elif path == '/api/writeoff':
                self.handle_create_writeoff()
            else:
                self.send_error_json('Not Found', 404)
        except Exception as e:
            self.send_error_json(str(e), 500)

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

    def serve_objects(self):
        self.db.update_storage_stats()
        objects = self.db.get_all_objects()
        objects_list = [dict(obj) for obj in objects]
        self.send_json_response(objects_list)

    def serve_object_details(self, object_id):
        details = self.db.get_object_details(object_id)
        result = {
            'receipts': [dict(r) for r in details['receipts']],
            'writeoffs': [dict(w) for w in details['writeoffs']]
        }
        self.send_json_response(result)

    def serve_sellers(self):
        sellers = self.db.get_all_sellers()
        self.send_json_response([dict(s) for s in sellers])

    def serve_themes(self):
        themes = self.db.get_all_themes()
        self.send_json_response([dict(t) for t in themes])

    def serve_document_file(self, path):
        match = re.match(r'/api/file/(bill|invoice|entry_control)/(\d+)', path)
        if not match:
            self.send_error_json('Invalid file path', 400)
            return

        file_type = match.group(1)
        file_id = int(match.group(2))

        result = self.db.get_file(file_type, file_id)
        if not result or not result.get('file'):
            self.send_error_json('File not found', 404)
            return

        file_data = bytes(result['file'])
        filename = result.get('filename') or 'document'

        content_type = self.detect_content_type(file_data, filename)
        is_inline = content_type in ['application/pdf', 'image/jpeg', 'image/png', 'image/gif']

        encoded_filename = quote(filename)

        self.send_response(200)
        self.send_header('Content-Type', content_type)
        self.send_header('Content-Length', str(len(file_data)))

        if is_inline:
            self.send_header('Content-Disposition',
                             f"inline; filename*=UTF-8''{encoded_filename}")
        else:
            self.send_header('Content-Disposition',
                             f"attachment; filename*=UTF-8''{encoded_filename}")

        self.end_headers()
        self.wfile.write(file_data)

    def detect_content_type(self, file_data, filename=''):
        ext = filename.lower().split('.')[-1] if '.' in filename else ''

        ext_map = {
            'pdf': 'application/pdf',
            'jpg': 'image/jpeg',
            'jpeg': 'image/jpeg',
            'png': 'image/png',
            'gif': 'image/gif',
            'doc': 'application/msword',
            'docx': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
            'xls': 'application/vnd.ms-excel',
            'xlsx': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        }

        if ext in ext_map:
            return ext_map[ext]

        if file_data[:4] == b'%PDF':
            return 'application/pdf'
        if file_data[:3] == b'\xff\xd8\xff':
            return 'image/jpeg'
        if file_data[:8] == b'\x89PNG\r\n\x1a\n':
            return 'image/png'

        return 'application/octet-stream'

    def parse_multipart(self):
        content_type = self.headers.get('Content-Type', '')
        if 'multipart/form-data' not in content_type:
            content_length = int(self.headers.get('Content-Length', 0))
            body = self.rfile.read(content_length)
            return json.loads(body.decode('utf-8')), {}

        boundary = content_type.split('boundary=')[1].encode()
        content_length = int(self.headers.get('Content-Length', 0))
        body = self.rfile.read(content_length)

        fields = {}
        files = {}

        parts = body.split(b'--' + boundary)
        for part in parts:
            if not part or part == b'--\r\n' or part == b'--':
                continue

            if b'\r\n\r\n' not in part:
                continue

            headers_raw, content = part.split(b'\r\n\r\n', 1)
            content = content.rstrip(b'\r\n')

            headers_text = headers_raw.decode('utf-8', errors='ignore')

            name_match = re.search(r'name="([^"]+)"', headers_text)
            filename_match = re.search(r'filename="([^"]+)"', headers_text)

            if name_match:
                field_name = name_match.group(1)
                if filename_match:
                    files[field_name] = {
                        'filename': filename_match.group(1),
                        'data': content
                    }
                else:
                    fields[field_name] = content.decode('utf-8')

        return fields, files

    def handle_create_object(self):
        fields, _ = self.parse_multipart()
        object_name = fields.get('objectName')
        if not object_name:
            self.send_error_json('Missing objectName', 400)
            return
        result = self.manager.create_object(object_name)
        self.send_json_response({
            'success': True,
            'id': result['id'],
            'message': f'Объект "{object_name}" успешно создан'
        })

    def handle_create_seller(self):
        fields, _ = self.parse_multipart()
        name = fields.get('name')
        inn = fields.get('inn')
        kpp = fields.get('kpp')
        if not all([name, inn, kpp]):
            self.send_error_json('Missing required fields', 400)
            return
        result = self.manager.create_seller(name, inn, kpp)
        self.send_json_response({
            'success': True,
            'id': result['id'],
            'message': f'Поставщик "{name}" успешно создан'
        })

    def handle_create_theme(self):
        fields, _ = self.parse_multipart()
        name = fields.get('name')
        if not name:
            self.send_error_json('Missing name', 400)
            return
        result = self.manager.create_theme(name)
        self.send_json_response({
            'success': True,
            'id': result['id'],
            'message': f'Тема "{name}" успешно создана'
        })

    def handle_create_receipt(self):
        fields, files = self.parse_multipart()

        seller_id = fields.get('sellerId')
        if fields.get('newSellerName'):
            result = self.manager.create_seller(
                fields['newSellerName'],
                fields.get('newSellerInn', ''),
                fields.get('newSellerKpp', '')
            )
            seller_id = result['id']

        theme_id = fields.get('themeId')
        if fields.get('newThemeName'):
            result = self.manager.create_theme(fields['newThemeName'])
            theme_id = result['id']

        object_id = fields.get('objectId')
        object_name = ''
        if fields.get('newObjectName'):
            object_name = fields['newObjectName']
            result = self.manager.create_object(object_name)
            object_id = result['id']

        bill_file_info = files.get('billFile', {})
        bill_result = self.manager.create_bill(
            fields.get('billNumber', ''),
            fields.get('billDate'),
            seller_id,
            bill_file_info.get('data'),
            bill_file_info.get('filename')
        )
        bill_id = bill_result['id']

        invoice_file_info = files.get('invoiceFile', {})
        invoice_result = self.manager.create_invoice(
            fields.get('invoiceNumber', ''),
            fields.get('invoiceDate'),
            seller_id,
            bill_id,
            invoice_file_info.get('data'),
            invoice_file_info.get('filename')
        )
        invoice_id = invoice_result['id']

        entry_control_file_info = files.get('entryControlFile', {})
        entry_control_result = self.manager.create_entry_control(
            fields.get('entryControlNumber', ''),
            fields.get('entryControlDate'),
            entry_control_file_info.get('data'),
            entry_control_file_info.get('filename')
        )
        entry_control_id = entry_control_result['id']

        receipt_result = self.manager.create_receipt(
            object_id,
            fields.get('sellerObjectName', ''),
            seller_id,
            bill_id,
            theme_id,
            invoice_id,
            entry_control_id,
            fields.get('location', ''),
            int(fields.get('quantity', 0))
        )

        self.send_json_response({
            'success': True,
            'id': receipt_result['id'],
            'message': f'Поступление успешно зарегистрировано!\n\n'
                    f'Количество: {fields.get("quantity", 0)} шт.\n'
                    f'Счёт: {fields.get("billNumber", "-")}\n'
                    f'Накладная: {fields.get("invoiceNumber", "-")}'
        })

    def handle_create_writeoff(self):
        fields, _ = self.parse_multipart()

        object_id = fields.get('objectId')
        theme_id = fields.get('themeId')

        if fields.get('newThemeName'):
            result = self.manager.create_theme(fields['newThemeName'])
            theme_id = result['id']

        quantity = int(fields.get('quantity', 0))

        result = self.manager.create_writeoff(object_id, theme_id, quantity)
        self.send_json_response({
            'success': True,
            'id': result['id'],
            'message': f'Списание успешно зарегистрировано!\n\nКоличество: {quantity} шт.'
        })

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

    StorageHandler.db = db
    StorageHandler.manager = StorageManager(db)

    server_address = (server_config['host'], server_config['port'])
    httpd = HTTPServer(server_address, StorageHandler)

    print(f"Server running at http://{server_config['host']}:{server_config['port']}")

    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nServer stopped")
        httpd.shutdown()
        db.close()

if __name__ == '__main__':
    run_server()