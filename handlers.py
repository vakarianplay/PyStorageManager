import json
import re
from urllib.parse import quote
from auth import SessionManager

class RequestHandler:
    def __init__(self, db, manager, user_manager, session_manager, pricing_manager=None):
        self.db = db
        self.manager = manager
        self.user_manager = user_manager
        self.session_manager = session_manager
        self.pricing_manager = pricing_manager

    # Auth handlers
    def login(self, username, password):
        password_hash = SessionManager.hash_password(password)
        user = self.db.authenticate_user(username, password_hash)

        if not user:
            raise ValueError('Неверное имя пользователя или пароль')

        session_id = self.session_manager.create_session(dict(user))
        return {
            'success': True,
            'session_id': session_id,
            'user': {
                'id': user['id'],
                'username': user['username'],
                'admin': user['admin']
            }
        }

    def logout(self, session_id):
        self.session_manager.delete_session(session_id)
        return {'success': True}

    def get_current_user(self, session_id):
        user = self.session_manager.get_session(session_id)
        if user:
            return {'authenticated': True, 'user': user}
        return {'authenticated': False}

    # User management (admin only)
    def get_users(self):
        users = self.db.get_all_users()
        return [dict(u) for u in users]

    def get_user(self, user_id):
        user = self.db.get_user_by_id(user_id)
        if user:
            return dict(user)
        raise ValueError('User not found')

    def create_user(self, fields):
        username = fields.get('username')
        password = fields.get('password')
        admin = fields.get('admin', '').lower() == 'true'

        if not username or not password:
            raise ValueError('Заполните все поля')

        password_hash = SessionManager.hash_password(password)
        result = self.user_manager.create_user(username, password_hash, admin)
        return {
            'success': True,
            'id': result['id'],
            'message': f'Пользователь "{username}" успешно создан'
        }

    def update_user(self, fields):
        user_id = int(fields.get('id'))
        username = fields.get('username')
        admin = fields.get('admin', '').lower() == 'true'

        self.user_manager.update_user(user_id, username, admin)

        password = fields.get('password', '').strip()
        if password:
            password_hash = SessionManager.hash_password(password)
            self.user_manager.update_user_password(user_id, password_hash)

        return {
            'success': True,
            'message': f'Пользователь "{username}" успешно обновлён'
        }

    def delete_user(self, user_id):
        self.user_manager.delete_user(user_id)
        return {
            'success': True,
            'message': 'Пользователь успешно удалён'
        }

    # GET handlers
    def get_objects(self):
        self.db.update_storage_stats()
        objects = self.db.get_all_objects()
        return [dict(obj) for obj in objects]

    def get_object(self, object_id):
        obj = self.db.get_object_by_id(object_id)
        if obj:
            return dict(obj)
        raise ValueError('Object not found')

    def get_object_details(self, object_id):
        details = self.db.get_object_details(object_id)
        return {
            'receipts': [dict(r) for r in details['receipts']],
            'writeoffs': [dict(w) for w in details['writeoffs']]
        }

    def get_sellers(self):
        sellers = self.db.get_all_sellers()
        return [dict(s) for s in sellers]

    def get_seller(self, seller_id):
        seller = self.db.get_seller_by_id(seller_id)
        if seller:
            return dict(seller)
        raise ValueError('Seller not found')

    def get_themes(self):
        themes = self.db.get_all_themes()
        return [dict(t) for t in themes]

    def get_theme(self, theme_id):
        theme = self.db.get_theme_by_id(theme_id)
        if theme:
            return dict(theme)
        raise ValueError('Theme not found')

    def get_receipt(self, receipt_id):
        receipt = self.db.get_receipt_by_id(receipt_id)
        if receipt:
            return dict(receipt)
        raise ValueError('Receipt not found')

    def get_writeoff(self, writeoff_id):
        writeoff = self.db.get_writeoff_by_id(writeoff_id)
        if writeoff:
            return dict(writeoff)
        raise ValueError('Writeoff not found')

    def get_file(self, file_type, file_id):
        result = self.db.get_file(file_type, file_id)
        if not result or not result.get('file'):
            raise ValueError('File not found')
        return result

    # Search handler
    def search_objects(self, search_type, search_value):
        self.db.update_storage_stats()

        if search_type == 'name':
            objects = self.db.search_objects_by_name(search_value)
        elif search_type == 'seller_name':
            objects = self.db.search_objects_by_seller_name(search_value)
        elif search_type == 'theme':
            objects = self.db.search_objects_by_theme(int(search_value))
        elif search_type == 'bill':
            objects = self.db.search_objects_by_bill(search_value)
        elif search_type == 'invoice':
            objects = self.db.search_objects_by_invoice(search_value)
        else:
            objects = self.db.get_all_objects()

        return [dict(obj) for obj in objects]

    # CREATE handlers
    def create_object(self, fields):
        object_name = fields.get('objectName')
        if not object_name:
            raise ValueError('Missing objectName')
        result = self.manager.create_object(object_name)
        return {
            'success': True,
            'id': result['id'],
            'message': f'Объект "{object_name}" успешно создан'
        }

    def create_seller(self, fields):
        name = fields.get('name')
        inn = fields.get('inn')
        kpp = fields.get('kpp')
        if not all([name, inn, kpp]):
            raise ValueError('Missing required fields')
        result = self.manager.create_seller(name, inn, kpp)
        return {
            'success': True,
            'id': result['id'],
            'message': f'Поставщик "{name}" успешно создан'
        }

    def create_theme(self, fields):
        name = fields.get('name')
        if not name:
            raise ValueError('Missing name')
        result = self.manager.create_theme(name)
        return {
            'success': True,
            'id': result['id'],
            'message': f'Тема "{name}" успешно создана'
        }

    def create_receipt(self, fields, files):
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
        if fields.get('newObjectName'):
            result = self.manager.create_object(fields['newObjectName'])
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

        return {
            'success': True,
            'id': receipt_result['id'],
            'message': f'Поступление успешно зарегистрировано!\n\n'
                       f'Количество: {fields.get("quantity", 0)} шт.\n'
                       f'Счёт: {fields.get("billNumber", "-")}\n'
                       f'Накладная: {fields.get("invoiceNumber", "-")}'
        }

    def create_writeoff(self, fields, files=None):
        if files is None:
            files = {}

        object_id = fields.get('objectId')
        theme_id = fields.get('themeId')

        if fields.get('newThemeName'):
            result = self.manager.create_theme(fields['newThemeName'])
            theme_id = result['id']

        quantity = int(fields.get('quantity', 0))
        writeoff_date = fields.get('writeoffDate') or None

        doc_info = files.get('writeoffDocument', {})
        file_data = doc_info.get('data')
        filename = doc_info.get('filename')

        # Если файл пустой (не выбран), обнуляем
        if file_data is not None and len(file_data) == 0:
            file_data = None
            filename = None

        result = self.manager.create_writeoff(
            object_id, theme_id, quantity,
            writeoff_date, file_data, filename
        )
        return {
            'success': True,
            'id': result['id'],
            'message': f'Списание успешно зарегистрировано!\n\n'
                    f'Количество: {quantity} шт.'
        }

    # UPDATE handlers
    def update_object(self, fields):
        object_id = int(fields.get('id'))
        object_name = fields.get('objectName')
        self.manager.update_object(object_id, object_name)
        return {
            'success': True,
            'message': f'Объект "{object_name}" успешно обновлён'
        }

    def update_seller(self, fields):
        seller_id = int(fields.get('id'))
        name = fields.get('name')
        inn = fields.get('inn')
        kpp = fields.get('kpp')
        self.manager.update_seller(seller_id, name, inn, kpp)
        return {
            'success': True,
            'message': f'Поставщик "{name}" успешно обновлён'
        }

    def update_theme(self, fields):
        theme_id = int(fields.get('id'))
        name = fields.get('name')
        self.manager.update_theme(theme_id, name)
        return {
            'success': True,
            'message': f'Тема "{name}" успешно обновлена'
        }

    def update_receipt(self, fields):
        receipt_id = int(fields.get('id'))
        object_id = int(fields.get('objectId'))
        seller_object_name = fields.get('sellerObjectName')
        seller_id = int(fields.get('sellerId'))
        theme_id = int(fields.get('themeId'))
        location = fields.get('location')
        quantity = int(fields.get('quantity'))

        self.manager.update_receipt(
            receipt_id, object_id, seller_object_name,
            seller_id, theme_id, location, quantity
        )
        return {
            'success': True,
            'message': 'Поступление успешно обновлено'
        }

    def update_writeoff(self, fields, files=None):
        if files is None:
            files = {}

        writeoff_id = int(fields.get('id'))
        object_id = int(fields.get('objectId'))
        theme_id = int(fields.get('themeId'))
        quantity = int(fields.get('quantity'))
        writeoff_date = fields.get('writeoffDate')

        doc_info = files.get('writeoffDocument', {})
        file_data = doc_info.get('data')
        filename = doc_info.get('filename')

        self.manager.update_writeoff(
            writeoff_id, object_id, theme_id, quantity,
            writeoff_date, file_data, filename
        )
        return {
            'success': True,
            'message': 'Списание успешно обновлено'
        }

    # DELETE handlers
    def delete_object(self, object_id):
        self.manager.delete_object(object_id)
        return {
            'success': True,
            'message': 'Объект успешно удалён'
        }

    def delete_seller(self, seller_id):
        self.manager.delete_seller(seller_id)
        return {
            'success': True,
            'message': 'Поставщик успешно удалён'
        }

    def delete_theme(self, theme_id):
        self.manager.delete_theme(theme_id)
        return {
            'success': True,
            'message': 'Тема успешно удалена'
        }

    def delete_receipt(self, receipt_id):
        self.manager.delete_receipt(receipt_id)
        return {
            'success': True,
            'message': 'Поступление успешно удалено'
        }

    def delete_writeoff(self, writeoff_id):
        self.manager.delete_writeoff(writeoff_id)
        return {
            'success': True,
            'message': 'Списание успешно удалено'
        }
        
    # Pricing handlers
    def get_all_pricing(self):
        pricing = self.db.get_all_pricing()
        return [dict(p) for p in pricing]

    def get_pricing(self, pricing_id):
        pricing = self.db.get_pricing_by_id(pricing_id)
        if pricing:
            return dict(pricing)
        raise ValueError('Pricing not found')

    def get_pricing_by_receipt(self, receipt_id):
        pricing = self.db.get_pricing_by_receipt(receipt_id)
        if pricing:
            return dict(pricing)
        return None

    def create_pricing(self, fields):
        receipt_id = int(fields.get('receiptId'))
        price = float(fields.get('price'))
        tax = float(fields.get('tax', 20.0))

        existing = self.db.get_pricing_by_receipt(receipt_id)
        if existing:
            raise ValueError('Цена для этого поступления уже существует')

        result = self.pricing_manager.create_pricing(receipt_id, price, tax)
        return {
            'success': True,
            'id': result['id'],
            'message': 'Цена успешно добавлена'
        }

    def update_pricing(self, fields):
        pricing_id = int(fields.get('id'))
        price = float(fields.get('price'))
        tax = float(fields.get('tax'))

        self.pricing_manager.update_pricing(pricing_id, price, tax)
        return {
            'success': True,
            'message': 'Цена успешно обновлена'
        }

    def delete_pricing(self, pricing_id):
        self.pricing_manager.delete_pricing(pricing_id)
        return {
            'success': True,
            'message': 'Цена успешно удалена'
        }

class MultipartParser:
    @staticmethod
    def parse(headers, rfile):
        """Старый метод — читает rfile сам."""
        content_length = int(headers.get('Content-Length', 0))
        if content_length == 0:
            return {}, {}
        body = rfile.read(content_length)
        return MultipartParser.parse_body(headers, body)

    @staticmethod
    def parse_body(headers, body):
        """Новый метод — принимает уже прочитанное тело."""
        if not body:
            return {}, {}

        content_type = headers.get('Content-Type', '')

        if 'multipart/form-data' not in content_type:
            try:
                return json.loads(body.decode('utf-8')), {}
            except (json.JSONDecodeError, UnicodeDecodeError):
                return {}, {}

        # Извлекаем boundary
        boundary = None
        for part in content_type.split(';'):
            part = part.strip()
            if part.startswith('boundary='):
                boundary = part.split('=', 1)[1].strip()
                break

        if not boundary:
            return {}, {}

        boundary = boundary.encode()

        fields = {}
        files = {}

        parts = body.split(b'--' + boundary)
        for part in parts:
            if not part or part.strip() in (
                b'', b'--', b'--\r\n'
            ):
                continue

            if b'\r\n\r\n' not in part:
                continue

            headers_raw, content = part.split(b'\r\n\r\n', 1)

            if content.endswith(b'\r\n'):
                content = content[:-2]

            headers_text = headers_raw.decode(
                'utf-8', errors='ignore'
            )

            name_match = re.search(
                r'name="([^"]+)"', headers_text
            )
            filename_match = re.search(
                r'filename="([^"]*)"', headers_text
            )

            if name_match:
                field_name = name_match.group(1)
                if filename_match:
                    fname = filename_match.group(1)
                    if fname and len(content) > 0:
                        files[field_name] = {
                            'filename': fname,
                            'data': content
                        }
                else:
                    fields[field_name] = content.decode('utf-8')

        return fields, files

class FileHelper:
    CONTENT_TYPES = {
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

    INLINE_TYPES = ['application/pdf', 'image/jpeg', 'image/png', 'image/gif']

    @classmethod
    def detect_content_type(cls, file_data, filename=''):
        ext = filename.lower().split('.')[-1] if '.' in filename else ''

        if ext in cls.CONTENT_TYPES:
            return cls.CONTENT_TYPES[ext]

        if file_data[:4] == b'%PDF':
            return 'application/pdf'
        if file_data[:3] == b'\xff\xd8\xff':
            return 'image/jpeg'
        if file_data[:8] == b'\x89PNG\r\n\x1a\n':
            return 'image/png'

        return 'application/octet-stream'

    @classmethod
    def is_inline(cls, content_type):
        return content_type in cls.INLINE_TYPES

    @staticmethod
    def encode_filename(filename):
        return quote(filename)