import json
import re
from urllib.parse import quote
from auth import SessionManager

class RequestHandler:
    def __init__(self, db, manager, user_manager,
                 session_manager, pricing_manager=None):
        self.db = db
        self.manager = manager
        self.user_manager = user_manager
        self.session_manager = session_manager
        self.pricing_manager = pricing_manager

    # ==================== LOGGING ====================
    def _log(self, session_id, action, entity_type,
             entity_id, entity_name, details=None):
        """Записывает действие в лог."""
        user = self.session_manager.get_session(session_id)
        if user:
            self.db.add_log(
                user['id'], user['username'], action,
                entity_type, entity_id, entity_name, details
            )

    def get_logs(self, limit=500, offset=0):
        logs = self.db.get_all_logs(limit, offset)
        return [dict(l) for l in logs]

    def get_logs_count(self):
        return self.db.get_logs_count()

    def search_logs(self, search_text, limit=500, offset=0):
        logs = self.db.search_logs(search_text, limit, offset)
        return [dict(l) for l in logs]

    # ==================== AUTH ====================
    def login(self, username, password):
        password_hash = SessionManager.hash_password(password)
        user = self.db.authenticate_user(username, password_hash)

        if not user:
            raise ValueError(
                'Неверное имя пользователя или пароль'
            )

        session_id = self.session_manager.create_session(
            dict(user)
        )
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

    # ==================== USER MANAGEMENT ====================
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
        result = self.user_manager.create_user(
            username, password_hash, admin
        )
        return {
            'success': True,
            'id': result['id'],
            'message': f'Пользователь "{username}" успешно создан'
        }

    def update_user(self, fields, session_id=None):
        user_id = int(fields.get('id'))
        username = fields.get('username')
        admin = fields.get('admin', '').lower() == 'true'

        self.user_manager.update_user(user_id, username, admin)

        password = fields.get('password', '').strip()
        if password:
            password_hash = SessionManager.hash_password(password)
            self.user_manager.update_user_password(
                user_id, password_hash
            )

        if session_id:
            self._log(
                session_id, 'Редактирование', 'Пользователь',
                user_id, username, None
            )

        return {
            'success': True,
            'message': f'Пользователь "{username}" успешно обновлён'
        }

    def delete_user(self, user_id, session_id=None):
        user = self.db.get_user_by_id(user_id)
        user_name = user['username'] if user else str(user_id)

        self.user_manager.delete_user(user_id)

        if session_id:
            self._log(
                session_id, 'Удаление', 'Пользователь',
                user_id, user_name, None
            )

        return {
            'success': True,
            'message': 'Пользователь успешно удалён'
        }

    # ==================== GET HANDLERS ====================
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

    # ==================== SEARCH ====================
    def search_objects(self, search_type, search_value):
        self.db.update_storage_stats()

        if search_type == 'name':
            objects = self.db.search_objects_by_name(search_value)
        elif search_type == 'seller_name':
            objects = self.db.search_objects_by_seller_name(
                search_value
            )
        elif search_type == 'theme':
            objects = self.db.search_objects_by_theme(
                int(search_value)
            )
        elif search_type == 'bill':
            objects = self.db.search_objects_by_bill(search_value)
        elif search_type == 'invoice':
            objects = self.db.search_objects_by_invoice(
                search_value
            )
        else:
            objects = self.db.get_all_objects()

        return [dict(obj) for obj in objects]

    # ==================== CREATE ====================
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
            result = self.manager.create_theme(
                fields['newThemeName']
            )
            theme_id = result['id']

        object_id = fields.get('objectId')
        if fields.get('newObjectName'):
            result = self.manager.create_object(
                fields['newObjectName']
            )
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
            seller_id, bill_id,
            invoice_file_info.get('data'),
            invoice_file_info.get('filename')
        )
        invoice_id = invoice_result['id']

        ec_file_info = files.get('entryControlFile', {})
        ec_result = self.manager.create_entry_control(
            fields.get('entryControlNumber', ''),
            fields.get('entryControlDate'),
            ec_file_info.get('data'),
            ec_file_info.get('filename')
        )
        entry_control_id = ec_result['id']

        receipt_result = self.manager.create_receipt(
            object_id,
            fields.get('sellerObjectName', ''),
            seller_id, bill_id, theme_id,
            invoice_id, entry_control_id,
            fields.get('location', ''),
            int(fields.get('quantity', 0))
        )

        return {
            'success': True,
            'id': receipt_result['id'],
            'message': (
                f'Поступление успешно зарегистрировано!\n\n'
                f'Количество: {fields.get("quantity", 0)} шт.\n'
                f'Счёт: {fields.get("billNumber", "-")}\n'
                f'Накладная: {fields.get("invoiceNumber", "-")}'
            )
        }

    def create_writeoff(self, fields, files=None):
        if files is None:
            files = {}

        object_id = fields.get('objectId')
        theme_id = fields.get('themeId')

        if fields.get('newThemeName'):
            result = self.manager.create_theme(
                fields['newThemeName']
            )
            theme_id = result['id']

        quantity = int(fields.get('quantity', 0))
        writeoff_date = fields.get('writeoffDate') or None

        doc_info = files.get('writeoffDocument', {})
        file_data = doc_info.get('data')
        filename = doc_info.get('filename')

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
            'message': (
                f'Списание успешно зарегистрировано!\n\n'
                f'Количество: {quantity} шт.'
            )
        }

    # ==================== UPDATE (с логированием) ===========
    def update_object(self, fields, session_id=None):
        object_id = int(fields.get('id'))
        object_name = fields.get('objectName')
        self.manager.update_object(object_id, object_name)

        if session_id:
            self._log(
                session_id, 'Редактирование', 'Объект',
                object_id, object_name, None
            )

        return {
            'success': True,
            'message': f'Объект "{object_name}" успешно обновлён'
        }

    def update_seller(self, fields, session_id=None):
        seller_id = int(fields.get('id'))
        name = fields.get('name')
        inn = fields.get('inn')
        kpp = fields.get('kpp')
        self.manager.update_seller(seller_id, name, inn, kpp)

        if session_id:
            self._log(
                session_id, 'Редактирование', 'Поставщик',
                seller_id, name,
                f'ИНН: {inn}, КПП: {kpp}'
            )

        return {
            'success': True,
            'message': f'Поставщик "{name}" успешно обновлён'
        }

    def update_theme(self, fields, session_id=None):
        theme_id = int(fields.get('id'))
        name = fields.get('name')
        self.manager.update_theme(theme_id, name)

        if session_id:
            self._log(
                session_id, 'Редактирование', 'Тема',
                theme_id, name, None
            )

        return {
            'success': True,
            'message': f'Тема "{name}" успешно обновлена'
        }

    def update_receipt(self, fields, session_id=None):
        receipt_id = int(fields.get('id'))
        object_id = int(fields.get('objectId'))
        seller_object_name = fields.get('sellerObjectName')
        seller_id = int(fields.get('sellerId'))
        theme_id = int(fields.get('themeId'))
        location = fields.get('location')
        quantity = int(fields.get('quantity'))

        # Получаем имя объекта для лога
        obj = self.db.get_object_by_id(object_id)
        obj_name = obj['objectname'] if obj else str(object_id)

        self.manager.update_receipt(
            receipt_id, object_id, seller_object_name,
            seller_id, theme_id, location, quantity
        )

        if session_id:
            # Получаем данные о документах
            receipt = self.db.get_receipt_by_id(receipt_id)
            details_parts = [
                f'Наименование: {seller_object_name}',
                f'Кол-во: {quantity}'
            ]
            if receipt:
                if receipt.get('bill_number'):
                    details_parts.append(
                        f'Счёт: {receipt["bill_number"]}'
                    )
                if receipt.get('invoice_number'):
                    details_parts.append(
                        f'Накладная: {receipt["invoice_number"]}'
                    )

            self._log(
                session_id, 'Редактирование', 'Поступление',
                receipt_id, obj_name,
                ', '.join(details_parts)
            )

        return {
            'success': True,
            'message': 'Поступление успешно обновлено'
        }

    def update_writeoff(self, fields, files=None,
                        session_id=None):
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

        # Получаем имя объекта для лога
        obj = self.db.get_object_by_id(object_id)
        obj_name = obj['objectname'] if obj else str(object_id)

        # Получаем данные списания до обновления
        old_writeoff = self.db.get_writeoff_by_id(writeoff_id)

        self.manager.update_writeoff(
            writeoff_id, object_id, theme_id, quantity,
            writeoff_date, file_data, filename
        )

        if session_id:
            details_parts = [f'Кол-во: {quantity}']
            if writeoff_date:
                details_parts.append(f'Дата: {writeoff_date}')
            if filename:
                details_parts.append(f'Документ: {filename}')
            elif old_writeoff and old_writeoff.get('document_filename'):
                details_parts.append(
                    f'Документ: {old_writeoff["document_filename"]}'
                )

            self._log(
                session_id, 'Редактирование', 'Списание',
                writeoff_id, obj_name,
                ', '.join(details_parts)
            )

        return {
            'success': True,
            'message': 'Списание успешно обновлено'
        }

    def update_object(self, fields, session_id=None):
        object_id = int(fields.get('id'))

        # Получаем старое имя для лога
        old_obj = self.db.get_object_by_id(object_id)
        old_name = old_obj['objectname'] if old_obj else ''

        object_name = fields.get('objectName')
        self.manager.update_object(object_id, object_name)

        if session_id:
            details = None
            if old_name and old_name != object_name:
                details = f'Было: {old_name}'
            self._log(
                session_id, 'Редактирование', 'Объект',
                object_id, object_name, details
            )

        return {
            'success': True,
            'message': f'Объект "{object_name}" успешно обновлён'
        }

    def update_pricing(self, fields, session_id=None):
        pricing_id = int(fields.get('id'))
        price = float(fields.get('price'))
        tax = float(fields.get('tax'))

        # Получаем данные о поступлении и объекте
        pricing = self.db.get_pricing_by_id(pricing_id)
        obj_name = f'Цена #{pricing_id}'
        if pricing:
            receipt = self.db.get_receipt_by_id(
                pricing['receipt_id']
            )
            if receipt:
                obj = self.db.get_object_by_id(
                    receipt['object_id']
                )
                if obj:
                    obj_name = obj['objectname']

        self.pricing_manager.update_pricing(
            pricing_id, price, tax
        )

        if session_id:
            self._log(
                session_id, 'Редактирование', 'Цена',
                pricing_id, obj_name,
                f'Цена: {price}, НДС: {tax}%'
            )

        return {
            'success': True,
            'message': 'Цена успешно обновлена'
        }

    # ==================== DELETE (с логированием) ============
    def delete_object(self, object_id, session_id=None):
        obj = self.db.get_object_by_id(object_id)
        obj_name = obj['objectname'] if obj else str(object_id)

        # Считаем связанные записи для лога
        receipts = self.db.get_receipts_by_object(object_id)
        writeoffs = self.db.get_writeoffs_by_object(object_id)

        self.manager.delete_object(object_id)

        if session_id:
            details = (
                f'Удалено поступлений: {len(receipts)}, '
                f'списаний: {len(writeoffs)}'
            )
            self._log(
                session_id, 'Удаление', 'Объект',
                object_id, obj_name, details
            )

        return {
            'success': True,
            'message': 'Объект успешно удалён'
        }

    def delete_receipt(self, receipt_id, session_id=None):
        receipt = self.db.get_receipt_by_id(receipt_id)

        obj_name = str(receipt_id)
        details_parts = []

        if receipt:
            obj = self.db.get_object_by_id(receipt.get('object_id'))
            if obj:
                obj_name = obj['objectname']

            if receipt.get('seller_object_name'):
                details_parts.append(
                    f'Наименование: {receipt["seller_object_name"]}'
                )
            if receipt.get('bill_number'):
                details_parts.append(
                    f'Счёт: {receipt["bill_number"]}'
                )
            if receipt.get('invoice_number'):
                details_parts.append(
                    f'Накладная: {receipt["invoice_number"]}'
                )
            if receipt.get('quantity'):
                details_parts.append(
                    f'Кол-во: {receipt["quantity"]}'
                )

        self.manager.delete_receipt(receipt_id)

        if session_id:
            self._log(
                session_id, 'Удаление', 'Поступление',
                receipt_id, obj_name,
                ', '.join(details_parts) if details_parts else None
            )

        return {
            'success': True,
            'message': 'Поступление успешно удалено'
        }

    def delete_writeoff(self, writeoff_id, session_id=None):
        writeoff = self.db.get_writeoff_by_id(writeoff_id)

        obj_name = str(writeoff_id)
        details_parts = []

        if writeoff:
            obj_name = (
                writeoff.get('object_name') or str(writeoff_id)
            )
            if writeoff.get('writeoff_date'):
                details_parts.append(
                    f'Дата: {writeoff["writeoff_date"]}'
                )
            if writeoff.get('document_filename'):
                details_parts.append(
                    f'Документ: {writeoff["document_filename"]}'
                )
            if writeoff.get('quantity'):
                details_parts.append(
                    f'Кол-во: {writeoff["quantity"]}'
                )
            if writeoff.get('theme_name'):
                details_parts.append(
                    f'Тема: {writeoff["theme_name"]}'
                )

        self.manager.delete_writeoff(writeoff_id)

        if session_id:
            self._log(
                session_id, 'Удаление', 'Списание',
                writeoff_id, obj_name,
                ', '.join(details_parts) if details_parts else None
            )

        return {
            'success': True,
            'message': 'Списание успешно удалено'
        }

    def delete_pricing(self, pricing_id, session_id=None):
        pricing = self.db.get_pricing_by_id(pricing_id)

        obj_name = f'Цена #{pricing_id}'
        details = None

        if pricing:
            receipt = self.db.get_receipt_by_id(
                pricing['receipt_id']
            )
            if receipt:
                obj = self.db.get_object_by_id(
                    receipt['object_id']
                )
                if obj:
                    obj_name = obj['objectname']
            details = (
                f'Цена: {pricing["price"]}, '
                f'НДС: {pricing["tax"]}%'
            )

        self.pricing_manager.delete_pricing(pricing_id)

        if session_id:
            self._log(
                session_id, 'Удаление', 'Цена',
                pricing_id, obj_name, details
            )

        return {
            'success': True,
            'message': 'Цена успешно удалена'
        }

    def delete_seller(self, seller_id, session_id=None):
        seller = self.db.get_seller_by_id(seller_id)
        seller_name = seller['name'] if seller else str(seller_id)

        details = None
        if seller:
            details = f'ИНН: {seller["inn"]}, КПП: {seller["kpp"]}'

        self.manager.delete_seller(seller_id)

        if session_id:
            self._log(
                session_id, 'Удаление', 'Поставщик',
                seller_id, seller_name, details
            )

        return {
            'success': True,
            'message': 'Поставщик успешно удалён'
        }

    def delete_theme(self, theme_id, session_id=None):
        theme = self.db.get_theme_by_id(theme_id)
        theme_name = theme['name'] if theme else str(theme_id)

        self.manager.delete_theme(theme_id)

        if session_id:
            self._log(
                session_id, 'Удаление', 'Тема',
                theme_id, theme_name, None
            )

        return {
            'success': True,
            'message': 'Тема успешно удалена'
        }

    def update_seller(self, fields, session_id=None):
        seller_id = int(fields.get('id'))
        name = fields.get('name')
        inn = fields.get('inn')
        kpp = fields.get('kpp')

        old_seller = self.db.get_seller_by_id(seller_id)

        self.manager.update_seller(seller_id, name, inn, kpp)

        if session_id:
            details = f'ИНН: {inn}, КПП: {kpp}'
            if old_seller and old_seller['name'] != name:
                details = f'Было: {old_seller["name"]}, {details}'
            self._log(
                session_id, 'Редактирование', 'Поставщик',
                seller_id, name, details
            )

        return {
            'success': True,
            'message': f'Поставщик "{name}" успешно обновлён'
        }

    def update_theme(self, fields, session_id=None):
        theme_id = int(fields.get('id'))
        name = fields.get('name')

        old_theme = self.db.get_theme_by_id(theme_id)

        self.manager.update_theme(theme_id, name)

        if session_id:
            details = None
            if old_theme and old_theme['name'] != name:
                details = f'Было: {old_theme["name"]}'
            self._log(
                session_id, 'Редактирование', 'Тема',
                theme_id, name, details
            )

        return {
            'success': True,
            'message': f'Тема "{name}" успешно обновлена'
        }

    def update_user(self, fields, session_id=None):
        user_id = int(fields.get('id'))
        username = fields.get('username')
        admin = fields.get('admin', '').lower() == 'true'

        old_user = self.db.get_user_by_id(user_id)

        self.user_manager.update_user(user_id, username, admin)

        password = fields.get('password', '').strip()
        if password:
            password_hash = SessionManager.hash_password(password)
            self.user_manager.update_user_password(
                user_id, password_hash
            )

        if session_id:
            details_parts = []
            if old_user and old_user['username'] != username:
                details_parts.append(
                    f'Было: {old_user["username"]}'
                )
            role = 'Администратор' if admin else 'Пользователь'
            details_parts.append(f'Роль: {role}')
            if password:
                details_parts.append('Пароль изменён')

            self._log(
                session_id, 'Редактирование', 'Пользователь',
                user_id, username,
                ', '.join(details_parts)
            )

        return {
            'success': True,
            'message': f'Пользователь "{username}" успешно обновлён'
        }

    def delete_user(self, user_id, session_id=None):
        user = self.db.get_user_by_id(user_id)
        user_name = user['username'] if user else str(user_id)

        self.user_manager.delete_user(user_id)

        if session_id:
            self._log(
                session_id, 'Удаление', 'Пользователь',
                user_id, user_name, None
            )

        return {
            'success': True,
            'message': 'Пользователь успешно удалён'
        }

    # ==================== PRICING GET ====================
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
            raise ValueError(
                'Цена для этого поступления уже существует'
            )

        result = self.pricing_manager.create_pricing(
            receipt_id, price, tax
        )
        return {
            'success': True,
            'id': result['id'],
            'message': 'Цена успешно добавлена'
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