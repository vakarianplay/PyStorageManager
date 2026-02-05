import psycopg2
from database import Database

class StorageManager:
    def __init__(self, db: Database):
        self._db = db

    # Objects
    def create_object(self, object_name):
        new_id = self._db.call_function_scalar('create_object', (object_name,))
        return {'id': new_id}

    def update_object(self, object_id, object_name):
        result = self._db.call_function_scalar('update_object', (object_id, object_name))
        return {'success': result}

    def delete_object(self, object_id):
        result = self._db.call_function_scalar('delete_object', (object_id,))
        return {'success': result}

    # Sellers
    def create_seller(self, name, inn, kpp):
        new_id = self._db.call_function_scalar('create_seller', (name, inn, kpp))
        return {'id': new_id}

    def update_seller(self, seller_id, name, inn, kpp):
        result = self._db.call_function_scalar('update_seller', (seller_id, name, inn, kpp))
        return {'success': result}

    def delete_seller(self, seller_id):
        result = self._db.call_function_scalar('delete_seller', (seller_id,))
        return {'success': result}

    # Themes
    def create_theme(self, name):
        new_id = self._db.call_function_scalar('create_theme', (name,))
        return {'id': new_id}

    def update_theme(self, theme_id, name):
        result = self._db.call_function_scalar('update_theme', (theme_id, name))
        return {'success': result}

    def delete_theme(self, theme_id):
        result = self._db.call_function_scalar('delete_theme', (theme_id,))
        return {'success': result}

    # Bills
    def create_bill(self, number, date, seller_id, file_data, filename):
        file_binary = psycopg2.Binary(file_data) if file_data else None
        new_id = self._db.call_function_scalar(
            'create_bill',
            (number, date, seller_id, file_binary, filename)
        )
        return {'id': new_id}

    # Invoices
    def create_invoice(self, number, date, seller_id, bill_id, file_data, filename):
        file_binary = psycopg2.Binary(file_data) if file_data else None
        new_id = self._db.call_function_scalar(
            'create_invoice',
            (number, date, seller_id, bill_id, file_binary, filename)
        )
        return {'id': new_id}

    # Entry Control
    def create_entry_control(self, number, date, file_data, filename):
        file_binary = psycopg2.Binary(file_data) if file_data else None
        new_id = self._db.call_function_scalar(
            'create_entry_control',
            (number, date, file_binary, filename)
        )
        return {'id': new_id}

    # Receipts
    def create_receipt(self, object_id, seller_object_name, seller_id, bill_id,
                       theme_id, invoice_id, entry_control_id, location, quantity):
        new_id = self._db.call_function_scalar(
            'create_receipt',
            (object_id, seller_object_name, seller_id, bill_id,
             theme_id, invoice_id, entry_control_id, location, quantity)
        )
        return {'id': new_id}

    def update_receipt(self, receipt_id, object_id, seller_object_name, seller_id,
                       theme_id, location, quantity):
        result = self._db.call_function_scalar(
            'update_receipt',
            (receipt_id, object_id, seller_object_name, seller_id,
             theme_id, location, quantity)
        )
        return {'success': result}

    def delete_receipt(self, receipt_id):
        result = self._db.call_function_scalar('delete_receipt', (receipt_id,))
        return {'success': result}

    # Writeoffs
    def create_writeoff(self, object_id, theme_id, quantity):
        new_id = self._db.call_function_scalar(
            'create_writeoff',
            (object_id, theme_id, quantity)
        )
        return {'id': new_id}

    def update_writeoff(self, writeoff_id, object_id, theme_id, quantity):
        result = self._db.call_function_scalar(
            'update_writeoff',
            (writeoff_id, object_id, theme_id, quantity)
        )
        return {'success': result}

    def delete_writeoff(self, writeoff_id):
        result = self._db.call_function_scalar('delete_writeoff', (writeoff_id,))
        return {'success': result}