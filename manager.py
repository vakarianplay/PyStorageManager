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

    def update_receipt(self, receipt_id, object_id,
                    seller_object_name, seller_id,
                    theme_id, location, quantity,
                    bill_number=None, bill_date=None,
                    bill_file=None, bill_filename=None,
                    invoice_number=None, invoice_date=None,
                    invoice_file=None, invoice_filename=None,
                    ec_number=None, ec_date=None,
                    ec_file=None, ec_filename=None):
        bill_binary = (
            psycopg2.Binary(bill_file) if bill_file else None
        )
        invoice_binary = (
            psycopg2.Binary(invoice_file) if invoice_file else None
        )
        ec_binary = (
            psycopg2.Binary(ec_file) if ec_file else None
        )

        result = self._db.call_function_scalar(
            'update_receipt',
            (receipt_id, object_id, seller_object_name,
            seller_id, theme_id, location, quantity,
            bill_number, bill_date,
            bill_binary, bill_filename,
            invoice_number, invoice_date,
            invoice_binary, invoice_filename,
            ec_number, ec_date,
            ec_binary, ec_filename)
        )
        return {'success': result}

    def delete_receipt(self, receipt_id):
        result = self._db.call_function_scalar('delete_receipt', (receipt_id,))
        return {'success': result}

    def create_writeoff(self, object_id, theme_id, quantity,
                    writeoff_date, file_data, filename):
        file_binary = psycopg2.Binary(file_data) if file_data else None
        new_id = self._db.call_function_scalar(
            'create_writeoff',
            (object_id, theme_id, quantity,
            writeoff_date, file_binary, filename)
        )
        return {'id': new_id}

    def update_writeoff(self, writeoff_id, object_id, theme_id,
                        quantity, writeoff_date, file_data, filename):
        file_binary = psycopg2.Binary(file_data) if file_data else None
        result = self._db.call_function_scalar(
            'update_writeoff',
            (writeoff_id, object_id, theme_id, quantity,
            writeoff_date, file_binary, filename)
        )
        return {'success': result}

    def delete_writeoff(self, writeoff_id):
        result = self._db.call_function_scalar('delete_writeoff', (writeoff_id,))
        return {'success': result}

class UserManager:
    def __init__(self, db: Database):
        self._db = db

    def create_user(self, username, password_hash, admin=False):
        new_id = self._db.call_function_scalar('create_user', (username, password_hash, admin))
        return {'id': new_id}

    def update_user(self, user_id, username, admin):
        result = self._db.call_function_scalar('update_user', (user_id, username, admin))
        return {'success': result}

    def update_user_password(self, user_id, password_hash):
        result = self._db.call_function_scalar('update_user_password', (user_id, password_hash))
        return {'success': result}

    def delete_user(self, user_id):
        result = self._db.call_function_scalar('delete_user', (user_id,))
        return {'success': result}
    
class PricingManager:
    def __init__(self, db: Database):
        self._db = db

    def create_pricing(self, receipt_id, price, tax):
        new_id = self._db.call_function_scalar(
            'create_pricing',
            (receipt_id, price, tax)
        )
        return {'id': new_id}

    def update_pricing(self, pricing_id, price, tax):
        result = self._db.call_function_scalar(
            'update_pricing',
            (pricing_id, price, tax)
        )
        return {'success': result}

    def delete_pricing(self, pricing_id):
        result = self._db.call_function_scalar('delete_pricing', (pricing_id,))
        return {'success': result}