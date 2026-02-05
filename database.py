import yaml
import psycopg2
from psycopg2.extras import RealDictCursor

class Database:
    def __init__(self, config_path='config.yaml'):
        self._config = self._load_config(config_path)
        self._connection = None

    def _load_config(self, config_path):
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        return config['database']

    def _get_connection(self):
        if self._connection is None or self._connection.closed:
            self._connection = psycopg2.connect(
                host=self._config['host'],
                port=self._config['port'],
                database=self._config['name'],
                user=self._config['user'],
                password=self._config['password']
            )
        return self._connection

    def _call_function(self, func_name, params=None, fetch=False):
        conn = self._get_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            if params:
                placeholders = ', '.join(['%s'] * len(params))
                cur.execute(f"SELECT * FROM {func_name}({placeholders})", params)
            else:
                cur.execute(f"SELECT * FROM {func_name}()")

            if fetch:
                return cur.fetchall()
            conn.commit()

    def _call_function_scalar(self, func_name, params=None):
        conn = self._get_connection()
        with conn.cursor() as cur:
            if params:
                placeholders = ', '.join(['%s'] * len(params))
                cur.execute(f"SELECT {func_name}({placeholders})", params)
            else:
                cur.execute(f"SELECT {func_name}()")
            result = cur.fetchone()
            conn.commit()
            return result[0] if result else None

    def close(self):
        if self._connection and not self._connection.closed:
            self._connection.close()

    def test_connection(self):
        conn = self._get_connection()
        conn.close()
        self._connection = None
        return True

    def update_storage_stats(self):
        self._call_function_scalar('update_objects_storage_stats')

    def get_all_objects(self):
        return self._call_function('get_all_objects', fetch=True)

    def get_receipts_by_object(self, object_id):
        return self._call_function('get_receipts_by_object', (object_id,), fetch=True)

    def get_writeoffs_by_object(self, object_id):
        return self._call_function('get_writeoffs_by_object', (object_id,), fetch=True)

    def get_object_details(self, object_id):
        return {
            'receipts': self.get_receipts_by_object(object_id),
            'writeoffs': self.get_writeoffs_by_object(object_id)
        }

    def get_all_sellers(self):
        return self._call_function('get_all_sellers', fetch=True)

    def get_all_themes(self):
        return self._call_function('get_all_themes', fetch=True)

    def get_file(self, file_type, file_id):
        result = self._call_function('get_file', (file_type, file_id), fetch=True)
        return result[0] if result else None

class StorageManager:
    def __init__(self, db: Database):
        self._db = db

    def create_object(self, object_name):
        new_id = self._db._call_function_scalar('create_object', (object_name,))
        return {'id': new_id}

    def create_seller(self, name, inn, kpp):
        new_id = self._db._call_function_scalar('create_seller', (name, inn, kpp))
        return {'id': new_id}

    def create_theme(self, name):
        new_id = self._db._call_function_scalar('create_theme', (name,))
        return {'id': new_id}

    def create_bill(self, number, date, seller_id, file_data, filename):
        file_binary = psycopg2.Binary(file_data) if file_data else None
        new_id = self._db._call_function_scalar(
            'create_bill',
            (number, date, seller_id, file_binary, filename)
        )
        return {'id': new_id}

    def create_invoice(self, number, date, seller_id, bill_id, file_data, filename):
        file_binary = psycopg2.Binary(file_data) if file_data else None
        new_id = self._db._call_function_scalar(
            'create_invoice',
            (number, date, seller_id, bill_id, file_binary, filename)
        )
        return {'id': new_id}

    def create_entry_control(self, number, date, file_data, filename):
        file_binary = psycopg2.Binary(file_data) if file_data else None
        new_id = self._db._call_function_scalar(
            'create_entry_control',
            (number, date, file_binary, filename)
        )
        return {'id': new_id}

    def create_receipt(self, object_id, seller_object_name, seller_id, bill_id,
                       theme_id, invoice_id, entry_control_id, location, quantity):
        new_id = self._db._call_function_scalar(
            'create_receipt',
            (object_id, seller_object_name, seller_id, bill_id,
             theme_id, invoice_id, entry_control_id, location, quantity)
        )
        return {'id': new_id}

    def create_writeoff(self, object_id, theme_id, quantity):
        new_id = self._db._call_function_scalar(
            'create_writeoff',
            (object_id, theme_id, quantity)
        )
        return {'id': new_id}