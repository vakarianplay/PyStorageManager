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

    def _rollback(self):
        """Откатывает транзакцию при ошибке."""
        try:
            if self._connection and not self._connection.closed:
                self._connection.rollback()
        except Exception:
            pass

    def call_function(self, func_name, params=None, fetch=False):
        conn = self._get_connection()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                if params:
                    placeholders = ', '.join(['%s'] * len(params))
                    cur.execute(
                        f"SELECT * FROM {func_name}({placeholders})",
                        params
                    )
                else:
                    cur.execute(f"SELECT * FROM {func_name}()")

                if fetch:
                    result = cur.fetchall()
                    conn.commit()
                    return result
                conn.commit()
        except psycopg2.Error as e:
            self._rollback()
            raise e

    def call_function_scalar(self, func_name, params=None):
        conn = self._get_connection()
        try:
            with conn.cursor() as cur:
                if params:
                    placeholders = ', '.join(['%s'] * len(params))
                    cur.execute(
                        f"SELECT {func_name}({placeholders})",
                        params
                    )
                else:
                    cur.execute(f"SELECT {func_name}()")
                result = cur.fetchone()
                conn.commit()
                return result[0] if result else None
        except psycopg2.Error as e:
            self._rollback()
            raise e

    def close(self):
        if self._connection and not self._connection.closed:
            self._connection.close()

    def test_connection(self):
        conn = self._get_connection()
        conn.close()
        self._connection = None
        return True

    # Storage methods
    def update_storage_stats(self):
        self.call_function_scalar('update_objects_storage_stats')

    def get_all_objects(self):
        return self.call_function('get_all_objects', fetch=True)

    def get_object_by_id(self, object_id):
        result = self.call_function(
            'get_object_by_id', (object_id,), fetch=True
        )
        return result[0] if result else None

    def get_receipts_by_object(self, object_id):
        return self.call_function(
            'get_receipts_by_object', (object_id,), fetch=True
        )

    def get_receipt_by_id(self, receipt_id):
        result = self.call_function(
            'get_receipt_by_id', (receipt_id,), fetch=True
        )
        return result[0] if result else None

    def get_writeoffs_by_object(self, object_id):
        return self.call_function(
            'get_writeoffs_by_object', (object_id,), fetch=True
        )

    def get_writeoff_by_id(self, writeoff_id):
        result = self.call_function(
            'get_writeoff_by_id', (writeoff_id,), fetch=True
        )
        return result[0] if result else None

    def get_object_details(self, object_id):
        return {
            'receipts': self.get_receipts_by_object(object_id),
            'writeoffs': self.get_writeoffs_by_object(object_id)
        }

    def get_all_sellers(self):
        return self.call_function('get_all_sellers', fetch=True)

    def get_seller_by_id(self, seller_id):
        result = self.call_function(
            'get_seller_by_id', (seller_id,), fetch=True
        )
        return result[0] if result else None

    def get_all_themes(self):
        return self.call_function('get_all_themes', fetch=True)

    def get_theme_by_id(self, theme_id):
        result = self.call_function(
            'get_theme_by_id', (theme_id,), fetch=True
        )
        return result[0] if result else None

    def get_file(self, file_type, file_id):
        result = self.call_function(
            'get_file', (file_type, file_id), fetch=True
        )
        return result[0] if result else None

    # Search methods
    def search_objects_by_name(self, search_text):
        return self.call_function(
            'search_objects_by_name', (search_text,), fetch=True
        )

    def search_objects_by_seller_name(self, search_text):
        return self.call_function(
            'search_objects_by_seller_name', (search_text,),
            fetch=True
        )

    def search_objects_by_theme(self, theme_id):
        return self.call_function(
            'search_objects_by_theme', (theme_id,), fetch=True
        )

    def search_objects_by_bill(self, search_text):
        return self.call_function(
            'search_objects_by_bill', (search_text,), fetch=True
        )

    def search_objects_by_invoice(self, search_text):
        return self.call_function(
            'search_objects_by_invoice', (search_text,), fetch=True
        )

    # User methods
    def authenticate_user(self, username, password_hash):
        result = self.call_function(
            'authenticate_user', (username, password_hash),
            fetch=True
        )
        return result[0] if result else None

    def get_all_users(self):
        return self.call_function('get_all_users', fetch=True)

    def get_user_by_id(self, user_id):
        result = self.call_function(
            'get_user_by_id', (user_id,), fetch=True
        )
        return result[0] if result else None

    # Pricing methods
    def get_all_pricing(self):
        return self.call_function('get_all_pricing', fetch=True)

    def get_pricing_by_id(self, pricing_id):
        result = self.call_function(
            'get_pricing_by_id', (pricing_id,), fetch=True
        )
        return result[0] if result else None

    def get_pricing_by_receipt(self, receipt_id):
        result = self.call_function(
            'get_pricing_by_receipt', (receipt_id,), fetch=True
        )
        return result[0] if result else None
    
    # Logging methods
    def add_log(self, user_id, username, action,
                entity_type, entity_id, entity_name,
                details=None):
        return self.call_function_scalar(
            'add_log',
            (user_id, username, action,
             entity_type, entity_id, entity_name, details)
        )

    def get_all_logs(self, limit=500, offset=0):
        return self.call_function(
            'get_all_logs', (limit, offset), fetch=True
        )

    def get_logs_count(self):
        return self.call_function_scalar('get_logs_count')

    def search_logs(self, search_text, limit=500, offset=0):
        return self.call_function(
            'search_logs', (search_text, limit, offset),
            fetch=True
        )