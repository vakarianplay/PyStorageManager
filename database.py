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

    def _execute(self, query, params=None, fetch=False, returning=False):
        conn = self._get_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, params)
            if fetch:
                return cur.fetchall()
            if returning:
                result = cur.fetchone()
                conn.commit()
                return result
            conn.commit()

    def close(self):
        if self._connection and not self._connection.closed:
            self._connection.close()

    def test_connection(self):
        conn = self._get_connection()
        conn.close()
        self._connection = None
        return True

    def update_storage_stats(self):
        self._execute("SELECT update_objects_storage_stats();")

    def get_all_objects(self):
        return self._execute("""
            SELECT id, objectName, amount, write_off, balance
            FROM ObjectsStorage
            ORDER BY objectName
        """, fetch=True)

    def get_receipts_by_object(self, object_id):
        return self._execute("""
            SELECT 
                r.id,
                o.objectName AS object_name,
                r.sellerObjectName,
                r.quantity,
                r.location,
                s.name AS seller_name,
                t.name AS theme_name,
                b.id AS bill_id,
                b.number AS bill_number,
                b.date AS bill_date,
                b.filename AS bill_filename,
                i.id AS invoice_id,
                i.number AS invoice_number,
                i.date AS invoice_date,
                i.filename AS invoice_filename,
                ec.id AS entry_control_id,
                ec.number AS entry_control_number,
                ec.date AS entry_control_date,
                ec.filename AS entry_control_filename
            FROM Receipt r
            LEFT JOIN ObjectsStorage o ON r.objectID = o.id
            LEFT JOIN Sellers s ON r.sellerID = s.id
            LEFT JOIN Themes t ON r.themeID = t.id
            LEFT JOIN Bills b ON r.billID = b.id
            LEFT JOIN Invoices i ON r.invoiceID = i.id
            LEFT JOIN EntryControl ec ON r.entryControlId = ec.id
            WHERE r.objectID = %s
            ORDER BY b.date DESC, r.id DESC
        """, (object_id,), fetch=True)

    def get_writeoffs_by_object(self, object_id):
        return self._execute("""
            SELECT 
                w.id,
                o.objectName AS object_name,
                w.quantity,
                t.name AS theme_name
            FROM WriteOff w
            LEFT JOIN ObjectsStorage o ON w.objectID = o.id
            LEFT JOIN Themes t ON w.themeID = t.id
            WHERE w.objectID = %s
            ORDER BY w.id DESC
        """, (object_id,), fetch=True)

    def get_object_details(self, object_id):
        return {
            'receipts': self.get_receipts_by_object(object_id),
            'writeoffs': self.get_writeoffs_by_object(object_id)
        }

    def get_all_sellers(self):
        return self._execute("SELECT id, name, inn, kpp FROM Sellers ORDER BY name", fetch=True)

    def get_all_themes(self):
        return self._execute("SELECT id, name FROM Themes ORDER BY name", fetch=True)

    def get_file(self, file_type, file_id):
        table_map = {
            'bill': 'Bills',
            'invoice': 'Invoices',
            'entry_control': 'EntryControl'
        }
        table = table_map.get(file_type)
        if not table:
            return None
        result = self._execute(
            f"SELECT file, filename FROM {table} WHERE id = %s",
            (file_id,), fetch=True
        )
        return result[0] if result else None

class StorageManager:
    def __init__(self, db: Database):
        self._db = db

    def create_object(self, object_name):
        return self._db._execute("""
            INSERT INTO ObjectsStorage (objectName, amount, write_off, balance)
            VALUES (%s, 0, 0, 0)
            RETURNING id
        """, (object_name,), returning=True)

    def create_seller(self, name, inn, kpp):
        return self._db._execute("""
            INSERT INTO Sellers (name, inn, kpp)
            VALUES (%s, %s, %s)
            RETURNING id
        """, (name, inn, kpp), returning=True)

    def create_theme(self, name):
        return self._db._execute("""
            INSERT INTO Themes (name)
            VALUES (%s)
            RETURNING id
        """, (name,), returning=True)

    def create_bill(self, number, date, seller_id, file_data, filename):
        return self._db._execute("""
            INSERT INTO Bills (number, date, sellerID, file, filename)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING id
        """, (number, date, seller_id,
              psycopg2.Binary(file_data) if file_data else None,
              filename), returning=True)

    def create_invoice(self, number, date, seller_id, bill_id, file_data, filename):
        return self._db._execute("""
            INSERT INTO Invoices (number, date, sellerID, billID, file, filename)
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING id
        """, (number, date, seller_id, bill_id,
              psycopg2.Binary(file_data) if file_data else None,
              filename), returning=True)

    def create_entry_control(self, number, date, file_data, filename):
        return self._db._execute("""
            INSERT INTO EntryControl (number, date, file, filename)
            VALUES (%s, %s, %s, %s)
            RETURNING id
        """, (number, date,
              psycopg2.Binary(file_data) if file_data else None,
              filename), returning=True)

    def create_receipt(self, object_id, seller_object_name, seller_id, bill_id,
                       theme_id, invoice_id, entry_control_id, location, quantity):
        return self._db._execute("""
            INSERT INTO Receipt (objectID, sellerObjectName, sellerID, billID,
                                 themeID, invoiceID, entryControlId, location, quantity)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """, (object_id, seller_object_name, seller_id, bill_id,
              theme_id, invoice_id, entry_control_id, location, quantity), returning=True)

    def create_writeoff(self, object_id, theme_id, quantity):
        return self._db._execute("""
            INSERT INTO WriteOff (objectID, themeID, quantity)
            VALUES (%s, %s, %s)
            RETURNING id
        """, (object_id, theme_id, quantity), returning=True)