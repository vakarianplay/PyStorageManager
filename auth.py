import hashlib
import secrets
import time

class SessionManager:
    def __init__(self):
        self._sessions = {}
        self._session_timeout = 3600 * 8  # 8 часов

    def create_session(self, user_data):
        session_id = secrets.token_hex(32)
        self._sessions[session_id] = {
            'user': user_data,
            'created_at': time.time()
        }
        return session_id

    def get_session(self, session_id):
        if not session_id or session_id not in self._sessions:
            return None

        session = self._sessions[session_id]
        if time.time() - session['created_at'] > self._session_timeout:
            del self._sessions[session_id]
            return None

        return session['user']

    def delete_session(self, session_id):
        if session_id in self._sessions:
            del self._sessions[session_id]

    def cleanup_expired(self):
        current_time = time.time()
        expired = [
            sid for sid, data in self._sessions.items()
            if current_time - data['created_at'] > self._session_timeout
        ]
        for sid in expired:
            del self._sessions[sid]

    @staticmethod
    def hash_password(password):
        return hashlib.sha256(password.encode()).hexdigest()