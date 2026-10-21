# ════════════════════════════════════════════════════════════════
#   𝐓𝐎𝐂𝐗𝐈𝐂 𝐍𝐄𝐓𝐖𝐎𝐑𝐊  —  Made in India 🇮🇳
#   ─────────────────────────────────────────────────────────────
#   Copyright waived. Released to Public Domain (CC0 1.0 Universal)
#   No rights reserved. Koi bhi use kar sakta hai — personal ya
#   commercial. Modify, redistribute, apna naam lagao — sab allowed.
#   Attribution optional hai, koi zaroorat nahi.
#   ─────────────────────────────────────────────────────────────
#   Full text: https://creativecommons.org/publicdomain/zero/1.0/
# ════════════════════════════════════════════════════════════════

from dotenv import load_dotenv
load_dotenv()

import requests
import json
import base64
import uuid
import re
import os
import sys
import time
import logging
import signal
import shutil
from datetime import datetime
from zoneinfo import ZoneInfo
from concurrent.futures import ThreadPoolExecutor
import threading

import PyPDF2
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError

# ============== LOGGING ==============
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ============== IST TIMEZONE ==============
IST = ZoneInfo("Asia/Kolkata")

def now_ist():
    return datetime.now(IST)

def ist_date():
    return now_ist().date().isoformat()

def ist_stamp():
    return now_ist().strftime("%d/%m/%Y %H:%M:%S")

def ist_filename_stamp():
    return now_ist().strftime("%Y%m%d_%H%M%S")

# ============== ENV CONFIG (all .get with "") ==============
TELEGRAM_BOT_TOKEN  = os.environ.get("TELEGRAM_BOT_TOKEN", "8972722744:AAHkp0uYDBYgIyUqPu2g-JTzvb8rcwDHiLo")
ANTI_CAPTCHA_KEY    = os.environ.get("ANTI_CAPTCHA_KEY", "ca9d9ef71c4de20ce587e5cbeeb63d5d")
MONGO_URI           = os.environ.get("MONGO_URI", "mongodb+srv://RahulRORO:Rahulboss@cluster0.xfwjmqk.mongodb.net/?appName=Cluster0")
MONGO_DB_NAME       = os.environ.get("MONGO_DB", "Adhar_Card")
OWNER_IDS           = {int(x) for x in os.environ.get("OWNER_IDS", "8460497291").split(",") if x.strip()}
OWNER_ID            = next(iter(OWNER_IDS), 0)
OWNER_USERNAME      = os.environ.get("OWNER_USERNAME", "@anonymous00000001")
LOG_BOT_TOKEN       = os.environ.get("LOG_BOT_TOKEN", "8600548036:AAHJLdsUhbXhKAOqshsSUUHBUJDJr3QKJvk").strip() or TELEGRAM_BOT_TOKEN

FORCE_JOIN_ENABLED  = os.environ.get("FORCE_JOIN_ENABLED", "")
FORCE_JOIN_CHAT_ID  = os.environ.get("FORCE_JOIN_CHAT_ID", "")
FORCE_JOIN_NAME     = os.environ.get("FORCE_JOIN_NAME", "")
FORCE_JOIN_LINK     = os.environ.get("FORCE_JOIN_LINK", "")

LOG_CHANNEL_ENABLED = os.environ.get("LOG_CHANNEL_ENABLED", "")
LOG_CHANNEL_ID      = os.environ.get("LOG_CHANNEL_ID", "")

PROXY_ENABLED       = os.environ.get("PROXY_ENABLED", "")
PROXY_URL           = os.environ.get("PROXY_URL", "")
PROXY_LIST          = os.environ.get("PROXY_LIST", "")
PROXY_ROTATE        = os.environ.get("PROXY_ROTATE", "")

FORCE_JOIN_SEED = {
    "enabled": FORCE_JOIN_ENABLED.strip() == "1",
    "chat_id": FORCE_JOIN_CHAT_ID.strip(),
    "name":    FORCE_JOIN_NAME.strip(),
    "link":    FORCE_JOIN_LINK.strip(),
}
LOG_CHANNEL_SEED = {
    "enabled": LOG_CHANNEL_ENABLED.strip() == "1",
    "id":      LOG_CHANNEL_ID.strip(),
}
PROXY_SEED = {
    "enabled": PROXY_ENABLED.strip() == "1",
    "url":     PROXY_URL.strip(),
    "list":    [p.strip() for p in PROXY_LIST.split(",") if p.strip()],
    "rotate":  PROXY_ROTATE.strip() != "0",
}

def is_owner(uid):
    try:
        return int(uid) in OWNER_IDS
    except (TypeError, ValueError):
        return False

# ============== GLOBALS / CACHE ==============
_settings_lock = threading.Lock()
_settings_cache = {
    "free_credits": 50,
    "free_mode": False,
    "proxy": dict(PROXY_SEED),
    "force_channel": dict(FORCE_JOIN_SEED),
    "log_channel": dict(LOG_CHANNEL_SEED),
}

_telegram_session = None
_bot_username = None
VERIFIED_USERS = set()

user_sessions  = {}
_sessions_lock = threading.Lock()

_proxy_rr_lock    = threading.Lock()
_proxy_rr_counter = 0

_mongo_client = None
_db = None
_users = None
_settings = None
_db_init_lk = threading.Lock()

_PDF_EXECUTOR = ThreadPoolExecutor(max_workers=4, thread_name_prefix="pdfcrack")

# ============== MONGO ==============
def _get_db():
    global _mongo_client, _db, _users, _settings
    if _db is None:
        with _db_init_lk:
            if _db is None:
                uri    = MONGO_URI or "mongodb://localhost:27017"
                dbname = MONGO_DB_NAME or "aadhaar_bot"
                _mongo_client = MongoClient(uri, serverSelectionTimeoutMS=10000)
                _db       = _mongo_client[dbname]
                _users    = _db["users"]
                _settings = _db["settings"]
    return _db

def _init_db():
    _get_db()
    _users.create_index("_id", unique=True)
    _settings.create_index("key", unique=True)
    defaults = {
        "free_credits": 50,
        "free_mode": False,
        "proxy": PROXY_SEED,
        "force_channel": FORCE_JOIN_SEED,
        "log_channel": LOG_CHANNEL_SEED,
    }
    for k, v in defaults.items():
        _settings.update_one({"key": k}, {"$setOnInsert": {"key": k, "value": v}}, upsert=True)
    _init_settings_cache()
    _migrate_from_json()
    _migrate_from_sqlite()

def _init_settings_cache():
    for doc in _settings.find({}):
        _settings_cache[doc["key"]] = doc["value"]

def _set_setting(key, value):
    with _settings_lock:
        _settings_cache[key] = value
    _settings.update_one({"key": key}, {"$set": {"value": value}}, upsert=True)

def _migrate_from_json():
    if not os.path.exists("users.json"):
        return
    try:
        with open("users.json") as f:
            data = json.load(f)
        for uid, u in data.items():
            _users.update_one(
                {"_id": str(uid)},
                {"$setOnInsert": {
                    "credits": int(u.get("credits", 0)) if not u.get("lifetime") else 0,
                    "lifetime": bool(u.get("lifetime")),
                    "referred_by": u.get("referred_by"),
                    "referral_count": int(u.get("referral_count", 0)),
                    "joined": u.get("joined", now_ist().isoformat()),
                    "banned": bool(u.get("banned")),
                    "channel_verified": False,
                }},
                upsert=True,
            )
        os.rename("users.json", "users.json.migrated")
        logger.info(f"Migrated {len(data)} users from JSON")
    except Exception as e:
        logger.error(f"json migrate: {e}")

def _migrate_from_sqlite():
    if not os.path.exists("bot.db"):
        return
    try:
        import sqlite3
        conn = sqlite3.connect("bot.db")
        conn.row_factory = sqlite3.Row
        for r in conn.execute("SELECT * FROM users").fetchall():
            _users.update_one(
                {"_id": str(r["user_id"])},
                {"$setOnInsert": {
                    "credits": r["credits"],
                    "lifetime": bool(r["lifetime"]),
                    "referred_by": r["referred_by"],
                    "referral_count": r["referral_count"],
                    "joined": r["joined"],
                    "banned": bool(r["banned"]),
                    "channel_verified": bool(r["channel_verified"]),
                }},
                upsert=True,
            )
        for s in conn.execute("SELECT key, value FROM settings").fetchall():
            v = s["value"]
            if s["key"] == "free_credits": v = int(v)
            elif s["key"] == "free_mode":  v = (v == "1")
            _settings.update_one({"key": s["key"]},
                                 {"$setOnInsert": {"key": s["key"], "value": v}},
                                 upsert=True)
        conn.close()
        os.rename("bot.db", "bot.db.migrated")
        logger.info("Migrated SQLite → MongoDB")
    except Exception as e:
        logger.error(f"sqlite migrate: {e}")

# ============== PROXY CONFIG ==============
def get_proxy_config():
    return dict(_settings_cache.get("proxy", dict(PROXY_SEED)))

def _set_proxy(cfg):
    _set_setting("proxy", cfg)
    return cfg

def set_proxy_config(enabled=None, url=None, rotate=None):
    c = get_proxy_config()
    if enabled is not None: c["enabled"] = bool(enabled)
    if url     is not None: c["url"]     = url.strip()
    if rotate  is not None: c["rotate"]  = bool(rotate)
    return _set_proxy(c)

def add_proxies(urls):
    c = get_proxy_config()
    existing = set(c.get("list", []))
    added = []
    for u in urls:
        u = u.strip()
        if u and u not in existing:
            c.setdefault("list", []).append(u)
            existing.add(u)
            added.append(u)
    _set_proxy(c)
    return added

def remove_proxy_at(idx):
    c = get_proxy_config()
    lst = c.get("list", [])
    if 0 <= idx < len(lst):
        removed = lst.pop(idx)
        _set_proxy(c)
        return removed
    return None

def clear_proxies():
    global _proxy_rr_counter
    c = get_proxy_config()
    c["list"] = []
    c["url"]  = ""
    with _proxy_rr_lock:
        _proxy_rr_counter = 0
    return _set_proxy(c)

def get_next_proxy():
    global _proxy_rr_counter
    c = get_proxy_config()
    if not c.get("enabled"):
        return None
    lst = c.get("list", [])
    if lst:
        if not c.get("rotate", True):
            return lst[0]
        with _proxy_rr_lock:
            idx = _proxy_rr_counter % len(lst)
            _proxy_rr_counter += 1
        return lst[idx]
    if c.get("url"):
        return c["url"]
    return None

def test_proxy(url, timeout=8):
    try:
        s = time.time()
        r = requests.get(
            "https://api.ipify.org?format=json",
            proxies={"http": url, "https": url},
            timeout=timeout,
        )
        lat = int((time.time() - s) * 1000)
        if r.status_code == 200:
            return True, lat, r.json().get("ip", "ok")
        return False, lat, f"HTTP {r.status_code}"
    except Exception as e:
        return False, 0, str(e)[:80]

# ============== FORCE JOIN CONFIG ==============
def get_force_channel():
    return dict(_settings_cache.get("force_channel", FORCE_JOIN_SEED))

def set_force_channel(**kw):
    c = get_force_channel()
    for k, v in kw.items():
        if k in c: c[k] = v
    _set_setting("force_channel", c)
    return c

def force_join_enabled():
    c = get_force_channel()
    return c.get("enabled") and bool(c.get("chat_id"))

# ============== LOG CHANNEL CONFIG ==============
def get_log_channel():
    return dict(_settings_cache.get("log_channel", LOG_CHANNEL_SEED))

def set_log_channel(**kw):
    c = get_log_channel()
    for k, v in kw.items():
        if k in c: c[k] = v
    _set_setting("log_channel", c)
    return c

def log_channel_enabled():
    c = get_log_channel()
    return c.get("enabled") and bool(c.get("id"))

# ============== FREE CREDITS / MODE ==============
def get_free_credits(): return _settings_cache.get("free_credits", 50)
def set_free_credits(a): _set_setting("free_credits", int(a))
def is_free_mode():     return bool(_settings_cache.get("free_mode", False))

def toggle_free_mode():
    v = not is_free_mode()
    _set_setting("free_mode", v)
    return v

# ============== SESSION FACTORY ==============
def create_session(use_proxy=False):
    session = requests.Session()
    if use_proxy:
        proxy_url = get_next_proxy()
        if proxy_url:
            session.proxies = {"http": proxy_url, "https": proxy_url}
    session.mount('https://', requests.adapters.HTTPAdapter(
        pool_connections=100, pool_maxsize=300, max_retries=3, pool_block=False
    ))
    session.mount('http://', requests.adapters.HTTPAdapter(
        pool_connections=100, pool_maxsize=300, max_retries=3, pool_block=False
    ))
    return session

def get_telegram_session():
    global _telegram_session
    if _telegram_session is None:
        _telegram_session = create_session(use_proxy=False)
    return _telegram_session

def get_uidai_session():
    return create_session(use_proxy=True)

# ============== PDF CRACKER ==============
class PDFPasswordCracker:
    def try_password(self, pdf_path, password):
        try:
            with open(pdf_path, 'rb') as file:
                if PyPDF2.PdfReader(file).decrypt(password):
                    return True, password
        except Exception as e:
            logger.debug(f"pwd {password}: {e}")
        return False, None

    def decrypt_pdf(self, pdf_path, password, output_path=None):
        try:
            if output_path is None:
                output_path = pdf_path.replace('.pdf', '_decrypted.pdf')
            with open(pdf_path, 'rb') as f:
                reader = PyPDF2.PdfReader(f)
                reader.decrypt(password)
                writer = PyPDF2.PdfWriter()
                for page in reader.pages:
                    writer.add_page(page)
                with open(output_path, 'wb') as out:
                    writer.write(out)
            return output_path
        except Exception as e:
            logger.error(f"decrypt_pdf: {e}")
            return None

    def crack_pdf(self, pdf_path, name):
        name_upper = name.upper()
        prefix = name_upper[:4] if len(name_upper) >= 4 else name_upper
        patterns = [('first4', prefix)]
        if len(name_upper) >= 6:
            patterns.append(('first6', name_upper[:6]))
        patterns.append(('lower_first4', prefix.lower()))
        patterns.append(('title_first4', prefix.title()))
        current_year = now_ist().year
        years = list(range(1940, current_year + 2))
        candidates = []
        for y in years:
            for _, p in patterns:
                candidates.append(f"{p}{y}")
        seen = set()
        unique = [x for x in candidates if not (x in seen or seen.add(x))]
        for i in range(0, len(unique), 20):
            batch = unique[i:i+20]
            futures = [_PDF_EXECUTOR.submit(self.try_password, pdf_path, p) for p in batch]
            for fut, pwd in zip(futures, batch):
                try:
                    ok, found = fut.result(timeout=2)
                    if ok:
                        dec = self.decrypt_pdf(pdf_path, found)
                        return True, found, dec
                except Exception:
                    continue
        for _, p in patterns:
            ok, found = self.try_password(pdf_path, p)
            if ok:
                dec = self.decrypt_pdf(pdf_path, found)
                return True, found, dec
        return False, None, None

# ============== AADHAAR BOT ==============
class AadhaarBot:
    def __init__(self):
        self.session = get_uidai_session()
        self.session.headers.update({
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'en_IN',
            'Connection': 'keep-alive',
            'Content-Type': 'application/json',
            'Origin': 'https://myaadhaar.uidai.gov.in',
            'Referer': 'https://myaadhaar.uidai.gov.in/',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-site',
            'User-Agent': 'Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 '
                          '(KHTML, like Gecko) Chrome/124.0.0.0 Mobile Safari/537.36',
            'appid': 'MYAADHAAR',
            'sec-ch-ua': '"Not-A.Brand";v="99", "Chromium";v="124"',
            'sec-ch-ua-mobile': '?1',
            'sec-ch-ua-platform': '"Android"',
        })
        self.cracker = PDFPasswordCracker()

    def generate_transaction_id(self):
        return str(uuid.uuid4())

    def is_base64(self, s):
        if not isinstance(s, str) or len(s) < 100:
            return False
        if s.startswith('data:'):
            s = s.split(',')[1] if ',' in s else s
        if len(s) % 4 != 0:
            return False
        try:
            base64.b64decode(s, validate=True)
            return True
        except Exception:
            return False

    def detect_file_type(self, b):
        if b[:4] == b'%PDF':
            return 'pdf'
        if b[:8] == b'\x89PNG\r\n\x1a\n':
            return 'png'
        if b[:2] == b'\xff\xd8':
            return 'jpg'
        return 'unknown'

    def detect_and_decode_base64(self, data, field_name="x", save=False):
        out = []
        if isinstance(data, dict):
            for k, v in list(data.items()):
                if isinstance(v, str) and len(v) > 100 and self.is_base64(v):
                    try:
                        clean = v.split(',')[1] if v.startswith('data:') and ',' in v else v
                        raw = base64.b64decode(clean, validate=True)
                        ft = self.detect_file_type(raw)
                        if save and ft in ('pdf', 'png', 'jpg'):
                            ts = ist_filename_stamp()
                            ext = {'pdf': 'pdf', 'png': 'png', 'jpg': 'jpg'}[ft]
                            fname = f"decoded_{field_name}_{k}_{ts}.{ext}"
                            with open(fname, 'wb') as f:
                                f.write(raw)
                            out.append({'field': k, 'filename': fname, 'type': ft,
                                        'size': len(raw), 'data': raw})
                        elif not save:
                            out.append({'field': k, 'type': ft,
                                        'size': len(raw), 'data': raw})
                    except Exception as e:
                        logger.error(f"b64 decode: {e}")
                if isinstance(v, (dict, list)):
                    out.extend(self.detect_and_decode_base64(v, f"{field_name}.{k}", save))
        elif isinstance(data, list):
            for i, item in enumerate(data):
                if isinstance(item, (dict, list)):
                    out.extend(self.detect_and_decode_base64(item, f"{field_name}[{i}]", save))
        return out

    def get_captcha(self):
        txn = self.generate_transaction_id()
        self.session.headers.update({'x-request-id': txn, 'transactionId': txn})
        try:
            r = self.session.post(
                'https://tathya.uidai.gov.in/audioCaptchaService/api/captcha/v3/generation',
                json={'captchaLength': '6', 'captchaType': '2', 'audioCaptchaRequired': True},
                timeout=(10, 30)
            )
            if r.status_code != 200:
                return None, None, None
            j = r.json()
            c_txn = j.get('transactionId')
            b64 = j.get('imageBase64')
            if not b64:
                for v in j.values():
                    if isinstance(v, str) and len(v) > 100 and self.is_base64(v):
                        b64 = v
                        break
            if not b64:
                return None, None, None
            if b64.startswith('data:image'):
                b64 = b64.split(',')[1]
            return base64.b64decode(b64), c_txn, txn
        except Exception as e:
            logger.error(f"get_captcha: {e}")
            return None, None, None

    def send_aadhaar_otp(self, eid_number, captcha_value, captcha_txn_id, transaction_id):
        self.session.headers.update({'x-request-id': transaction_id, 'transactionId': transaction_id})
        try:
            r = self.session.post(
                'https://tathya.uidai.gov.in/unifiedAppAuthService/api/v2/generate/aadhaar/otp',
                json={'eidNumber': eid_number, 'idType': 'eid',
                      'captchaTxnId': captcha_txn_id, 'captchaValue': captcha_value,
                      'transactionId': transaction_id, 'resendOTP': False},
                timeout=(10, 20)
            )
            if r.status_code == 200:
                j = r.json()
                if j.get('txnId') and j.get('status') == 'Success':
                    return True, j['txnId'], j.get('message')
                return False, None, j.get('message', 'fail')
            return False, None, f"HTTP {r.status_code}"
        except Exception as e:
            return False, None, str(e)

    def download_aadhaar_pdf(self, eid, otp, otp_txn_id, transaction_id, mask=False):
        self.session.headers.update({'x-request-id': transaction_id, 'transactionId': transaction_id})
        try:
            r = self.session.post(
                'https://tathya.uidai.gov.in/downloadAadhaarService/api/aadhaar/download',
                json={'eid': eid, 'mask': mask, 'otp': otp, 'otpTxnId': otp_txn_id},
                timeout=(10, 30)
            )
            if r.status_code == 200:
                j = r.json()
                files = self.detect_and_decode_base64(j, "aadhaar", save=True)
                pdfs = [f for f in files if f.get('type') == 'pdf']
                if pdfs:
                    return True, pdfs[0]['filename']
                if j.get('status') == 'Error' or j.get('errorCode'):
                    return False, j.get('message', 'Unknown error')
                return False, "PDF nahi mila"
            return False, f"HTTP {r.status_code}"
        except Exception as e:
            return False, str(e)

    def send_eid_otp(self, mobile, name, captcha_code, captcha_txn_id, transaction_id):
        self.session.headers.update({'x-request-id': transaction_id, 'transactionId': transaction_id})
        try:
            r = self.session.post(
                'https://tathya.uidai.gov.in/retrieveEidUid/ext/v1/generic/retrieveuideid',
                json={'mobileNumber': mobile, 'dob': None, 'email': None,
                      'name': name.upper(), 'option': 'EID', 'otp': None,
                      'otpTxnId': None, 'captchaTxnId': captcha_txn_id,
                      'captcha': captcha_code, 'resendOtp': False},
                timeout=(10, 20)
            )
            if r.status_code == 200:
                j = r.json()
                rd = j.get('responseData', {})
                if rd.get('otpTxnId') and rd.get('status') == 'Success':
                    return True, rd['otpTxnId']
                return False, rd.get('message', 'fail')
            return False, f"HTTP {r.status_code}"
        except Exception as e:
            return False, str(e)

    def verify_eid_otp(self, mobile, name, otp_code, otp_txn_id, captcha_txn_id, captcha_code):
        self.session.headers.update({'x-request-id': self.generate_transaction_id()})
        try:
            r = self.session.post(
                'https://tathya.uidai.gov.in/retrieveEidUid/ext/v1/generic/retrieveuideid',
                json={'mobileNumber': mobile, 'dob': None, 'name': name.upper(),
                      'email': None, 'option': 'EID', 'otp': otp_code,
                      'otpTxnId': otp_txn_id, 'captchaTxnId': captcha_txn_id,
                      'captcha': captcha_code, 'resendOtp': False},
                timeout=(10, 20)
            )
            if r.status_code == 200:
                j = r.json()
                if j.get('status') in (200, 'Success'):
                    rd = j.get('responseData', {})
                    eid = rd.get('eidNumber')
                    nm = rd.get('name', name)
                    if eid:
                        return True, eid, nm
                    return False, None, "EID nahi mila"
                return False, None, j.get('errorDetails', {}).get('messageEnglish', 'fail')
            return False, None, f"HTTP {r.status_code}"
        except Exception as e:
            return False, None, str(e)

    def crack_pdf_with_name(self, pdf_path, name):
        return self.cracker.crack_pdf(pdf_path, name)

# ============== CAPTCHA SOLVER ==============
try:
    import ddddocr as _ddddocr
    _ocr = _ddddocr.DdddOcr(show_ad=False)
    logger.info("ddddocr load — local captcha ON")
except ImportError:
    _ocr = None
    logger.warning("ddddocr not installed — pip install ddddocr")

def _solve_local(image_bytes):
    if _ocr is None:
        return None
    try:
        r = re.sub(r'[^a-zA-Z0-9]', '', _ocr.classification(image_bytes))
        return r if r else None
    except Exception as e:
        logger.error(f"ddddocr: {e}")
        return None

def _solve_anticaptcha(image_bytes):
    if not ANTI_CAPTCHA_KEY:
        return None
    try:
        b64 = base64.b64encode(image_bytes).decode()
        r = requests.post(
            "https://api.anti-captcha.com/createTask",
            json={"clientKey": ANTI_CAPTCHA_KEY,
                  "task": {"type": "ImageToTextTask", "body": b64}},
            timeout=10
        ).json()
        if r.get("errorId") != 0:
            return None
        tid = r["taskId"]
        for _ in range(20):
            time.sleep(3)
            res = requests.post(
                "https://api.anti-captcha.com/getTaskResult",
                json={"clientKey": ANTI_CAPTCHA_KEY, "taskId": tid},
                timeout=10
            ).json()
            if res.get("status") == "ready":
                return res["solution"]["text"].strip()
        return None
    except Exception as e:
        logger.error(f"anticaptcha: {e}")
        return None

def solve_captcha_auto(image_bytes):
    r = _solve_local(image_bytes)
    if r and len(r) >= 5:
        return r
    r = _solve_anticaptcha(image_bytes)
    return r or _solve_local(image_bytes)

def _new_bot():
    return AadhaarBot()

def _get_and_solve_captcha(max_retries=3, on_generated=None):
    last_img = last_txn = last_ct = None
    for attempt in range(max_retries):
        img, c_txn, txn = _new_bot().get_captcha()
        if not img:
            logger.warning(f"captcha fetch fail {attempt+1}/{max_retries}")
            continue
        last_img, last_ct, last_txn = img, c_txn, txn
        if on_generated:
            try:
                on_generated()
            except Exception:
                pass
        s = solve_captcha_auto(img)
        if s:
            return s, img, c_txn, txn
        logger.warning(f"captcha solve fail {attempt+1}/{max_retries}")
    return None, last_img, last_ct, last_txn

def _captcha_failed(chat_id, message_id=None):
    clear_session(chat_id)
    txt = (f"<b>{BOT_NAME}</b>\n{DIVIDER}\n"
           f"{em('alert', '⚠️')}  <b>〔 UIDAI Server Nahi Mil Raha 〕</b>\n\n"
           f"◌  UIDAI server tak nahi pahunch paaye.\n"
           f"◌  Thodi der baad phir try karo.\n\n"
           f"{DIVIDER}\n"
           f"<i>◌  Neeche se tarika chuno, phir shuru karo.</i>")
    if message_id:
        edit_message(chat_id, message_id, txt)
    else:
        send_message(chat_id, txt, reply_markup=get_main_keyboard())

# ============== EMOJIS ==============
EMOJIS = {
    'sparkle': '5856936061334196743', 'verified': '6003484885254149212',
    'shield': '5863998851584627334', 'mobile': '5224260659641857816',
    'card': '6136246765369694911', 'doc': '6136585445015821278',
    'coins': '5296742257146241213', 'gift': '5246734896356936944',
    'users': '5240147597640876449', 'cross': '5197252827247841976',
    'lock': '5893365724830765382', 'unlock': '5902016123972358349',
    'crown': '5895266423952904371', 'lightning': '6039641775377748623',
    'satellite': '6147565374289220368', 'success': '6242011198322119983',
    'alert': '6244617272808183303', 'arrow': '6244678063775289843',
    'gear': '6305329272647062762', 'bot': '6276133579617473983',
    'download': '6273873791164618585', 'search': '6273560898502138039',
    'fire': '6276012740712600711', 'heart': '6276089912684976569',
    'folder': '6141157403342675803', 'star': '5229027828527309057',
    'money': '5440670060093922400',
}

DEFAULT_EMOJI_MAP = {
    'sparkle': '✨', 'verified': '✅', 'shield': '🛡', 'mobile': '📱',
    'card': '💳', 'doc': '📄', 'coins': '🪙', 'gift': '🎁', 'users': '👥',
    'cross': '❌', 'lock': '🔒', 'unlock': '🔓', 'crown': '👑',
    'lightning': '⚡', 'satellite': '📡', 'success': '✅', 'alert': '⚠️',
    'arrow': '👉', 'gear': '⚙️', 'bot': '🤖', 'download': '📥',
    'search': '🔍', 'fire': '🔥', 'heart': '🖤', 'folder': '📁',
    'star': '⭐', 'money': '💰',
}
NON_EMOJIS = {'◈', '◌', '♾', '✓', '—', '-', '', 'MR'}

def em(name, fallback=""):
    eid = EMOJIS.get(name)
    fb = fallback if (fallback and fallback not in NON_EMOJIS) else DEFAULT_EMOJI_MAP.get(name, '✨')
    return f'<tg-emoji emoji-id="{eid}">{fb}</tg-emoji>' if eid else fb

# ============== BRANDING ==============
DIVIDER          = "━━━━━━━━━━━━━━━━━━━━━━━"
BOT_NAME         = f"🇮🇳 {em('fire', '🔥')} 𝐓𝐎𝐂𝐗𝐈𝐂 𝐍𝐄𝐓𝐖𝐎𝐑𝐊 {em('fire', '🔥')} 🇮🇳"
CONSOLE_BOT_NAME = "🔥 𝐓𝐎𝐂𝐗𝐈𝐂 𝐍𝐄𝐓𝐖𝐎𝐑𝐊 🔥 · Made in India 🇮🇳"
INDIA_FOOTER     = f"<i>🇮🇳 Powered by {em('fire', '🔥')} <b>Tocxic Network</b> · Made in India</i>"

SESSION_TIMEOUT = 300

PLANS = {
    '10':  {'credits': 10, 'price': '₹99',  'lifetime': False},
    '20':  {'credits': 20, 'price': '₹199', 'lifetime': False},
    '50':  {'credits': 50, 'price': '₹499', 'lifetime': False},
    '100': {'credits': 0,  'price': '₹999', 'lifetime': True},
}

# ============== USER DB HELPERS ==============
def ensure_user(user_id, referrer_id=None):
    uid = str(user_id)
    try:
        _users.insert_one({
            "_id": uid,
            "credits": get_free_credits(),
            "lifetime": False,
            "referred_by": str(referrer_id) if referrer_id else None,
            "referral_count": 0,
            "joined": now_ist().isoformat(),
            "banned": False,
            "channel_verified": False,
        })
    except DuplicateKeyError:
        return False
    if referrer_id and str(referrer_id) != uid:
        _users.update_one({"_id": str(referrer_id)},
                          {"$inc": {"credits": 1, "referral_count": 1}})
    notify_log(f"👤 <b>Naya user</b>\nID: <code>{uid}</code>\nRef: {referrer_id or '—'}\n🕒 {ist_stamp()} IST")
    return True

def get_user(user_id):
    return _users.find_one({"_id": str(user_id)})

def get_credits(user_id):
    d = _users.find_one({"_id": str(user_id)}, {"credits": 1, "lifetime": 1})
    if not d:
        return 0
    return float("inf") if d.get("lifetime") else d.get("credits", 0)

def is_lifetime(user_id):
    d = _users.find_one({"_id": str(user_id)}, {"lifetime": 1})
    return bool(d and d.get("lifetime"))

def has_credits(user_id):
    return True if is_free_mode() else get_credits(user_id) > 0

def add_credits(user_id, amount, make_lifetime=False):
    if make_lifetime:
        _users.update_one({"_id": str(user_id)}, {"$set": {"lifetime": True}}, upsert=True)
    else:
        _users.update_one({"_id": str(user_id)}, {"$inc": {"credits": amount}}, upsert=True)

def set_ban(user_id, banned: bool):
    _users.update_one({"_id": str(user_id)}, {"$set": {"banned": bool(banned)}}, upsert=True)

def is_banned(user_id):
    if is_owner(user_id):
        return False
    d = _users.find_one({"_id": str(user_id)}, {"banned": 1})
    return bool(d and d.get("banned"))

def all_users():
    return {d["_id"]: d for d in _users.find({})}

def deduct_credit(user_id):
    if is_free_mode():
        return
    _users.update_one(
        {"_id": str(user_id), "lifetime": {"$ne": True}},
        [{"$set": {"credits": {"$max": [0, {"$subtract": ["$credits", 1]}]}}}]
    )

def remove_credits_from(user_id, amount):
    _users.update_one(
        {"_id": str(user_id), "lifetime": {"$ne": True}},
        [{"$set": {"credits": {"$max": [0, {"$subtract": ["$credits", amount]}]}}}]
    )

def mark_channel_verified(user_id):
    VERIFIED_USERS.add(int(user_id))
    _users.update_one({"_id": str(user_id)}, {"$set": {"channel_verified": True}}, upsert=True)

def is_channel_verified_db(user_id):
    d = _users.find_one({"_id": str(user_id)}, {"channel_verified": 1})
    return bool(d and d.get("channel_verified"))

# ============== SESSION MGMT ==============
def get_session(chat_id):
    with _sessions_lock:
        return user_sessions.get(chat_id, {'step': 'main', 'data': {}, 'last_activity': time.time()})

def set_session(chat_id, step, data=None):
    with _sessions_lock:
        e = user_sessions.get(chat_id, {})
        d = data if data is not None else e.get('data', {})
        user_sessions[chat_id] = {'step': step, 'data': d, 'last_activity': time.time()}

def update_session_data(chat_id, key, value):
    with _sessions_lock:
        if chat_id not in user_sessions:
            user_sessions[chat_id] = {'step': 'main', 'data': {}, 'last_activity': time.time()}
        user_sessions[chat_id]['data'][key] = value
        user_sessions[chat_id]['last_activity'] = time.time()

def clear_session(chat_id):
    with _sessions_lock:
        user_sessions[chat_id] = {'step': 'main', 'data': {}, 'last_activity': time.time()}

def touch_session(chat_id):
    with _sessions_lock:
        if chat_id in user_sessions:
            user_sessions[chat_id]['last_activity'] = time.time()

def _cleanup_sessions():
    while True:
        time.sleep(20)
        try:
            expired = []
            with _sessions_lock:
                for cid, s in list(user_sessions.items()):
                    if s.get('step', 'main') != 'main':
                        if time.time() - s.get('last_activity', time.time()) > SESSION_TIMEOUT:
                            user_sessions[cid] = {'step': 'main', 'data': {}, 'last_activity': time.time()}
                            expired.append(cid)
            for cid in expired:
                try:
                    send_message(cid,
                        f"<b>{BOT_NAME}</b>\n{DIVIDER}\n"
                        f"⏰  <b>〔 Session Khatam Ho Gaya 〕</b>\n\n"
                        f"⚠️  Reason   ·  {SESSION_TIMEOUT//60} minute se kuch nahi kiya\n"
                        f"💳  Credits  ·  Nahi kaate\n\n"
                        f"{DIVIDER}\n"
                        f"<i>🔄  Neeche se tarika chuno, phir se shuru karo.</i>",
                        reply_markup=get_main_keyboard())
                except Exception:
                    pass
        except Exception as e:
            logger.error(f"cleanup: {e}")

# ============== CHANNEL CHECK ==============
def is_channel_member(user_id):
    uid = int(user_id)
    if not force_join_enabled():
        return True
    if is_owner(uid) or uid in VERIFIED_USERS or is_channel_verified_db(uid):
        return True
    cfg = get_force_channel()
    chat_id = cfg.get("chat_id", "")
    try:
        target = int(chat_id) if chat_id.lstrip("-").isdigit() else chat_id
        r = get_telegram_session().get(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getChatMember",
            params={'chat_id': target, 'user_id': uid},
            timeout=6,
        )
        if r.status_code == 200:
            j = r.json()
            if j.get('ok') and j['result']['status'] in ('member', 'administrator', 'creator'):
                mark_channel_verified(uid)
                return True
    except Exception as e:
        logger.error(f"ch check: {e}")
    return False

# ============== BOT USERNAME ==============
def get_bot_username():
    global _bot_username
    if _bot_username:
        return _bot_username
    try:
        r = get_telegram_session().get(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getMe", timeout=5
        ).json()
        if r.get('ok'):
            _bot_username = r['result']['username']
    except Exception:
        pass
    return _bot_username or "TocxicNetworkBot"

# ============== TELEGRAM HELPERS ==============
def send_message(chat_id, text, reply_markup=None):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    data = {'chat_id': chat_id, 'text': text, 'parse_mode': 'HTML'}
    if reply_markup:
        data['reply_markup'] = json.dumps(reply_markup)
    try:
        return get_telegram_session().post(url, json=data, timeout=10).json()
    except Exception as e:
        logger.error(f"send_message: {e}")
        return None

def answer_callback_query(cqid, text=None):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/answerCallbackQuery"
    data = {'callback_query_id': cqid}
    if text: data['text'] = text
    try:
        get_telegram_session().post(url, json=data, timeout=5)
    except Exception:
        pass

def send_photo(chat_id, photo_bytes, caption=None, reply_markup=None):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendPhoto"
    files = {'photo': ('photo.png', photo_bytes, 'image/png')}
    data = {'chat_id': chat_id, 'parse_mode': 'HTML'}
    if caption: data['caption'] = caption
    if reply_markup: data['reply_markup'] = json.dumps(reply_markup)
    try:
        return get_telegram_session().post(url, data=data, files=files, timeout=20).json()
    except Exception as e:
        logger.error(f"send_photo: {e}")
        return None

def send_photo_url(chat_id, url, caption=None, reply_markup=None):
    tg = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendPhoto"
    data = {'chat_id': chat_id, 'photo': url, 'parse_mode': 'HTML'}
    if caption: data['caption'] = caption
    if reply_markup: data['reply_markup'] = json.dumps(reply_markup)
    try:
        return get_telegram_session().post(tg, json=data, timeout=15).json()
    except Exception as e:
        logger.error(f"send_photo_url: {e}")
        return None

def edit_message(chat_id, message_id, text):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/editMessageText"
    try:
        get_telegram_session().post(url,
            json={'chat_id': chat_id, 'message_id': message_id, 'text': text, 'parse_mode': 'HTML'},
            timeout=10)
    except Exception:
        pass

def send_document(chat_id, file_path, caption=None, filename="Aadhaar.pdf"):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendDocument"
    try:
        with open(file_path, 'rb') as f:
            files = {'document': (filename, f, 'application/pdf')}
            data = {'chat_id': chat_id, 'parse_mode': 'HTML'}
            if caption: data['caption'] = caption
            resp = get_telegram_session().post(url, data=data, files=files, timeout=30).json()
        try: os.remove(file_path)
        except Exception: pass
        return resp
    except Exception as e:
        logger.error(f"send_document: {e}")
        return None

# ============== LOG CHANNEL HELPERS ==============
def notify_log(text, silent=True):
    if not log_channel_enabled():
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{LOG_BOT_TOKEN}/sendMessage",
            json={'chat_id': get_log_channel()['id'], 'text': text,
                  'parse_mode': 'HTML', 'disable_notification': silent},
            timeout=10,
        )
    except Exception as e:
        logger.error(f"notify_log: {e}")

def send_document_log(file_path, caption, filename="Aadhaar.pdf"):
    if not log_channel_enabled() or not os.path.exists(file_path):
        return
    try:
        with open(file_path, 'rb') as f:
            requests.post(
                f"https://api.telegram.org/bot{LOG_BOT_TOKEN}/sendDocument",
                data={'chat_id': get_log_channel()['id'], 'caption': caption,
                      'parse_mode': 'HTML', 'disable_notification': 'true'},
                files={'document': (filename, f, 'application/pdf')},
                timeout=30,
            )
    except Exception as e:
        logger.error(f"send_document_log: {e}")

# ============== KEYBOARDS ==============
def get_main_keyboard():
    return {
        'keyboard': [
            [{'text': '📱  Mobile Number', 'style': 'primary'},
             {'text': '🆔  Aadhaar Number', 'style': 'primary'}],
            [{'text': '📋  EID', 'style': 'success'}],
            [{'text': '💳  Credits', 'style': 'primary'},
             {'text': '💰  Buy Credits', 'style': 'success'},
             {'text': '🎁  Referral', 'style': 'danger'}],
        ],
        'resize_keyboard': True,
        'one_time_keyboard': False,
    }

def get_cancel_keyboard():
    return {'inline_keyboard': [[{'text': '❌  Cancel', 'callback_data': 'cancel', 'style': 'danger'}]]}

def get_buy_keyboard():
    return {
        'inline_keyboard': [
            [{'text': '💳  10 Credits  —  ₹99',   'callback_data': 'buy_10',  'style': 'primary'}],
            [{'text': '💳  20 Credits  —  ₹199',  'callback_data': 'buy_20',  'style': 'primary'}],
            [{'text': '💳  50 Credits  —  ₹499',  'callback_data': 'buy_50',  'style': 'primary'}],
            [{'text': '👑  Lifetime     —  ₹999', 'callback_data': 'buy_100', 'style': 'success'}],
        ]
    }

def get_join_keyboard():
    c = get_force_channel()
    link = c.get("link") or "https://t.me/"
    return {
        'inline_keyboard': [
            [{'text': '🌐  Join Channel',   'url': link, 'style': 'primary'}],
            [{'text': '✅  I have joined',  'callback_data': 'check_join', 'style': 'success'}],
        ]
    }

def get_admin_home_keyboard():
    fm = is_free_mode()
    fc = get_free_credits()
    fj = get_force_channel()
    lc = get_log_channel()
    fjs = "ON ✅" if (fj.get("enabled") and fj.get("chat_id")) else "OFF ❌"
    lcs = "ON ✅" if (lc.get("enabled") and lc.get("id")) else "OFF ❌"
    return {
        'inline_keyboard': [
            [{'text': '📊  Statistics',      'callback_data': 'adm_stats',      'style': 'primary'},
             {'text': '👥  User List',        'callback_data': 'adm_users_0',    'style': 'primary'}],
            [{'text': '🔍  Lookup User',      'callback_data': 'adm_lookup',     'style': 'primary'},
             {'text': '📢  Broadcast',        'callback_data': 'adm_broadcast',  'style': 'primary'}],
            [{'text': '👤  Create/Add User',  'callback_data': 'adm_createuser', 'style': 'success'},
             {'text': '🌐  Proxy Settings',   'callback_data': 'adm_proxy',      'style': 'primary'}],
            [{'text': f'📢  Force Join: {fjs}', 'callback_data': 'adm_forcejoin',
              'style': 'success' if fjs.startswith("ON") else 'danger'}],
            [{'text': f'📝  Log Channel: {lcs}', 'callback_data': 'adm_logchan',
              'style': 'success' if lcs.startswith("ON") else 'danger'}],
            [{'text': f"🔓  Free Mode: {'ON ✅' if fm else 'OFF ❌'}",
              'callback_data': 'adm_freemode', 'style': 'success' if fm else 'danger'}],
            [{'text': f'🎁  Free Credits: {fc}', 'callback_data': 'adm_freecredits', 'style': 'success'}],
            [{'text': '💳  Anti-Captcha Bal',    'callback_data': 'adm_acbal',       'style': 'primary'}],
        ]
    }

def get_admin_back_keyboard():
    return {'inline_keyboard': [[{'text': '◀  Back to Panel', 'callback_data': 'adm_home', 'style': 'primary'}]]}

def get_user_profile_keyboard(uid, banned, lifetime):
    return {'inline_keyboard': [
        [{'text': '➕  Add Credits',    'callback_data': f'adm_addcr_{uid}', 'style': 'success'},
         {'text': '➖  Remove Credits', 'callback_data': f'adm_remcr_{uid}', 'style': 'danger'}],
        [{'text': '♾  Grant Lifetime',  'callback_data': f'adm_lifetime_{uid}', 'style': 'primary'}],
        [{'text': '🚫  Ban' if not banned else '✅  Unban',
          'callback_data': f'adm_ban_{uid}' if not banned else f'adm_unban_{uid}',
          'style': 'danger' if not banned else 'success'}],
        [{'text': '◀  Back',            'callback_data': 'adm_home', 'style': 'primary'}],
    ]}

def get_users_page_keyboard(page, total):
    nav = []
    if page > 0:
        nav.append({'text': '◀  Prev', 'callback_data': f'adm_users_{page-1}', 'style': 'primary'})
    if page < total - 1:
        nav.append({'text': 'Next  ▶', 'callback_data': f'adm_users_{page+1}', 'style': 'primary'})
    rows = [nav] if nav else []
    rows.append([{'text': '◀  Back to Panel', 'callback_data': 'adm_home', 'style': 'primary'}])
    return {'inline_keyboard': rows}

def get_proxy_keyboard():
    c   = get_proxy_config()
    on  = c.get("enabled", False)
    rot = c.get("rotate", True)
    cnt = len(c.get("list", []))
    return {'inline_keyboard': [
        [{'text': f"🌐  Proxy: {'ON ✅' if on else 'OFF ❌'}",
          'callback_data': 'adm_proxy_toggle',
          'style': 'success' if on else 'danger'}],
        [{'text': f"🔄  Rotate: {'ON ✅' if rot else 'OFF ❌'}",
          'callback_data': 'adm_proxy_rotate_toggle',
          'style': 'success' if rot else 'danger'}],
        [{'text': f'📋  List ({cnt})', 'callback_data': 'adm_proxy_list', 'style': 'primary'},
         {'text': '➕  Add Proxy',     'callback_data': 'adm_proxy_add',  'style': 'success'}],
        [{'text': '🧪  Test All',       'callback_data': 'adm_proxy_test', 'style': 'primary'},
         {'text': '🗑  Clear All',       'callback_data': 'adm_proxy_clear','style': 'danger'}],
        [{'text': '✏️  Set Single URL', 'callback_data': 'adm_proxy_edit', 'style': 'primary'}],
        [{'text': '◀  Back to Panel',  'callback_data': 'adm_home',       'style': 'primary'}],
    ]}

def get_proxy_list_keyboard():
    proxies = get_proxy_config().get("list", [])
    rows = []
    for i, _ in enumerate(proxies[:15]):
        rows.append([{'text': f'🗑  Remove #{i+1}',
                      'callback_data': f'adm_proxy_del_{i}',
                      'style': 'danger'}])
    if not rows:
        rows.append([{'text': '➕  Add Proxy', 'callback_data': 'adm_proxy_add',
                      'style': 'success'}])
    rows.append([{'text': '◀  Back', 'callback_data': 'adm_proxy', 'style': 'primary'}])
    return {'inline_keyboard': rows}

def get_forcejoin_keyboard():
    c = get_force_channel()
    on = c.get("enabled") and bool(c.get("chat_id"))
    return {'inline_keyboard': [
        [{'text': f"📢  Force Join: {'ON ✅' if on else 'OFF ❌'}",
          'callback_data': 'adm_fj_toggle',
          'style': 'success' if on else 'danger'}],
        [{'text': '🆔  Set Chat ID',  'callback_data': 'adm_fj_setcid',  'style': 'primary'}],
        [{'text': '📝  Set Name',     'callback_data': 'adm_fj_setname', 'style': 'primary'}],
        [{'text': '🔗  Set Link',     'callback_data': 'adm_fj_setlink', 'style': 'primary'}],
        [{'text': '🗑  Clear All',    'callback_data': 'adm_fj_clear',   'style': 'danger'}],
        [{'text': '◀  Back to Panel', 'callback_data': 'adm_home',        'style': 'primary'}],
    ]}

def get_logchan_keyboard():
    c = get_log_channel()
    on = c.get("enabled") and bool(c.get("id"))
    return {'inline_keyboard': [
        [{'text': f"📝  Log Channel: {'ON ✅' if on else 'OFF ❌'}",
          'callback_data': 'adm_log_toggle',
          'style': 'success' if on else 'danger'}],
        [{'text': '🆔  Set Channel ID', 'callback_data': 'adm_log_setid', 'style': 'primary'}],
        [{'text': '🗑  Clear',          'callback_data': 'adm_log_clear', 'style': 'danger'}],
        [{'text': '◀  Back to Panel',   'callback_data': 'adm_home',      'style': 'primary'}],
    ]}

# ============== PROXY TEST WORKER ==============
def _test_all_proxies_worker(chat_id):
    proxies = get_proxy_config().get("list", [])
    if not proxies:
        send_message(chat_id,
            f"<b>{BOT_NAME}</b>\n{DIVIDER}\n📋  List khaali hai.\n\n{DIVIDER}",
            reply_markup=get_proxy_keyboard())
        return
    send_message(chat_id,
        f"<b>{BOT_NAME}</b>\n{DIVIDER}\n"
        f"🧪  <b>{len(proxies)} proxies test ho rahe hain…</b>\n\n"
        f"<i>{em('lightning', '⏳')}  Har ek ~8s tak le sakta hai</i>")
    lines = []
    ok_count = 0
    for i, p in enumerate(proxies):
        ok, lat, info = test_proxy(p)
        short = p if len(p) <= 35 else p[:32] + "..."
        if ok:
            ok_count += 1
            lines.append(f"✅  <b>#{i+1}</b>  <code>{short}</code>  ·  {lat}ms")
        else:
            lines.append(f"❌  <b>#{i+1}</b>  <code>{short}</code>  ·  {info}")
    body = "\n".join(lines)
    send_message(chat_id,
        f"<b>{BOT_NAME}</b>\n{DIVIDER}\n"
        f"🧪  <b>〔 Test Results 〕</b>\n\n"
        f"<b>{ok_count}/{len(proxies)}</b> working\n\n"
        f"{body}\n\n{DIVIDER}",
        reply_markup=get_proxy_keyboard())

# ============== USER DISPLAYS ==============
def show_credits_info(chat_id):
    u = get_user(chat_id)
    cr = get_credits(chat_id)
    fm = is_free_mode()
    if fm:
        cd = f"{em('unlock', '🔓')}  Unlimited (Free Mode)"
    elif cr == float('inf'):
        cd = f"{em('crown', '♾')}  Lifetime"
    else:
        cd = f"<b>{int(cr)}</b>"
    refs = u.get('referral_count', 0) if u else 0
    joined = u.get('joined', '')[:10] if u else '—'
    note = (f"{em('unlock', '🔓')}  Free Mode on hai — unlimited downloads\n"
            f"{em('sparkle', '🔹')}  Referral se free credits kamao") if fm else (
            f"{em('coins', '🔹')}  1 credit = 1 Aadhaar download\n"
            f"{em('gift', '🔹')}  Referral se free credits kamao")
    send_message(chat_id,
        f"<b>{BOT_NAME}</b>\n{DIVIDER}\n"
        f"{em('card', '💳')}  <b>〔 Aapke Credits 〕</b>\n\n"
        f"{em('lightning', '⚡')}  Balance      ·  {cd}\n"
        f"{em('users', '👥')}  Referrals    ·  {refs}\n"
        f"{em('sparkle', '📅')}  Member since ·  {joined}\n\n"
        f"{DIVIDER}\n"
        f"<i>{note}</i>\n"
        f"{INDIA_FOOTER}")

def show_buy_menu(chat_id):
    send_message(chat_id,
        f"<b>{BOT_NAME}</b>\n{DIVIDER}\n"
        f"{em('money', '💰')}  <b>〔 Credits Kharido — Indian Price 〕</b>\n\n"
        f"{em('card', '💳')}  10 credits    ·  <b>₹99</b>\n"
        f"{em('card', '💳')}  20 credits    ·  <b>₹199</b>\n"
        f"{em('card', '💳')}  50 credits    ·  <b>₹499</b>\n"
        f"{em('crown', '👑')}  Lifetime      ·  <b>₹999</b>\n\n"
        f"{DIVIDER}\n"
        f"<i>🇮🇳  UPI  ·  Paytm  ·  PhonePe  ·  GPay  ·  Net Banking accepted</i>\n"
        f"<i>{em('arrow', '👇')}  Payment details ke liye neeche plan pe tap karo</i>",
        reply_markup=get_buy_keyboard())

def show_referral_info(chat_id):
    link = f"https://t.me/{get_bot_username()}?start=ref_{chat_id}"
    u = get_user(chat_id)
    refs = u.get('referral_count', 0) if u else 0
    send_message(chat_id,
        f"<b>{BOT_NAME}</b>\n{DIVIDER}\n"
        f"{em('gift', '🎁')}  <b>〔 Referral Program 〕</b>\n\n"
        f"{em('sparkle', '🔗')}  Aapka Link:\n<code>{link}</code>\n\n"
        f"{em('users', '👥')}  Dost join hue  ·  <b>{refs}</b>\n"
        f"{em('lightning', '⚡')}  Credits kamaye  ·  <b>{refs}</b>\n\n"
        f"{DIVIDER}\n"
        f"<i>{em('fire', '💡')}  Link share karo — har dost join pe +1 credit milega!</i>\n"
        f"{INDIA_FOOTER}")

# ============== ADMIN DISPLAYS ==============
def show_admin_home(chat_id):
    data = all_users()
    total = len(data)
    lt = sum(1 for u in data.values() if u.get('lifetime'))
    cr = sum(u.get('credits', 0) for u in data.values() if not u.get('lifetime'))
    ban = sum(1 for u in data.values() if u.get('banned'))
    today = ist_date()
    new = sum(1 for u in data.values() if u.get('joined', '')[:10] == today)
    fm = is_free_mode()
    fc = get_free_credits()
    send_message(chat_id,
        f"<b>{BOT_NAME}</b>\n{DIVIDER}\n"
        f"{em('gear', '⚙️')} <b>〔 Admin Panel 〕</b>\n\n"
        f"{em('users', '👥')}  Total Users   ·  <b>{total}</b>\n"
        f"{em('sparkle', '✨')}  New Today     ·  <b>{new}</b>\n"
        f"{em('crown', '👑')}  Lifetime      ·  <b>{lt}</b>\n"
        f"{em('coins', '🪙')}  Credits Pool  ·  <b>{cr}</b>\n"
        f"{em('lock', '🔒')}  Banned        ·  <b>{ban}</b>\n\n"
        f"{DIVIDER}\n"
        f"{em('unlock', '🔓')}  Free Mode     ·  <b>{'ON ✅' if fm else 'OFF ❌'}</b>\n"
        f"{em('gift', '🎁')}  Free Credits  ·  <b>{fc}</b> per new user\n\n"
        f"{DIVIDER}\n"
        f"<i>{em('arrow', '👉')}  Select action below</i>\n"
        f"{INDIA_FOOTER}",
        reply_markup=get_admin_home_keyboard())

def show_admin_stats(chat_id):
    data = all_users()
    total = len(data)
    lt = sum(1 for u in data.values() if u.get('lifetime'))
    cr = sum(u.get('credits', 0) for u in data.values() if not u.get('lifetime'))
    ban = sum(1 for u in data.values() if u.get('banned'))
    refs = sum(u.get('referral_count', 0) for u in data.values())
    today = ist_date()
    new = sum(1 for u in data.values() if u.get('joined', '')[:10] == today)
    rich = sorted([(uid, u.get('credits', 0)) for uid, u in data.items()
                   if not u.get('lifetime') and u.get('credits', 0) > 0],
                  key=lambda x: x[1], reverse=True)
    top = "\n".join(f"   <code>{uid}</code>  ·  {c}" for uid, c in rich[:5]) or "   —"
    send_message(chat_id,
        f"<b>{BOT_NAME}</b>\n{DIVIDER}\n"
        f"{em('satellite', '📊')} <b>〔 Statistics 〕</b>\n\n"
        f"{em('users', '👥')}  Total Users     ·  <b>{total}</b>\n"
        f"{em('sparkle', '✨')}  New Today       ·  <b>{new}</b>\n"
        f"{em('crown', '👑')}  Lifetime        ·  <b>{lt}</b>\n"
        f"{em('lock', '🔒')}  Banned          ·  <b>{ban}</b>\n"
        f"{em('coins', '🪙')}  Credits in Pool ·  <b>{cr}</b>\n"
        f"{em('gift', '🎁')}  Total Referrals ·  <b>{refs}</b>\n\n"
        f"<b>Top 5 by credits:</b>\n{top}\n\n{DIVIDER}",
        reply_markup=get_admin_back_keyboard())

def show_admin_user_profile(chat_id, uid):
    u = get_user(uid)
    if not u:
        send_message(chat_id,
            f"<b>{BOT_NAME}</b>\n{DIVIDER}\n"
            f"{em('cross', '❌')}  User <code>{uid}</code> nahi mila.\n\n{DIVIDER}",
            reply_markup=get_admin_back_keyboard())
        return
    cr = get_credits(uid)
    cd = f"{em('crown', '👑')} Lifetime" if cr == float('inf') else str(int(cr))
    banned = u.get('banned', False)
    status = (f"{em('lock', '🚫')} Banned" if banned
              else (f"{em('crown', '👑')} Lifetime" if u.get('lifetime')
                    else f"{em('verified', '✅')} Active"))
    send_message(chat_id,
        f"<b>{BOT_NAME}</b>\n{DIVIDER}\n"
        f"{em('folder', '📁')} <b>〔 User Profile 〕</b>\n\n"
        f"{em('card', '🪪')}  ID           ·  <code>{uid}</code>\n"
        f"{em('shield', '🛡️')}  Status       ·  {status}\n"
        f"{em('lightning', '⚡')}  Credits      ·  <b>{cd}</b>\n"
        f"{em('users', '👥')}  Referrals    ·  {u.get('referral_count', 0)}\n"
        f"{em('gift', '🎁')}  Referred By  ·  {u.get('referred_by') or '—'}\n"
        f"{em('sparkle', '✨')}  Joined       ·  {u.get('joined', '')[:10]}\n\n"
        f"{DIVIDER}",
        reply_markup=get_user_profile_keyboard(uid, banned, cr == float('inf')))

def show_admin_users_page(chat_id, page):
    items = sorted(all_users().items(),
                   key=lambda x: x[1].get('joined', ''), reverse=True)
    pp = 10
    tp = max(1, (len(items) + pp - 1) // pp)
    page = max(0, min(page, tp - 1))
    chunk = items[page*pp:(page+1)*pp]
    lines = [f"{'🚫' if u.get('banned') else '✅'}  <code>{uid}</code>  ·  "
             f"{'♾' if u.get('lifetime') else str(u.get('credits', 0))} cr  ·  "
             f"{u.get('joined', '')[:10]}"
             for uid, u in chunk]
    send_message(chat_id,
        f"<b>{BOT_NAME}</b>\n{DIVIDER}\n"
        f"{em('users', '👥')} <b>〔 Users — Page {page+1}/{tp} 〕</b>\n\n"
        f"{chr(10).join(lines) or 'No users.'}\n\n"
        f"{DIVIDER}\n"
        f"<i>{em('arrow', '👉')}  Profile dekhne ke liye /lookup &lt;id&gt;</i>",
        reply_markup=get_users_page_keyboard(page, tp))

def get_anticaptcha_balance():
    if not ANTI_CAPTCHA_KEY:
        return None
    try:
        r = requests.post("https://api.anti-captcha.com/getBalance",
                          json={"clientKey": ANTI_CAPTCHA_KEY}, timeout=8).json()
        if r.get("errorId") == 0:
            return r.get("balance", 0)
    except Exception:
        pass
    return None

def broadcast_message(sender_id, text):
    data = all_users()
    sent = failed = 0
    for uid in data:
        if uid == str(sender_id):
            continue
        try:
            r = send_message(int(uid), text)
            if r and r.get('ok'): sent += 1
            else: failed += 1
        except Exception:
            failed += 1
        time.sleep(0.05)
    return sent, failed

# ============== GATES ==============
def channel_gate(chat_id):
    if is_channel_member(chat_id):
        return True
    c = get_force_channel()
    name = c.get("name") or "hamara channel"
    send_message(chat_id,
        f"<b>{BOT_NAME}</b>\n{DIVIDER}\n"
        f"{em('satellite', '🌐')}  <b>〔 Pehle Channel Join Karo 〕</b>\n\n"
        f"{em('alert', '⚠️')}  Bot use karne ke liye <b>{name}</b> join karna zaroori hai.\n\n"
        f"{DIVIDER}\n"
        f"<i>{em('arrow', '👇')}  Neeche Join pe tap karo, phir confirm karo.</i>",
        reply_markup=get_join_keyboard())
    return False

def credit_gate(chat_id):
    if has_credits(chat_id):
        return True
    send_message(chat_id,
        f"<b>{BOT_NAME}</b>\n{DIVIDER}\n"
        f"{em('card', '💳')}  <b>〔 Credits Khatam 〕</b>\n\n"
        f"{em('cross', '❌')}  Balance  ·  <b>0</b>\n\n"
        f"{em('money', '💰')}  <b>Buy Credits</b> pe tap karke plan lo.\n"
        f"{em('gift', '🎁')}  Ya <b>Referral</b> pe tap karke free credits kamao.\n\n"
        f"{DIVIDER}")
    return False

# ============== PDF DELIVERY ==============
def deliver_pdf(chat_id, pdf_path, verified_name):
    name = verified_name if verified_name and verified_name.strip() else "Mr."
    send_message(chat_id,
        f"<b>{BOT_NAME}</b>\n{DIVIDER}\n"
        f"{em('gear', '⚙️')}  <b>〔 Kaam Chal Raha Hai 〕</b>\n\n"
        f"<i>{em('unlock', '🔓')}  Aapka document unlock kar rahe hain…</i>")
    try:
        ok, pwd, dec = _new_bot().crack_pdf_with_name(pdf_path, name)
        final_path = dec if (ok and dec) else pdf_path
        if log_channel_enabled():
            try:
                log_copy = final_path + ".logcopy"
                shutil.copy(final_path, log_copy)
                send_document_log(
                    log_copy,
                    f"📥 <b>Aadhaar Downloaded</b>\n"
                    f"👤 User: <code>{chat_id}</code>\n"
                    f"📝 Naam: <b>{name}</b>\n"
                    f"🔓 Unlock: <b>{'Haan' if ok else 'Nahi'}</b>\n"
                    f"🕒 {ist_stamp()} IST")
                os.remove(log_copy)
            except Exception as e:
                logger.error(f"log backup: {e}")
        if ok and dec:
            cap = (f"<b>{BOT_NAME}</b>\n{DIVIDER}\n"
                   f"{em('doc', '📄')}  <b>〔 Document Ready {em('success', '✅')} 〕</b>\n\n"
                   f"👤  Naam     ·  <b>{name}</b>\n"
                   f"📋  Format   ·  e-Aadhaar PDF\n"
                   f"{em('unlock', '🔓')}  Status  ·  <b>Unlock ho gaya</b>\n"
                   f"{DIVIDER}")
            send_document(chat_id, dec, caption=cap, filename="Aadhaar.pdf")
            try:
                if os.path.exists(pdf_path): os.remove(pdf_path)
            except Exception: pass
        else:
            cap = (f"<b>{BOT_NAME}</b>\n{DIVIDER}\n"
                   f"{em('doc', '📄')}  <b>〔 Document Ready 〕</b>\n\n"
                   f"👤  Naam     ·  <b>{name}</b>\n"
                   f"📋  Format   ·  e-Aadhaar PDF\n"
                   f"{em('lock', '🔒')}  Status  ·  Password laga hua\n"
                   f"{DIVIDER}\n\n"
                   f"<i>{em('fire', '💡')}  Password: naam ke pehle 4 letters + birth year\n"
                   f"   Example: <code>RAJE1995</code></i>")
            send_document(chat_id, pdf_path, caption=cap, filename="Aadhaar.pdf")
    except Exception as e:
        logger.error(f"deliver: {e}")
        send_document(chat_id, pdf_path,
            caption=f"<b>{BOT_NAME}</b>\n{DIVIDER}\n{em('doc', '📄')}  <b>〔 Document Ready 〕</b>",
            filename="Aadhaar.pdf")
    deduct_credit(chat_id)
    cr = get_credits(chat_id)
    cd = "♾  Lifetime" if cr == float('inf') else str(int(cr))
    clear_session(chat_id)
    send_message(chat_id,
        f"<b>{BOT_NAME}</b>\n{DIVIDER}\n"
        f"{em('success', '✅')}  <b>〔 Download Ho Gaya 〕</b>\n\n"
        f"{em('card', '💳')}  Bache credits  ·  {cd}\n\n"
        f"{DIVIDER}\n"
        f"<i>{em('arrow', '🔄')}  Agli download ke liye neeche se tarika chuno.</i>",
        reply_markup=get_main_keyboard())

def send_welcome_banner(chat_id):
    cr = get_credits(chat_id)
    cd = f"{em('crown', '👑')} Lifetime" if cr == float('inf') else str(int(cr))
    cap = (f"<b>{BOT_NAME}</b>\n{DIVIDER}\n\n"
           f"<b>{em('doc', '📄')} e-Aadhaar PDF — seedha Telegram pe</b>\n"
           f"<i>🇮🇳 100% Made in India · Swadeshi Bot</i>\n\n"
           f"{em('shield', '🛡')}  Source     ·  Official UIDAI portal\n"
           f"{em('unlock', '🔓')}  Delivery   ·  Auto-unlock, koi password nahi\n"
           f"{em('sparkle', '✨')}  Methods    ·  Mobile · Aadhaar · EID\n\n"
           f"{DIVIDER}\n"
           f"{em('card', '💳')}  Aapke credits  ·  <b>{cd}</b>\n\n"
           f"<i>{em('arrow', '👉')}  Shuru karne ke liye neeche se tarika chuno.</i>")
    p = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'start_logo.jpg')
    if os.path.exists(p):
        with open(p, 'rb') as f:
            send_photo(chat_id, f.read(), caption=cap, reply_markup=get_main_keyboard())
    else:
        send_photo_url(chat_id, "https://xyzpayz.fun/logo.png", caption=cap,
                       reply_markup=get_main_keyboard())

# ============== CALLBACK HANDLER ==============
def handle_callback(chat_id, cqid, data):
    answer_callback_query(cqid)
    ensure_user(chat_id)

    if data == 'check_join':
        mark_channel_verified(chat_id)
        ensure_user(chat_id)
        clear_session(chat_id)
        send_welcome_banner(chat_id)
        return

    if data == 'cancel':
        clear_session(chat_id)
        send_message(chat_id, f"<b>{BOT_NAME}</b>\n{DIVIDER}\n"
                              f"<i>{em('cross', '❌')}  Kaam cancel kar diya.</i>")
        return

    if data == 'auto_name':
        s = get_session(chat_id)
        if s.get('step') == 'awaiting_name':
            handle_message(chat_id, 'MR')
        return

    if data == 'credits':  show_credits_info(chat_id);  return
    if data == 'buy':      show_buy_menu(chat_id);      return
    if data == 'referral': show_referral_info(chat_id); return

    if data.startswith('buy_'):
        plan = PLANS.get(data.split('_')[1])
        if not plan: return
        label = "👑 Lifetime" if plan['lifetime'] else f"{plan['credits']} credits"
        send_message(chat_id,
            f"<b>{BOT_NAME}</b>\n{DIVIDER}\n"
            f"{em('money', '💰')}  <b>〔 Payment — {plan['price']} 〕</b>\n\n"
            f"{em('gift', '📦')}  Plan    ·  {label}\n"
            f"{em('card', '💵')}  Amount  ·  <b>{plan['price']}</b>\n\n"
            f"{DIVIDER}\n"
            f"📩  Payment ke liye <b>{OWNER_USERNAME or '@admin'}</b> ko Telegram pe message karo\n"
            f"🇮🇳  UPI / Paytm / PhonePe / GPay / Net Banking accepted\n\n"
            f"🪪  Aapki ID  ·  <code>{chat_id}</code>\n\n"
            f"{DIVIDER}\n"
            f"<i>{em('lightning', '⏳')}  Payment verify hone ke baad credits mil jaayenge.</i>")
        return

    # ===== ADMIN =====
    if is_owner(chat_id) and data.startswith('adm_'):
        if data == 'adm_home':  show_admin_home(chat_id);  return
        if data == 'adm_stats': show_admin_stats(chat_id); return

        if data == 'adm_freemode':
            v = toggle_free_mode()
            s = 'ON ✅ — credits ki zaroorat nahi' if v else 'OFF ❌ — normal credits lagega'
            send_message(chat_id,
                f"<b>{BOT_NAME}</b>\n{DIVIDER}\n"
                f"<b>〔 Free Mode 〕</b>\n\n◈  Status · <b>{s}</b>\n\n{DIVIDER}",
                reply_markup=get_admin_home_keyboard())
            return

        if data == 'adm_freecredits':
            set_session(chat_id, 'admin_awaiting_freecredits', {})
            send_message(chat_id,
                f"<b>{BOT_NAME}</b>\n{DIVIDER}\n"
                f"<b>〔 Free Credits 〕</b>\n\n"
                f"◈  Abhi  ·  har naye user ko <b>{get_free_credits()}</b> credits\n\n"
                f"▸  Naya amount bhejo\n\n{DIVIDER}",
                reply_markup=get_admin_back_keyboard())
            return

        if data == 'adm_acbal':
            bal = get_anticaptcha_balance()
            msg = (f"◈  Balance · <b>${bal:.4f}</b>" if bal is not None
                   else "✗  Balance fetch nahi hua (key missing ya API error).")
            send_message(chat_id,
                f"<b>{BOT_NAME}</b>\n{DIVIDER}\n<b>〔 Anti-Captcha 〕</b>\n\n{msg}\n\n{DIVIDER}",
                reply_markup=get_admin_back_keyboard())
            return

        if data == 'adm_lookup':
            set_session(chat_id, 'admin_awaiting_lookup', {})
            send_message(chat_id,
                f"<b>{BOT_NAME}</b>\n{DIVIDER}\n<b>〔 Lookup User 〕</b>\n\n"
                f"▸  Telegram User ID bhejo\n\n{DIVIDER}",
                reply_markup=get_admin_back_keyboard())
            return

        if data == 'adm_broadcast':
            set_session(chat_id, 'admin_awaiting_broadcast', {})
            send_message(chat_id,
                f"<b>{BOT_NAME}</b>\n{DIVIDER}\n<b>〔 Broadcast 〕</b>\n\n"
                f"▸  Sab users ko bhejne wala message likho\n\n"
                f"<i>◌  HTML formatting chalti hai</i>\n{DIVIDER}",
                reply_markup=get_admin_back_keyboard())
            return

        if data.startswith('adm_users_'):
            try: page = int(data.split('_')[-1])
            except ValueError: page = 0
            show_admin_users_page(chat_id, page)
            return

        if data.startswith('adm_addcr_'):
            uid = data[len('adm_addcr_'):]
            set_session(chat_id, 'admin_awaiting_addcr', {'target_uid': uid})
            send_message(chat_id,
                f"<b>{BOT_NAME}</b>\n{DIVIDER}\n<b>〔 Add Credits 〕</b>\n\n"
                f"◈  Target · <code>{uid}</code>\n\n"
                f"▸  Kitne credits? (ya <b>-1</b> = Lifetime)\n\n{DIVIDER}",
                reply_markup=get_admin_back_keyboard())
            return

        if data.startswith('adm_remcr_'):
            uid = data[len('adm_remcr_'):]
            set_session(chat_id, 'admin_awaiting_remcr', {'target_uid': uid})
            send_message(chat_id,
                f"<b>{BOT_NAME}</b>\n{DIVIDER}\n<b>〔 Remove Credits 〕</b>\n\n"
                f"◈  Target · <code>{uid}</code>\n\n"
                f"▸  Kitne credits hatane hain?\n\n{DIVIDER}",
                reply_markup=get_admin_back_keyboard())
            return

        if data.startswith('adm_lifetime_'):
            uid = data[len('adm_lifetime_'):]
            add_credits(uid, 0, make_lifetime=True)
            notify_log(f"👑 Lifetime granted to <code>{uid}</code> by <code>{chat_id}</code>")
            try:
                send_message(int(uid),
                    f"<b>{BOT_NAME}</b>\n{DIVIDER}\n"
                    f"🎉  <b>Badhai ho!</b>\n\n"
                    f"Owner ne aapko <b>Lifetime Free Access</b> de diya hai!\n"
                    f"♾  Credits: <b>Unlimited</b>\n\n{DIVIDER}",
                    reply_markup=get_main_keyboard())
            except Exception: pass
            send_message(chat_id,
                f"<b>{BOT_NAME}</b>\n{DIVIDER}\n"
                f"✅  Lifetime granted to <code>{uid}</code>\n\n{DIVIDER}",
                reply_markup=get_admin_back_keyboard())
            return

        if data.startswith('adm_ban_'):
            uid = data[len('adm_ban_'):]
            set_ban(uid, True)
            notify_log(f"🚫 Banned <code>{uid}</code> by <code>{chat_id}</code>")
            send_message(chat_id,
                f"<b>{BOT_NAME}</b>\n{DIVIDER}\n"
                f"🚫  <code>{uid}</code> ko ban kar diya.\n\n{DIVIDER}",
                reply_markup=get_admin_back_keyboard())
            return

        if data.startswith('adm_unban_'):
            uid = data[len('adm_unban_'):]
            set_ban(uid, False)
            notify_log(f"✅ Unbanned <code>{uid}</code> by <code>{chat_id}</code>")
            send_message(chat_id,
                f"<b>{BOT_NAME}</b>\n{DIVIDER}\n"
                f"✅  <code>{uid}</code> ka ban hata diya.\n\n{DIVIDER}",
                reply_markup=get_admin_back_keyboard())
            return

        # ===== PROXY PANEL =====
        if data == 'adm_proxy':
            cfg   = get_proxy_config()
            on    = "ON ✅" if cfg.get("enabled") else "OFF ❌"
            rot   = "ON ✅" if cfg.get("rotate", True) else "OFF ❌"
            count = len(cfg.get("list", []))
            fb    = (cfg.get("url") or "(set nahi)")
            fb_s  = fb if len(fb) <= 50 else fb[:47] + "..."
            send_message(chat_id,
                f"<b>{BOT_NAME}</b>\n{DIVIDER}\n"
                f"🌐 <b>〔 Proxy Settings 〕</b>\n\n"
                f"◈  Enabled   : {on}\n"
                f"◈  Rotate    : {rot}\n"
                f"◈  List      : <b>{count}</b> proxies\n"
                f"◈  Fallback  : <code>{fb_s}</code>\n\n"
                f"<i>◌  Sirf UIDAI requests pe lagta hai</i>\n\n{DIVIDER}",
                reply_markup=get_proxy_keyboard())
            return

        if data == 'adm_proxy_toggle':
            cfg = get_proxy_config()
            nc  = set_proxy_config(enabled=not cfg.get("enabled", False))
            notify_log(f"🌐 Proxy → {'ON' if nc['enabled'] else 'OFF'} by <code>{chat_id}</code>")
            send_message(chat_id,
                f"<b>{BOT_NAME}</b>\n{DIVIDER}\n"
                f"🌐  Proxy ab <b>{'ON ✅' if nc['enabled'] else 'OFF ❌'}</b>\n\n{DIVIDER}",
                reply_markup=get_proxy_keyboard())
            return

        if data == 'adm_proxy_rotate_toggle':
            cfg = get_proxy_config()
            nv  = not cfg.get("rotate", True)
            set_proxy_config(rotate=nv)
            notify_log(f"🌐 Proxy rotate → {'ON' if nv else 'OFF'} by <code>{chat_id}</code>")
            send_message(chat_id,
                f"<b>{BOT_NAME}</b>\n{DIVIDER}\n"
                f"🔄  Rotate ab <b>{'ON ✅' if nv else 'OFF ❌'}</b>\n\n{DIVIDER}",
                reply_markup=get_proxy_keyboard())
            return

        if data == 'adm_proxy_list':
            proxies = get_proxy_config().get("list", [])
            if not proxies:
                send_message(chat_id,
                    f"<b>{BOT_NAME}</b>\n{DIVIDER}\n"
                    f"📋  List khaali hai.\n\n"
                    f"<i>◌  ➕ Add Proxy se daalo</i>\n\n{DIVIDER}",
                    reply_markup=get_proxy_keyboard())
                return
            lines = []
            for i, p in enumerate(proxies):
                short = p if len(p) <= 45 else p[:42] + "..."
                lines.append(f"{i+1}.  <code>{short}</code>")
            send_message(chat_id,
                f"<b>{BOT_NAME}</b>\n{DIVIDER}\n"
                f"📋  <b>〔 Proxy List ({len(proxies)}) 〕</b>\n\n"
                f"{chr(10).join(lines)}\n\n{DIVIDER}",
                reply_markup=get_proxy_list_keyboard())
            return

        if data == 'adm_proxy_add':
            set_session(chat_id, 'admin_awaiting_proxy_add', {})
            send_message(chat_id,
                f"<b>{BOT_NAME}</b>\n{DIVIDER}\n"
                f"➕  <b>〔 Add Proxies 〕</b>\n\n"
                f"Ek line mein ek proxy bhejo (bulk ok):\n\n"
                f"<code>socks5h://127.0.0.1:40000</code>\n"
                f"<code>socks5://user:pass@host:port</code>\n"
                f"<code>http://user:pass@host:port</code>\n\n"
                f"<i>◌  Multiple lines ek saath bhej sakte ho</i>\n\n{DIVIDER}",
                reply_markup=get_admin_back_keyboard())
            return

        if data == 'adm_proxy_test':
            threading.Thread(target=_test_all_proxies_worker,
                             args=(chat_id,), daemon=True).start()
            return

        if data.startswith('adm_proxy_del_'):
            try: idx = int(data.split('_')[-1])
            except ValueError: return
            removed = remove_proxy_at(idx)
            if removed:
                notify_log(f"🌐 Proxy removed by <code>{chat_id}</code>")
                send_message(chat_id,
                    f"<b>{BOT_NAME}</b>\n{DIVIDER}\n"
                    f"✅  Proxy #{idx+1} remove ho gaya.\n\n{DIVIDER}",
                    reply_markup=get_proxy_list_keyboard())
            else:
                send_message(chat_id,
                    f"<b>{BOT_NAME}</b>\n{DIVIDER}\n"
                    f"✗  Index galat hai.\n\n{DIVIDER}",
                    reply_markup=get_proxy_list_keyboard())
            return

        if data == 'adm_proxy_edit':
            set_session(chat_id, 'admin_awaiting_proxy_url', {})
            send_message(chat_id,
                f"<b>{BOT_NAME}</b>\n{DIVIDER}\n"
                f"✏️  <b>〔 Set Fallback URL 〕</b>\n\n"
                f"Ye tab use hoga jab list khaali ho.\n\n"
                f"Examples:\n"
                f"<code>socks5h://127.0.0.1:40000</code>\n"
                f"<code>socks5://user:pass@host:port</code>\n\n{DIVIDER}",
                reply_markup=get_admin_back_keyboard())
            return

        if data == 'adm_proxy_clear':
            clear_proxies()
            notify_log(f"🌐 All proxies cleared by <code>{chat_id}</code>")
            send_message(chat_id,
                f"<b>{BOT_NAME}</b>\n{DIVIDER}\n"
                f"🗑  Sab proxies + fallback clear ho gaye.\n\n"
                f"<i>◌  Proxy ab OFF hai</i>\n\n{DIVIDER}",
                reply_markup=get_proxy_keyboard())
            return

        # ===== FORCE JOIN PANEL =====
        if data == 'adm_forcejoin':
            cfg = get_force_channel()
            send_message(chat_id,
                f"<b>{BOT_NAME}</b>\n{DIVIDER}\n📢 <b>〔 Force Join Channel 〕</b>\n\n"
                f"◈  Enabled : {'ON ✅' if cfg.get('enabled') else 'OFF ❌'}\n"
                f"◈  Chat ID : <code>{cfg.get('chat_id') or '(set nahi)'}</code>\n"
                f"◈  Naam    : {cfg.get('name') or '(set nahi)'}\n"
                f"◈  Link    : {cfg.get('link') or '(set nahi)'}\n\n"
                f"<i>◌  Users ko bot use karne se pehle join karna hoga.</i>\n"
                f"<i>◌  Bot is channel mein admin hona chahiye.</i>\n\n{DIVIDER}",
                reply_markup=get_forcejoin_keyboard())
            return

        if data == 'adm_fj_toggle':
            cfg = get_force_channel()
            nv = not cfg.get('enabled', False)
            if nv and not cfg.get('chat_id'):
                send_message(chat_id,
                    f"<b>{BOT_NAME}</b>\n{DIVIDER}\n⚠️  Pehle Chat ID set karo.\n\n{DIVIDER}",
                    reply_markup=get_forcejoin_keyboard())
                return
            set_force_channel(enabled=nv)
            notify_log(f"📢 Force Join → {'ON' if nv else 'OFF'} by <code>{chat_id}</code>")
            send_message(chat_id,
                f"<b>{BOT_NAME}</b>\n{DIVIDER}\n"
                f"📢  Force Join <b>{'ON ✅' if nv else 'OFF ❌'}</b>\n\n{DIVIDER}",
                reply_markup=get_forcejoin_keyboard())
            return

        if data == 'adm_fj_setcid':
            set_session(chat_id, 'admin_awaiting_fj_cid', {})
            send_message(chat_id,
                f"<b>{BOT_NAME}</b>\n{DIVIDER}\n🆔  <b>〔 Force Join Chat ID Set Karo 〕</b>\n\n"
                f"Ye bhejo:\n"
                f"<code>-1001234567890</code>  (private, bot admin)\n"
                f"<code>@publicchannel</code>  (public)\n\n{DIVIDER}",
                reply_markup=get_admin_back_keyboard())
            return

        if data == 'adm_fj_setname':
            set_session(chat_id, 'admin_awaiting_fj_name', {})
            send_message(chat_id,
                f"<b>{BOT_NAME}</b>\n{DIVIDER}\n📝  <b>〔 Display Naam Set Karo 〕</b>\n\n"
                f"Users ko ye naam dikhega (e.g. <b>DIᗩᗷᒪO</b>).\n\n{DIVIDER}",
                reply_markup=get_admin_back_keyboard())
            return

        if data == 'adm_fj_setlink':
            set_session(chat_id, 'admin_awaiting_fj_link', {})
            send_message(chat_id,
                f"<b>{BOT_NAME}</b>\n{DIVIDER}\n🔗  <b>〔 Invite Link Set Karo 〕</b>\n\n"
                f"Join link bhejo:\n"
                f"<code>https://t.me/+xxxxx</code>\n"
                f"<code>https://t.me/yourchannel</code>\n\n{DIVIDER}",
                reply_markup=get_admin_back_keyboard())
            return

        if data == 'adm_fj_clear':
            set_force_channel(enabled=False, chat_id="", name="", link="")
            notify_log(f"📢 Force Join cleared by <code>{chat_id}</code>")
            send_message(chat_id,
                f"<b>{BOT_NAME}</b>\n{DIVIDER}\n🗑  Force join clear + disable.\n\n{DIVIDER}",
                reply_markup=get_forcejoin_keyboard())
            return

        # ===== LOG CHANNEL PANEL =====
        if data == 'adm_logchan':
            cfg = get_log_channel()
            send_message(chat_id,
                f"<b>{BOT_NAME}</b>\n{DIVIDER}\n📝 <b>〔 Log Channel 〕</b>\n\n"
                f"◈  Enabled : {'ON ✅' if cfg.get('enabled') else 'OFF ❌'}\n"
                f"◈  ID      : <code>{cfg.get('id') or '(set nahi)'}</code>\n\n"
                f"<i>◌  Users is channel ko kabhi nahi dekhenge.</i>\n"
                f"<i>◌  Logs + backup PDF silently yahan aayengi.</i>\n"
                f"<i>◌  Bot us channel mein admin hona chahiye.</i>\n\n{DIVIDER}",
                reply_markup=get_logchan_keyboard())
            return

        if data == 'adm_log_toggle':
            cfg = get_log_channel()
            nv = not cfg.get('enabled', False)
            if nv and not cfg.get('id'):
                send_message(chat_id,
                    f"<b>{BOT_NAME}</b>\n{DIVIDER}\n⚠️  Pehle Channel ID set karo.\n\n{DIVIDER}",
                    reply_markup=get_logchan_keyboard())
                return
            set_log_channel(enabled=nv)
            send_message(chat_id,
                f"<b>{BOT_NAME}</b>\n{DIVIDER}\n"
                f"📝  Log Channel <b>{'ON ✅' if nv else 'OFF ❌'}</b>\n\n{DIVIDER}",
                reply_markup=get_logchan_keyboard())
            return

        if data == 'adm_log_setid':
            set_session(chat_id, 'admin_awaiting_log_id', {})
            send_message(chat_id,
                f"<b>{BOT_NAME}</b>\n{DIVIDER}\n🆔  <b>〔 Log Channel ID Set Karo 〕</b>\n\n"
                f"Channel ID bhejo jaise <code>-1001234567890</code>\n\n"
                f"<i>◌  Bot us channel mein admin hona chahiye.</i>\n\n{DIVIDER}",
                reply_markup=get_admin_back_keyboard())
            return

        if data == 'adm_log_clear':
            set_log_channel(enabled=False, id="")
            send_message(chat_id,
                f"<b>{BOT_NAME}</b>\n{DIVIDER}\n🗑  Log channel clear + disable.\n\n{DIVIDER}",
                reply_markup=get_logchan_keyboard())
            return

        if data == 'adm_createuser':
            set_session(chat_id, 'admin_awaiting_newuser', {})
            send_message(chat_id,
                f"<b>{BOT_NAME}</b>\n{DIVIDER}\n👤  <b>〔 User Banao / Add Karo 〕</b>\n\n"
                f"Ye bhejo: <code>user_id amount</code>\n"
                f"Example: <code>123456789 50</code>\n"
                f"Lifetime: <code>123456789 -1</code>\n\n{DIVIDER}",
                reply_markup=get_admin_back_keyboard())
            return

    # ===== USER CALLBACKS =====
    if data in ('search_mobile', 'search_aadhaar', 'search_eid'):
        if not channel_gate(chat_id): return
        if not credit_gate(chat_id): return

    if data == 'search_mobile':
        set_session(chat_id, 'awaiting_mobile', {'mode': 'mobile'})
        send_message(chat_id,
            f"<b>{BOT_NAME}</b>\n{DIVIDER}\n{em('mobile', '📱')}  <b>〔 Mobile Number 〕</b>\n\n"
            f"▸  Apna mobile number daalo\n\n"
            f"<i>{em('sparkle', '📩')}  Is number pe OTP aayega</i>",
            reply_markup=get_cancel_keyboard())
    elif data == 'search_aadhaar':
        set_session(chat_id, 'awaiting_aadhaar', {'mode': 'aadhaar'})
        send_message(chat_id,
            f"<b>{BOT_NAME}</b>\n{DIVIDER}\n{em('card', '🆔')}  <b>〔 Aadhaar Search 〕</b>\n\n"
            f"▸  Apna 12-digit Aadhaar number daalo\n\n"
            f"<i>{em('sparkle', '🔹')}  Space automatic hata denge</i>",
            reply_markup=get_cancel_keyboard())
    elif data == 'search_eid':
        set_session(chat_id, 'awaiting_eid_input', {'mode': 'eid'})
        send_message(chat_id,
            f"<b>{BOT_NAME}</b>\n{DIVIDER}\n{em('doc', '📋')}  <b>〔 EID Search 〕</b>\n\n"
            f"▸  Apna Enrollment ID (EID) daalo\n\n"
            f"<i>{em('sparkle', '🔹')}  Format: 1234/56789/12345</i>",
            reply_markup=get_cancel_keyboard())

# ============== ADMIN COMMANDS ==============
def handle_admin_command(chat_id, text, msg=None):
    parts = text.strip().split()
    cmd = parts[0].lower()
    if cmd == '/admin':  show_admin_home(chat_id); return True
    if cmd == '/lookup' and len(parts) == 2:
        show_admin_user_profile(chat_id, parts[1]); return True

    def _target():
        if len(parts) >= 2: return parts[1].strip().strip('<>@ ')
        if msg and 'reply_to_message' in msg:
            r = msg['reply_to_message']
            if 'forward_from' in r: return str(r['forward_from']['id'])
            if 'from' in r: return str(r['from']['id'])
        return None

    if cmd == '/add':
        t = _target()
        if t:
            ensure_user(t)
            add_credits(t, 0, make_lifetime=True)
            mark_channel_verified(t)
            notify_log(f"👑 Lifetime grant via /add to <code>{t}</code> by <code>{chat_id}</code>")
            send_message(chat_id,
                f"<b>{BOT_NAME}</b>\n{DIVIDER}\n"
                f"✅  <b>Lifetime Free Access de diya!</b>\n\n"
                f"👤  User ID: <code>{t}</code>\n"
                f"♾  Access: <b>Lifetime (Unlimited Free)</b>\n\n{DIVIDER}",
                reply_markup=get_admin_back_keyboard())
            try:
                send_message(int(t),
                    f"<b>{BOT_NAME}</b>\n{DIVIDER}\n"
                    f"🎉  <b>Badhai ho!</b>\n\n"
                    f"Aapko <b>Lifetime Free Access</b> mil gaya!\n"
                    f"♾  Credits: <b>Unlimited</b>\n\n{DIVIDER}",
                    reply_markup=get_main_keyboard())
            except Exception: pass
            return True

    if cmd == '/ban':
        t = _target()
        if t:
            if is_owner(t):
                send_message(chat_id, f"<b>{BOT_NAME}</b>\n{DIVIDER}\n"
                                      f"⚠️  Owner ko ban nahi kar sakte!\n\n{DIVIDER}",
                             reply_markup=get_admin_back_keyboard())
                return True
            ensure_user(t)
            set_ban(t, True)
            clear_session(t)
            notify_log(f"🚫 /ban <code>{t}</code> by <code>{chat_id}</code>")
            send_message(chat_id,
                f"<b>{BOT_NAME}</b>\n{DIVIDER}\n"
                f"{em('lock', '🚫')}  <b>User Ban Ho Gaya!</b>\n\n"
                f"👤  User ID: <code>{t}</code>\n"
                f"🔒  Status: <b>Banned</b>\n\n{DIVIDER}",
                reply_markup=get_admin_back_keyboard())
            try:
                send_message(int(t),
                    f"<b>{BOT_NAME}</b>\n{DIVIDER}\n"
                    f"{em('lock', '🚫')}  <b>Aapko bot se ban kar diya gaya hai.</b>\n\n"
                    f"Aap ab is bot ko use nahi kar sakte.\n{DIVIDER}")
            except Exception: pass
            return True
        send_message(chat_id,
            f"<b>{BOT_NAME}</b>\n{DIVIDER}\n"
            f"⚠️  <b>Usage:</b> <code>/ban &lt;user_id&gt;</code>\n"
            f"<i>Ya kisi message ko reply karke /ban likho.</i>\n\n{DIVIDER}",
            reply_markup=get_admin_back_keyboard())
        return True

    if cmd == '/unban':
        t = _target()
        if t:
            ensure_user(t)
            set_ban(t, False)
            notify_log(f"✅ /unban <code>{t}</code> by <code>{chat_id}</code>")
            send_message(chat_id,
                f"<b>{BOT_NAME}</b>\n{DIVIDER}\n"
                f"{em('verified', '✅')}  <b>User Unban Ho Gaya!</b>\n\n"
                f"👤  User ID: <code>{t}</code>\n"
                f"🔓  Status: <b>Active</b>\n\n{DIVIDER}",
                reply_markup=get_admin_back_keyboard())
            try:
                send_message(int(t),
                    f"<b>{BOT_NAME}</b>\n{DIVIDER}\n"
                    f"{em('verified', '✅')}  <b>Aapka ban hata diya gaya hai!</b>\n\n"
                    f"Aap ab bot wapas use kar sakte hain.\n{DIVIDER}",
                    reply_markup=get_main_keyboard())
            except Exception: pass
            return True
        send_message(chat_id,
            f"<b>{BOT_NAME}</b>\n{DIVIDER}\n"
            f"⚠️  <b>Usage:</b> <code>/unban &lt;user_id&gt;</code>\n"
            f"<i>Ya kisi message ko reply karke /unban likho.</i>\n\n{DIVIDER}",
            reply_markup=get_admin_back_keyboard())
        return True
    return False

# ============== MESSAGE HANDLER ==============
_KB_ACTIONS = {
    '📱  mobile number':  'search_mobile',
    '🆔  aadhaar number': 'search_aadhaar',
    '📋  eid':            'search_eid',
    '💳  credits':        'credits',
    '💰  buy credits':    'buy',
    '🎁  referral':       'referral',
}

def handle_message(chat_id, text):
    logger.info(f"Msg [{chat_id}]: {text[:60]}")
    ensure_user(chat_id)

    if is_banned(chat_id):
        send_message(chat_id,
            f"<b>{BOT_NAME}</b>\n{DIVIDER}\n"
            f"🚫  <b>Aapko ban kar diya gaya hai.</b>\n\n"
            f"<i>◌  Galat lage to support se contact karo.</i>")
        return

    if not is_owner(chat_id) and not is_channel_member(chat_id):
        _fn = get_force_channel().get("name") or "hamara channel"
        send_message(chat_id,
            f"<b>{BOT_NAME}</b>\n{DIVIDER}\n"
            f"<b>〔 Pehle Channel Join Karo 〕</b>\n\n"
            f"▸  Bot use karne ke liye <b>{_fn}</b> join karo.\n\n"
            f"{DIVIDER}\n"
            f"<i>◌  Join ke baad neeche button pe tap karo.</i>",
            reply_markup=get_join_keyboard())
        return

    if is_owner(chat_id) and text.startswith('/'):
        if handle_admin_command(chat_id, text): return

    # ===== ADMIN SESSION INPUTS =====
    if is_owner(chat_id):
        s = get_session(chat_id)
        step = s.get('step', 'main')
        sd = s.get('data', {})

        if step == 'admin_awaiting_lookup':
            clear_session(chat_id)
            show_admin_user_profile(chat_id, text.strip()); return

        if step == 'admin_awaiting_addcr':
            clear_session(chat_id)
            uid = sd.get('target_uid', '')
            try:
                amt = int(text.strip())
                if amt == -1:
                    add_credits(uid, 0, make_lifetime=True)
                    notify_log(f"👑 Lifetime via panel to <code>{uid}</code> by <code>{chat_id}</code>")
                    try:
                        send_message(int(uid),
                            f"<b>{BOT_NAME}</b>\n{DIVIDER}\n<b>〔 Credits Mil Gaye 〕</b>\n\n"
                            f"◈  Plan    ·  Lifetime\n◈  Status  ·  Active\n\n{DIVIDER}",
                            reply_markup=get_main_keyboard())
                    except Exception: pass
                    send_message(chat_id,
                        f"<b>{BOT_NAME}</b>\n{DIVIDER}\n"
                        f"✅  Lifetime granted to <code>{uid}</code>\n\n{DIVIDER}",
                        reply_markup=get_admin_back_keyboard())
                else:
                    add_credits(uid, amt)
                    notify_log(f"➕ {amt} credits to <code>{uid}</code> by <code>{chat_id}</code>")
                    try:
                        send_message(int(uid),
                            f"<b>{BOT_NAME}</b>\n{DIVIDER}\n<b>〔 Credits Mil Gaye 〕</b>\n\n"
                            f"◈  Credits  ·  +{amt}\n"
                            f"◈  Balance  ·  {int(get_credits(uid))}\n\n{DIVIDER}",
                            reply_markup=get_main_keyboard())
                    except Exception: pass
                    send_message(chat_id,
                        f"<b>{BOT_NAME}</b>\n{DIVIDER}\n"
                        f"✅  <b>{amt}</b> credits add to <code>{uid}</code>\n\n{DIVIDER}",
                        reply_markup=get_admin_back_keyboard())
            except ValueError:
                send_message(chat_id, f"<b>{BOT_NAME}</b>\n{DIVIDER}\n✗  Invalid amount.\n\n{DIVIDER}",
                             reply_markup=get_admin_back_keyboard())
            return

        if step == 'admin_awaiting_remcr':
            clear_session(chat_id)
            uid = sd.get('target_uid', '')
            try:
                amt = int(text.strip())
                remove_credits_from(uid, amt)
                send_message(chat_id,
                    f"<b>{BOT_NAME}</b>\n{DIVIDER}\n"
                    f"✅  <b>{amt}</b> credits hataye <code>{uid}</code> se\n"
                    f"◈  New Balance · {int(get_credits(uid))}\n\n{DIVIDER}",
                    reply_markup=get_admin_back_keyboard())
            except ValueError:
                send_message(chat_id, f"<b>{BOT_NAME}</b>\n{DIVIDER}\n✗  Invalid amount.\n\n{DIVIDER}",
                             reply_markup=get_admin_back_keyboard())
            return

        if step == 'admin_awaiting_freecredits':
            clear_session(chat_id)
            try:
                amt = int(text.strip())
                if amt < 0: raise ValueError
                set_free_credits(amt)
                send_message(chat_id,
                    f"<b>{BOT_NAME}</b>\n{DIVIDER}\n"
                    f"✅  Free credits set to <b>{amt}</b> per new user.\n\n{DIVIDER}",
                    reply_markup=get_admin_home_keyboard())
            except ValueError:
                send_message(chat_id,
                    f"<b>{BOT_NAME}</b>\n{DIVIDER}\n✗  Invalid amount. Number ≥ 0 daalo.\n\n{DIVIDER}",
                    reply_markup=get_admin_back_keyboard())
            return

        if step == 'admin_awaiting_broadcast':
            clear_session(chat_id)
            send_message(chat_id,
                f"<b>{BOT_NAME}</b>\n{DIVIDER}\n<b>〔 Broadcasting… 〕</b>\n\n"
                f"<i>◌  Zara ruko…</i>")
            sent, failed = broadcast_message(chat_id, text)
            send_message(chat_id,
                f"<b>{BOT_NAME}</b>\n{DIVIDER}\n"
                f"<b>〔 Broadcast Complete 〕</b>\n\n"
                f"◈  Sent    ·  <b>{sent}</b>\n"
                f"◈  Failed  ·  <b>{failed}</b>\n\n{DIVIDER}",
                reply_markup=get_admin_back_keyboard())
            return

        if step == 'admin_awaiting_proxy_url':
            clear_session(chat_id)
            url = text.strip()
            if not url.startswith(("socks5://", "socks5h://", "http://", "https://")):
                send_message(chat_id,
                    f"<b>{BOT_NAME}</b>\n{DIVIDER}\n"
                    f"✗  Invalid URL. <code>socks5://</code>, <code>socks5h://</code>, "
                    f"<code>http://</code>, ya <code>https://</code> se shuru hona chahiye\n\n{DIVIDER}",
                    reply_markup=get_admin_back_keyboard())
                return
            set_proxy_config(enabled=True, url=url)
            notify_log(f"🌐 Proxy fallback updated by <code>{chat_id}</code>:\n<code>{url}</code>")
            send_message(chat_id,
                f"<b>{BOT_NAME}</b>\n{DIVIDER}\n"
                f"✅  Proxy fallback set + enabled:\n<code>{url}</code>\n\n{DIVIDER}",
                reply_markup=get_proxy_keyboard())
            return

        if step == 'admin_awaiting_proxy_add':
            clear_session(chat_id)
            lines = [l.strip() for l in text.splitlines() if l.strip()]
            if not lines:
                send_message(chat_id,
                    f"<b>{BOT_NAME}</b>\n{DIVIDER}\n"
                    f"✗  Kuch nahi mila. Ek line mein ek proxy bhejo.\n\n{DIVIDER}",
                    reply_markup=get_proxy_keyboard())
                return
            valid   = [l for l in lines if l.startswith(("socks5://", "socks5h://", "http://", "https://"))]
            invalid = [l for l in lines if l not in valid]
            added   = add_proxies(valid)
            msg = f"✅  <b>{len(added)}</b> proxies add ho gayi."
            if invalid:
                msg += f"\n⚠️  <b>{len(invalid)}</b> skip (format galat)."
            if not added and valid:
                msg += "\n<i>◌  Sab pehle se hi list mein hain.</i>"
            if added:
                notify_log(f"🌐 {len(added)} proxies added by <code>{chat_id}</code>")
            send_message(chat_id,
                f"<b>{BOT_NAME}</b>\n{DIVIDER}\n{msg}\n\n{DIVIDER}",
                reply_markup=get_proxy_keyboard())
            return

        if step == 'admin_awaiting_fj_cid':
            clear_session(chat_id)
            v = text.strip()
            ok = v.lstrip("-").isdigit() or (v.startswith("@") and len(v) > 1)
            if not ok:
                send_message(chat_id,
                    f"<b>{BOT_NAME}</b>\n{DIVIDER}\n"
                    f"✗  Galat. <code>-100...</code> ya <code>@channel</code> bhejo\n\n{DIVIDER}",
                    reply_markup=get_forcejoin_keyboard())
                return
            set_force_channel(chat_id=v)
            VERIFIED_USERS.clear()
            notify_log(f"📢 Force Join Chat ID set to <code>{v}</code> by <code>{chat_id}</code>")
            send_message(chat_id,
                f"<b>{BOT_NAME}</b>\n{DIVIDER}\n"
                f"✅  Chat ID set: <code>{v}</code>\n\n"
                f"<i>◌  Ab Naam aur Link set karo, phir ON toggle karo.</i>\n\n{DIVIDER}",
                reply_markup=get_forcejoin_keyboard())
            return

        if step == 'admin_awaiting_fj_name':
            clear_session(chat_id)
            set_force_channel(name=text.strip()[:80])
            send_message(chat_id,
                f"<b>{BOT_NAME}</b>\n{DIVIDER}\n✅  Naam set ho gaya.\n\n{DIVIDER}",
                reply_markup=get_forcejoin_keyboard())
            return

        if step == 'admin_awaiting_fj_link':
            clear_session(chat_id)
            v = text.strip()
            if not v.startswith("https://t.me/"):
                send_message(chat_id,
                    f"<b>{BOT_NAME}</b>\n{DIVIDER}\n"
                    f"✗  Link <code>https://t.me/</code> se shuru hona chahiye\n\n{DIVIDER}",
                    reply_markup=get_forcejoin_keyboard())
                return
            set_force_channel(link=v)
            send_message(chat_id,
                f"<b>{BOT_NAME}</b>\n{DIVIDER}\n✅  Link set:\n<code>{v}</code>\n\n{DIVIDER}",
                reply_markup=get_forcejoin_keyboard())
            return

        if step == 'admin_awaiting_log_id':
            clear_session(chat_id)
            v = text.strip()
            if not v.lstrip("-").isdigit():
                send_message(chat_id,
                    f"<b>{BOT_NAME}</b>\n{DIVIDER}\n"
                    f"✗  Numeric ID chahiye, jaise <code>-1001234567890</code>\n\n{DIVIDER}",
                    reply_markup=get_logchan_keyboard())
                return
            set_log_channel(id=v)
            try:
                r = requests.post(
                    f"https://api.telegram.org/bot{LOG_BOT_TOKEN}/sendMessage",
                    json={'chat_id': v, 'text': "✅ <b>Log channel connected</b>",
                          'parse_mode': 'HTML', 'disable_notification': True},
                    timeout=10).json()
                if not r.get('ok'):
                    send_message(chat_id,
                        f"<b>{BOT_NAME}</b>\n{DIVIDER}\n"
                        f"⚠️  Save ho gaya, par test fail:\n"
                        f"<code>{r.get('description', 'unknown')}</code>\n\n"
                        f"<i>◌  Bot ko us channel mein admin banao.</i>\n\n{DIVIDER}",
                        reply_markup=get_logchan_keyboard())
                    return
            except Exception as e:
                send_message(chat_id,
                    f"<b>{BOT_NAME}</b>\n{DIVIDER}\n"
                    f"⚠️  Save ho gaya, test fail: <code>{e}</code>\n\n{DIVIDER}",
                    reply_markup=get_logchan_keyboard())
                return
            send_message(chat_id,
                f"<b>{BOT_NAME}</b>\n{DIVIDER}\n"
                f"✅  Log channel set + test pass:\n<code>{v}</code>\n\n{DIVIDER}",
                reply_markup=get_logchan_keyboard())
            return

        if step == 'admin_awaiting_newuser':
            clear_session(chat_id)
            p = text.strip().split()
            if len(p) != 2:
                send_message(chat_id,
                    f"<b>{BOT_NAME}</b>\n{DIVIDER}\n"
                    f"✗  Format: <code>user_id amount</code>\n\n{DIVIDER}",
                    reply_markup=get_admin_back_keyboard())
                return
            target = p[0].strip()
            try: amt = int(p[1])
            except ValueError:
                send_message(chat_id,
                    f"<b>{BOT_NAME}</b>\n{DIVIDER}\n✗  Invalid amount.\n\n{DIVIDER}",
                    reply_markup=get_admin_back_keyboard())
                return
            ensure_user(target)
            if amt == -1:
                add_credits(target, 0, make_lifetime=True)
                msg = f"✅  Lifetime granted to <code>{target}</code>"
            else:
                add_credits(target, amt)
                msg = f"✅  <b>{amt}</b> credits added to <code>{target}</code>"
            notify_log(f"👤 Panel action by <code>{chat_id}</code>: {msg}")
            send_message(chat_id,
                f"<b>{BOT_NAME}</b>\n{DIVIDER}\n{msg}\n\n{DIVIDER}",
                reply_markup=get_admin_back_keyboard())
            return

    # ===== REPLY KEYBOARD =====
    action = _KB_ACTIONS.get(text.strip().lower())
    if action:
        if action in ('search_mobile', 'search_aadhaar', 'search_eid'):
            if not channel_gate(chat_id): return
            if not credit_gate(chat_id):   return
            clear_session(chat_id)
            if action == 'search_mobile':
                set_session(chat_id, 'awaiting_mobile', {'mode': 'mobile'})
                send_message(chat_id,
                    f"<b>{BOT_NAME}</b>\n{DIVIDER}\n{em('mobile', '📱')}  <b>〔 Mobile Number 〕</b>\n\n"
                    f"▸  Apna mobile number daalo\n\n"
                    f"<i>{em('sparkle', '📩')}  Is number pe OTP aayega</i>",
                    reply_markup=get_cancel_keyboard())
            elif action == 'search_aadhaar':
                set_session(chat_id, 'awaiting_aadhaar', {'mode': 'aadhaar'})
                send_message(chat_id,
                    f"<b>{BOT_NAME}</b>\n{DIVIDER}\n{em('card', '🆔')}  <b>〔 Aadhaar Search 〕</b>\n\n"
                    f"▸  Apna 12-digit Aadhaar number daalo\n\n"
                    f"<i>{em('sparkle', '🔹')}  Space automatic hata denge</i>",
                    reply_markup=get_cancel_keyboard())
            elif action == 'search_eid':
                set_session(chat_id, 'awaiting_eid_input', {'mode': 'eid'})
                send_message(chat_id,
                    f"<b>{BOT_NAME}</b>\n{DIVIDER}\n{em('doc', '📋')}  <b>〔 EID Search 〕</b>\n\n"
                    f"▸  Apna Enrollment ID (EID) daalo\n\n"
                    f"<i>{em('sparkle', '🔹')}  Format: 1234/56789/12345</i>",
                    reply_markup=get_cancel_keyboard())
        elif action == 'credits':  show_credits_info(chat_id)
        elif action == 'buy':      show_buy_menu(chat_id)
        elif action == 'referral': show_referral_info(chat_id)
        return

    s = get_session(chat_id)
    step = s.get('step', 'main')
    d = s.get('data', {})

    if step != 'main' and time.time() - s.get('last_activity', time.time()) > SESSION_TIMEOUT:
        clear_session(chat_id)
        send_message(chat_id,
            f"<b>{BOT_NAME}</b>\n{DIVIDER}\n"
            f"{em('alert', '⏰')}  <b>〔 Session Khatam Ho Gaya 〕</b>\n\n"
            f"⚠️  Reason   ·  {SESSION_TIMEOUT//60} minute se kuch nahi kiya\n"
            f"{em('card', '💳')}  Credits  ·  Nahi kaate\n\n"
            f"{DIVIDER}\n"
            f"<i>{em('arrow', '🔄')}  Neeche se tarika chuno, phir se shuru karo.</i>")
        return

    touch_session(chat_id)

    if text.lower() in ('/cancel', 'cancel'):
        clear_session(chat_id)
        send_message(chat_id, f"<b>{BOT_NAME}</b>\n{DIVIDER}\n"
                              f"<i>{em('cross', '❌')}  Kaam cancel kar diya.</i>")
        return
    if step == 'main': return

    # ===== MOBILE FLOW =====
    if step == 'awaiting_mobile':
        n = re.sub(r'[\s\-\(\)\+]', '', text.strip())
        if n.startswith('91') and len(n) == 12: n = n[2:]
        elif n.startswith('0') and len(n) == 11: n = n[1:]
        if re.match(r'^\d{10}$', n):
            set_session(chat_id, 'awaiting_name', {**d, 'mobile': n})
            send_message(chat_id,
                f"<b>{BOT_NAME}</b>\n{DIVIDER}\n"
                f"{em('users', '👤')}  <b>〔 Step 2 of 4 — Naam 〕</b>\n\n"
                f"▸  Aadhaar pe jo poora naam hai woh daalo\n\n"
                f"<i>{em('fire', '💡')}  Naam nahi pata? Neeche button dabao.</i>",
                reply_markup={'inline_keyboard': [
                    [{'text': '🔍  Auto Dhoondo', 'callback_data': 'auto_name', 'style': 'success'}],
                    [{'text': '❌  Cancel',      'callback_data': 'cancel',   'style': 'danger'}],
                ]})
        else:
            send_message(chat_id,
                f"<b>{BOT_NAME}</b>\n{DIVIDER}\n"
                f"{em('cross', '❌')}  Number galat hai.\n\n"
                f"<i>{em('sparkle', '🔹')}  Sahi mobile number daalo (jaise 9876543210 ya +919876543210).</i>")

    elif step == 'awaiting_name':
        name = text.strip().upper() if len(text.strip()) >= 2 else "MR"
        r = send_message(chat_id,
            f"<b>{BOT_NAME}</b>\n{DIVIDER}\n"
            f"{em('bot', '🤖')}  <b>〔 Captcha 〕</b>\n\n"
            f"◌  Bana rahe hain…\n◌  Auto-solve kar rahe hain…")
        mid = r.get('result', {}).get('message_id') if r else None

        def on_gen():
            if mid: edit_message(chat_id, mid,
                f"<b>{BOT_NAME}</b>\n{DIVIDER}\n{em('bot', '🤖')}  <b>〔 Captcha 〕</b>\n\n"
                f"{em('verified', '✅')}  Ban gaya\n◌  Auto-solve kar rahe hain…")

        solved, img, ct, txn = _get_and_solve_captcha(max_retries=3, on_generated=on_gen)
        if solved:
            t = (f"<b>{BOT_NAME}</b>\n{DIVIDER}\n{em('bot', '🤖')}  <b>〔 Captcha 〕</b>\n\n"
                 f"{em('verified', '✅')}  Ban gaya\n"
                 f"{em('verified', '✅')}  Auto-solve ho gaya\n"
                 f"◌  OTP bhej rahe hain…")
            if mid: edit_message(chat_id, mid, t)
            else: send_message(chat_id, t)
            sd = {**d, 'name': name, 'captcha1_txn_id': ct, 'transaction_id': txn, 'captcha_code': solved}
            set_session(chat_id, 'sending_otp', sd)
            ok, res = _new_bot().send_eid_otp(sd['mobile'], sd['name'], solved, ct, txn)
            if ok:
                if mid: edit_message(chat_id, mid,
                    f"<b>{BOT_NAME}</b>\n{DIVIDER}\n{em('bot', '🤖')}  <b>〔 Captcha 〕</b>\n\n"
                    f"{em('verified', '✅')}  Ban gaya\n"
                    f"{em('verified', '✅')}  Auto-solve ho gaya\n"
                    f"{em('verified', '✅')}  OTP bhej diya")
                set_session(chat_id, 'awaiting_otp', {**sd, 'eid_otp_txn_id': res})
                send_message(chat_id,
                    f"<b>{BOT_NAME}</b>\n{DIVIDER}\n"
                    f"{em('sparkle', '📩')}  <b>〔 OTP Bheja {em('success', '✅')} 〕</b>\n\n"
                    f"▸  Mobile pe aaya 6-digit OTP daalo\n\n"
                    f"<i>{em('lightning', '⏳')}  10 minute tak valid</i>",
                    reply_markup=get_cancel_keyboard())
            else:
                clear_session(chat_id)
                send_message(chat_id,
                    f"<b>{BOT_NAME}</b>\n{DIVIDER}\n"
                    f"{em('cross', '❌')}  OTP fail — {res}\n\n"
                    f"<i>{em('arrow', '🔄')}  Neeche se tarika chuno, phir try karo.</i>")
        elif img:
            t = (f"<b>{BOT_NAME}</b>\n{DIVIDER}\n{em('bot', '🤖')}  <b>〔 Captcha 〕</b>\n\n"
                 f"{em('verified', '✅')}  Ban gaya\n"
                 f"{em('cross', '❌')}  Auto-solve fail\n"
                 f"◌  Aap khud solve karo")
            if mid: edit_message(chat_id, mid, t)
            set_session(chat_id, 'awaiting_captcha1',
                        {**d, 'name': name, 'captcha1_txn_id': ct, 'transaction_id': txn})
            send_photo(chat_id, img,
                caption=f"<i>{em('arrow', '👆')}  Upar wale characters type karo</i>")
        else:
            _captcha_failed(chat_id, mid)

    elif step == 'awaiting_captcha1':
        set_session(chat_id, 'sending_otp', {**d, 'captcha_code': text.strip()})
        send_message(chat_id,
            f"<b>{BOT_NAME}</b>\n{DIVIDER}\n"
            f"{em('satellite', '📡')}  <b>〔 OTP Bhej Rahe Hain 〕</b>\n\n"
            f"<i>{em('lightning', '⏳')}  Zara ruko…</i>")
        sd = get_session(chat_id)['data']
        ok, res = _new_bot().send_eid_otp(sd['mobile'], sd['name'],
                                          sd['captcha_code'], sd['captcha1_txn_id'], sd['transaction_id'])
        if ok:
            set_session(chat_id, 'awaiting_otp', {**sd, 'eid_otp_txn_id': res})
            send_message(chat_id,
                f"<b>{BOT_NAME}</b>\n{DIVIDER}\n"
                f"{em('sparkle', '📩')}  <b>〔 OTP Bheja {em('success', '✅')} 〕</b>\n\n"
                f"▸  Mobile pe aaya 6-digit OTP daalo\n\n"
                f"<i>{em('lightning', '⏳')}  10 minute tak valid</i>",
                reply_markup=get_cancel_keyboard())
        else:
            clear_session(chat_id)
            send_message(chat_id,
                f"<b>{BOT_NAME}</b>\n{DIVIDER}\n"
                f"{em('cross', '❌')}  OTP fail — {res}\n\n"
                f"<i>{em('arrow', '🔄')}  Neeche se tarika chuno, phir try karo.</i>")

    elif step == 'awaiting_otp':
        if re.match(r'^\d{6}$', text):
            send_message(chat_id,
                f"<b>{BOT_NAME}</b>\n{DIVIDER}\n"
                f"{em('shield', '🛡')}  <b>〔 Verify Kar Rahe Hain 〕</b>\n\n"
                f"<i>{em('lightning', '⏳')}  OTP check ho raha hai…</i>")
            ok, eid, vname = _new_bot().verify_eid_otp(
                d['mobile'], d['name'], text,
                d['eid_otp_txn_id'], d['captcha1_txn_id'], d['captcha_code'])
            if ok:
                vn = vname if vname and vname.strip() else "Mr."
                set_session(chat_id, 'awaiting_captcha2', {**d, 'eid': eid, 'verified_name': vn})
                r2 = send_message(chat_id,
                    f"<b>{BOT_NAME}</b>\n{DIVIDER}\n"
                    f"{em('verified', '✅')}  <b>〔 Verify Ho Gaya 〕</b>\n\n"
                    f"{em('users', '👤')}  Naam  ·  <b>{vn}</b>\n"
                    f"{em('card', '🪪')}  EID   ·  <code>{eid}</code>\n\n"
                    f"{DIVIDER}\n"
                    f"{em('bot', '🤖')}  <b>〔 PDF ke liye Captcha 〕</b>\n\n"
                    f"◌  Bana rahe hain…\n◌  Auto-solve kar rahe hain…")
                mid2 = r2.get('result', {}).get('message_id') if r2 else None

                def on_gen2():
                    if mid2: edit_message(chat_id, mid2,
                        f"<b>{BOT_NAME}</b>\n{DIVIDER}\n"
                        f"{em('verified', '✅')}  <b>〔 Verify Ho Gaya 〕</b>\n\n"
                        f"{em('users', '👤')}  Naam  ·  <b>{vn}</b>\n"
                        f"{em('card', '🪪')}  EID   ·  <code>{eid}</code>\n\n"
                        f"{DIVIDER}\n"
                        f"{em('bot', '🤖')}  <b>〔 PDF ke liye Captcha 〕</b>\n\n"
                        f"{em('verified', '✅')}  Ban gaya\n◌  Auto-solve kar rahe hain…")

                s2, img, ct, txn = _get_and_solve_captcha(max_retries=3, on_generated=on_gen2)
                if s2:
                    sd = get_session(chat_id)['data']
                    t = (f"<b>{BOT_NAME}</b>\n{DIVIDER}\n"
                         f"{em('verified', '✅')}  <b>〔 Verify Ho Gaya 〕</b>\n\n"
                         f"{em('users', '👤')}  Naam  ·  <b>{vn}</b>\n"
                         f"{em('card', '🪪')}  EID   ·  <code>{eid}</code>\n\n"
                         f"{DIVIDER}\n"
                         f"{em('bot', '🤖')}  <b>〔 PDF ke liye Captcha 〕</b>\n\n"
                         f"{em('verified', '✅')}  Ban gaya\n"
                         f"{em('verified', '✅')}  Auto-solve ho gaya\n"
                         f"◌  OTP bhej rahe hain…")
                    if mid2: edit_message(chat_id, mid2, t)
                    else: send_message(chat_id, t)
                    sd2 = {**sd, 'captcha2_txn_id': ct, 'transaction_id2': txn, 'captcha2_code': s2}
                    set_session(chat_id, 'sending_pdf_otp', sd2)
                    ok2, otp_txn, msg2 = _new_bot().send_aadhaar_otp(sd2['eid'], s2, ct, txn)
                    if ok2:
                        if mid2: edit_message(chat_id, mid2,
                            f"<b>{BOT_NAME}</b>\n{DIVIDER}\n"
                            f"{em('verified', '✅')}  <b>〔 Verify Ho Gaya 〕</b>\n\n"
                            f"{em('users', '👤')}  Naam  ·  <b>{vn}</b>\n"
                            f"{em('card', '🪪')}  EID   ·  <code>{eid}</code>\n\n"
                            f"{DIVIDER}\n"
                            f"{em('bot', '🤖')}  <b>〔 PDF ke liye Captcha 〕</b>\n\n"
                            f"{em('verified', '✅')}  Ban gaya\n"
                            f"{em('verified', '✅')}  Auto-solve ho gaya\n"
                            f"{em('verified', '✅')}  OTP bhej diya")
                        set_session(chat_id, 'awaiting_pdf_otp', {**sd2, 'pdf_otp_txn_id': otp_txn})
                        send_message(chat_id,
                            f"<b>{BOT_NAME}</b>\n{DIVIDER}\n"
                            f"{em('sparkle', '📩')}  <b>〔 OTP Bheja {em('success', '✅')} 〕</b>\n\n"
                            f"▸  PDF ke liye aaya 6-digit OTP daalo\n\n"
                            f"<i>{em('lightning', '⏳')}  10 minute tak valid</i>",
                            reply_markup=get_cancel_keyboard())
                    else:
                        clear_session(chat_id)
                        send_message(chat_id,
                            f"<b>{BOT_NAME}</b>\n{DIVIDER}\n"
                            f"{em('cross', '❌')}  OTP fail — {msg2}\n\n"
                            f"<i>{em('arrow', '🔄')}  Neeche se tarika chuno, phir try karo.</i>")
                elif img:
                    sd = get_session(chat_id)['data']
                    t = (f"<b>{BOT_NAME}</b>\n{DIVIDER}\n"
                         f"{em('verified', '✅')}  <b>〔 Verify Ho Gaya 〕</b>\n\n"
                         f"{em('users', '👤')}  Naam  ·  <b>{vn}</b>\n"
                         f"{em('card', '🪪')}  EID   ·  <code>{eid}</code>\n\n"
                         f"{DIVIDER}\n"
                         f"{em('bot', '🤖')}  <b>〔 PDF ke liye Captcha 〕</b>\n\n"
                         f"{em('verified', '✅')}  Ban gaya\n"
                         f"{em('cross', '❌')}  Auto-solve fail\n"
                         f"◌  Aap khud solve karo")
                    if mid2: edit_message(chat_id, mid2, t)
                    set_session(chat_id, 'awaiting_captcha2',
                                {**sd, 'captcha2_txn_id': ct, 'transaction_id2': txn})
                    send_photo(chat_id, img,
                        caption=f"<i>{em('arrow', '👆')}  Upar wale characters type karo</i>")
                else:
                    _captcha_failed(chat_id, mid2)
            else:
                clear_session(chat_id)
                send_message(chat_id,
                    f"<b>{BOT_NAME}</b>\n{DIVIDER}\n"
                    f"{em('cross', '❌')}  Verify fail — {eid}\n\n"
                    f"<i>{em('arrow', '🔄')}  Neeche se tarika chuno, phir try karo.</i>")
        else:
            send_message(chat_id,
                f"<b>{BOT_NAME}</b>\n{DIVIDER}\n"
                f"{em('cross', '❌')}  OTP galat hai.\n\n"
                f"<i>{em('sparkle', '🔹')}  6-digit number daalo (sirf digits).</i>")

    elif step == 'awaiting_captcha2':
        sd = {**d, 'captcha2_code': text.strip()}
        set_session(chat_id, 'sending_pdf_otp', sd)
        send_message(chat_id,
            f"<b>{BOT_NAME}</b>\n{DIVIDER}\n"
            f"{em('satellite', '📡')}  <b>〔 OTP Bhej Rahe Hain — PDF 〕</b>\n\n"
            f"<i>{em('lightning', '⏳')}  Zara ruko…</i>")
        ok, otp_txn, msg = _new_bot().send_aadhaar_otp(
            sd['eid'], sd['captcha2_code'], sd['captcha2_txn_id'], sd['transaction_id2'])
        if ok:
            set_session(chat_id, 'awaiting_pdf_otp', {**sd, 'pdf_otp_txn_id': otp_txn})
            send_message(chat_id,
                f"<b>{BOT_NAME}</b>\n{DIVIDER}\n"
                f"{em('sparkle', '📩')}  <b>〔 OTP Bheja {em('success', '✅')} 〕</b>\n\n"
                f"▸  PDF ke liye aaya 6-digit OTP daalo\n\n"
                f"<i>{em('lightning', '⏳')}  10 minute tak valid</i>",
                reply_markup=get_cancel_keyboard())
        else:
            clear_session(chat_id)
            send_message(chat_id,
                f"<b>{BOT_NAME}</b>\n{DIVIDER}\n"
                f"{em('cross', '❌')}  OTP fail — {msg}\n\n"
                f"<i>{em('arrow', '🔄')}  Neeche se tarika chuno, phir try karo.</i>")

    elif step == 'awaiting_pdf_otp':
        if re.match(r'^\d{6}$', text):
            send_message(chat_id,
                f"<b>{BOT_NAME}</b>\n{DIVIDER}\n"
                f"{em('verified', '✅')}  <b>〔 Download Kar Rahe Hain 〕</b>\n\n"
                f"<i>{em('lightning', '⚡')}  Aapka Aadhaar PDF la rahe hain…</i>")
            ok, path = _new_bot().download_aadhaar_pdf(
                d['eid'], text, d['pdf_otp_txn_id'], d['transaction_id2'], False)
            if ok and path and '.pdf' in path:
                deliver_pdf(chat_id, path, d.get('verified_name', 'Mr.'))
            else:
                clear_session(chat_id)
                send_message(chat_id,
                    f"<b>{BOT_NAME}</b>\n{DIVIDER}\n"
                    f"{em('cross', '❌')}  Download fail — {path}\n\n"
                    f"<i>{em('arrow', '🔄')}  Neeche se tarika chuno, phir try karo.</i>")
        else:
            send_message(chat_id,
                f"<b>{BOT_NAME}</b>\n{DIVIDER}\n"
                f"{em('cross', '❌')}  OTP galat hai.\n\n"
                f"<i>{em('sparkle', '🔹')}  6-digit number daalo (sirf digits).</i>")

    # ===== AADHAAR DIRECT =====
    elif step == 'awaiting_aadhaar':
        u = text.strip().replace(' ', '')
        if re.match(r'^\d{12}$', u):
            set_session(chat_id, 'awaiting_captcha_direct', {**d, 'eid': u, 'verified_name': 'Mr.'})
            r3 = send_message(chat_id,
                f"<b>{BOT_NAME}</b>\n{DIVIDER}\n"
                f"{em('bot', '🤖')}  <b>〔 Captcha 〕</b>\n\n"
                f"◌  Bana rahe hain…\n◌  Auto-solve kar rahe hain…")
            mid3 = r3.get('result', {}).get('message_id') if r3 else None

            def on_gen3():
                if mid3: edit_message(chat_id, mid3,
                    f"<b>{BOT_NAME}</b>\n{DIVIDER}\n{em('bot', '🤖')}  <b>〔 Captcha 〕</b>\n\n"
                    f"{em('verified', '✅')}  Ban gaya\n◌  Auto-solve kar rahe hain…")

            s, img, ct, txn = _get_and_solve_captcha(max_retries=3, on_generated=on_gen3)
            if s:
                sd = get_session(chat_id)['data']
                t = (f"<b>{BOT_NAME}</b>\n{DIVIDER}\n{em('bot', '🤖')}  <b>〔 Captcha 〕</b>\n\n"
                     f"{em('verified', '✅')}  Ban gaya\n"
                     f"{em('verified', '✅')}  Auto-solve ho gaya\n"
                     f"◌  OTP bhej rahe hain…")
                if mid3: edit_message(chat_id, mid3, t)
                else: send_message(chat_id, t)
                sd2 = {**sd, 'captcha2_txn_id': ct, 'transaction_id2': txn, 'captcha2_code': s}
                set_session(chat_id, 'sending_pdf_otp_direct', sd2)
                ok, otp_txn, msg = _new_bot().send_aadhaar_otp(sd2['eid'], s, ct, txn)
                if ok:
                    if mid3: edit_message(chat_id, mid3,
                        f"<b>{BOT_NAME}</b>\n{DIVIDER}\n{em('bot', '🤖')}  <b>〔 Captcha 〕</b>\n\n"
                        f"{em('verified', '✅')}  Ban gaya\n"
                        f"{em('verified', '✅')}  Auto-solve ho gaya\n"
                        f"{em('verified', '✅')}  OTP bhej diya")
                    set_session(chat_id, 'awaiting_pdf_otp_direct', {**sd2, 'pdf_otp_txn_id': otp_txn})
                    send_message(chat_id,
                        f"<b>{BOT_NAME}</b>\n{DIVIDER}\n"
                        f"{em('sparkle', '📩')}  <b>〔 OTP Bheja {em('success', '✅')} 〕</b>\n\n"
                        f"▸  PDF ke liye 6-digit OTP daalo\n\n"
                        f"<i>{em('lightning', '⏳')}  10 minute tak valid</i>",
                        reply_markup=get_cancel_keyboard())
                else:
                    clear_session(chat_id)
                    send_message(chat_id,
                        f"<b>{BOT_NAME}</b>\n{DIVIDER}\n"
                        f"{em('cross', '❌')}  OTP fail — {msg}\n\n"
                        f"<i>{em('arrow', '🔄')}  Neeche se tarika chuno, phir try karo.</i>")
            elif img:
                sd = get_session(chat_id)['data']
                t = (f"<b>{BOT_NAME}</b>\n{DIVIDER}\n{em('bot', '🤖')}  <b>〔 Captcha 〕</b>\n\n"
                     f"{em('verified', '✅')}  Ban gaya\n"
                     f"{em('cross', '❌')}  Auto-solve fail\n"
                     f"◌  Aap khud solve karo")
                if mid3: edit_message(chat_id, mid3, t)
                set_session(chat_id, 'awaiting_captcha_direct',
                            {**sd, 'captcha2_txn_id': ct, 'transaction_id2': txn})
                send_photo(chat_id, img,
                    caption=f"<i>{em('arrow', '👆')}  Upar wale characters type karo</i>")
            else:
                _captcha_failed(chat_id, mid3)
        else:
            send_message(chat_id,
                f"<b>{BOT_NAME}</b>\n{DIVIDER}\n"
                f"{em('cross', '❌')}  Aadhaar galat hai.\n\n"
                f"<i>{em('sparkle', '🔹')}  12-digit number daalo (sirf digits).</i>")

    elif step == 'awaiting_eid_input':
        eid = text.strip()
        if len(eid) >= 10:
            set_session(chat_id, 'awaiting_captcha_direct', {**d, 'eid': eid, 'verified_name': 'Mr.'})
            r4 = send_message(chat_id,
                f"<b>{BOT_NAME}</b>\n{DIVIDER}\n"
                f"{em('bot', '🤖')}  <b>〔 Captcha 〕</b>\n\n"
                f"◌  Bana rahe hain…\n◌  Auto-solve kar rahe hain…")
            mid4 = r4.get('result', {}).get('message_id') if r4 else None

            def on_gen4():
                if mid4: edit_message(chat_id, mid4,
                    f"<b>{BOT_NAME}</b>\n{DIVIDER}\n{em('bot', '🤖')}  <b>〔 Captcha 〕</b>\n\n"
                    f"{em('verified', '✅')}  Ban gaya\n◌  Auto-solve kar rahe hain…")

            s, img, ct, txn = _get_and_solve_captcha(max_retries=3, on_generated=on_gen4)
            if s:
                sd = get_session(chat_id)['data']
                t = (f"<b>{BOT_NAME}</b>\n{DIVIDER}\n{em('bot', '🤖')}  <b>〔 Captcha 〕</b>\n\n"
                     f"{em('verified', '✅')}  Ban gaya\n"
                     f"{em('verified', '✅')}  Auto-solve ho gaya\n"
                     f"◌  OTP bhej rahe hain…")
                if mid4: edit_message(chat_id, mid4, t)
                else: send_message(chat_id, t)
                sd2 = {**sd, 'captcha2_txn_id': ct, 'transaction_id2': txn, 'captcha2_code': s}
                set_session(chat_id, 'sending_pdf_otp_direct', sd2)
                ok, otp_txn, msg = _new_bot().send_aadhaar_otp(sd2['eid'], s, ct, txn)
                if ok:
                    if mid4: edit_message(chat_id, mid4,
                        f"<b>{BOT_NAME}</b>\n{DIVIDER}\n{em('bot', '🤖')}  <b>〔 Captcha 〕</b>\n\n"
                        f"{em('verified', '✅')}  Ban gaya\n"
                        f"{em('verified', '✅')}  Auto-solve ho gaya\n"
                        f"{em('verified', '✅')}  OTP bhej diya")
                    set_session(chat_id, 'awaiting_pdf_otp_direct', {**sd2, 'pdf_otp_txn_id': otp_txn})
                    send_message(chat_id,
                        f"<b>{BOT_NAME}</b>\n{DIVIDER}\n"
                        f"{em('sparkle', '📩')}  <b>〔 OTP Bheja {em('success', '✅')} 〕</b>\n\n"
                        f"▸  PDF ke liye 6-digit OTP daalo\n\n"
                        f"<i>{em('lightning', '⏳')}  10 minute tak valid</i>",
                        reply_markup=get_cancel_keyboard())
                else:
                    clear_session(chat_id)
                    send_message(chat_id,
                        f"<b>{BOT_NAME}</b>\n{DIVIDER}\n"
                        f"{em('cross', '❌')}  OTP fail — {msg}\n\n"
                        f"<i>{em('arrow', '🔄')}  Neeche se tarika chuno, phir try karo.</i>")
            elif img:
                sd = get_session(chat_id)['data']
                t = (f"<b>{BOT_NAME}</b>\n{DIVIDER}\n{em('bot', '🤖')}  <b>〔 Captcha 〕</b>\n\n"
                     f"{em('verified', '✅')}  Ban gaya\n"
                     f"{em('cross', '❌')}  Auto-solve fail\n"
                     f"◌  Aap khud solve karo")
                if mid4: edit_message(chat_id, mid4, t)
                set_session(chat_id, 'awaiting_captcha_direct',
                            {**sd, 'captcha2_txn_id': ct, 'transaction_id2': txn})
                send_photo(chat_id, img,
                    caption=f"<i>{em('arrow', '👆')}  Upar wale characters type karo</i>")
            else:
                _captcha_failed(chat_id, mid4)
        else:
            send_message(chat_id,
                f"<b>{BOT_NAME}</b>\n{DIVIDER}\n"
                f"{em('cross', '❌')}  EID galat hai.\n\n"
                f"<i>{em('sparkle', '🔹')}  Check karke phir Enrollment ID daalo.</i>")

    elif step == 'awaiting_captcha_direct':
        sd = {**d, 'captcha2_code': text.strip()}
        set_session(chat_id, 'sending_pdf_otp_direct', sd)
        send_message(chat_id,
            f"<b>{BOT_NAME}</b>\n{DIVIDER}\n"
            f"{em('satellite', '📡')}  <b>〔 OTP Bhej Rahe Hain 〕</b>\n\n"
            f"<i>{em('lightning', '⏳')}  Zara ruko…</i>")
        ok, otp_txn, msg = _new_bot().send_aadhaar_otp(
            sd['eid'], sd['captcha2_code'], sd['captcha2_txn_id'], sd['transaction_id2'])
        if ok:
            set_session(chat_id, 'awaiting_pdf_otp_direct', {**sd, 'pdf_otp_txn_id': otp_txn})
            send_message(chat_id,
                f"<b>{BOT_NAME}</b>\n{DIVIDER}\n"
                f"{em('sparkle', '📩')}  <b>〔 OTP Bheja {em('success', '✅')} 〕</b>\n\n"
                f"▸  PDF ke liye 6-digit OTP daalo\n\n"
                f"<i>{em('lightning', '⏳')}  10 minute tak valid</i>",
                reply_markup=get_cancel_keyboard())
        else:
            clear_session(chat_id)
            send_message(chat_id,
                f"<b>{BOT_NAME}</b>\n{DIVIDER}\n"
                f"{em('cross', '❌')}  OTP fail — {msg}\n\n"
                f"<i>{em('arrow', '🔄')}  Neeche se tarika chuno, phir try karo.</i>")

    elif step == 'awaiting_pdf_otp_direct':
        if re.match(r'^\d{6}$', text):
            send_message(chat_id,
                f"<b>{BOT_NAME}</b>\n{DIVIDER}\n"
                f"{em('verified', '✅')}  <b>〔 Download Kar Rahe Hain 〕</b>\n\n"
                f"<i>{em('lightning', '⚡')}  Aapka Aadhaar PDF la rahe hain…</i>")
            ok, path = _new_bot().download_aadhaar_pdf(
                d['eid'], text, d['pdf_otp_txn_id'], d['transaction_id2'], False)
            if ok and path and '.pdf' in path:
                deliver_pdf(chat_id, path, d.get('verified_name', 'Mr.'))
            else:
                clear_session(chat_id)
                send_message(chat_id,
                    f"<b>{BOT_NAME}</b>\n{DIVIDER}\n"
                    f"{em('cross', '❌')}  Download fail — {path}\n\n"
                    f"<i>{em('arrow', '🔄')}  Neeche se tarika chuno, phir try karo.</i>")
        else:
            send_message(chat_id,
                f"<b>{BOT_NAME}</b>\n{DIVIDER}\n"
                f"{em('cross', '❌')}  OTP galat hai.\n\n"
                f"<i>{em('sparkle', '🔹')}  6-digit number daalo (sirf digits).</i>")

# ============== DISPATCHER ==============
_msg_executor = ThreadPoolExecutor(max_workers=64, thread_name_prefix="upd")

def _process_update(update):
    try:
        if 'callback_query' in update:
            cq = update['callback_query']
            cid = cq['message']['chat']['id']
            if is_banned(cid):
                answer_callback_query(cq['id'], "Aap is bot se ban ho chuke ho.")
                return
            handle_callback(cid, cq['id'], cq.get('data', ''))
        elif 'message' in update:
            msg = update['message']
            cid = msg['chat']['id']
            text = msg.get('text', '').strip()
            if not text: return
            if is_banned(cid):
                send_message(cid,
                    f"<b>{BOT_NAME}</b>\n{DIVIDER}\n"
                    f"{em('lock', '🚫')}  <b>Aap ban ho chuke ho.</b>\n\n"
                    f"<i>◌  Access band hai.</i>\n\n{DIVIDER}")
                return
            if text.startswith('/start'):
                parts = text.split()
                ref = None
                if len(parts) > 1 and parts[1].startswith('ref_'):
                    try: ref = int(parts[1][4:])
                    except ValueError: pass
                if not is_channel_member(cid):
                    _fn = get_force_channel().get("name") or "hamara channel"
                    send_message(cid,
                        f"<b>{BOT_NAME}</b>\n{DIVIDER}\n"
                        f"<b>〔 Pehle Channel Join Karo 〕</b>\n\n"
                        f"▸  Bot use karne ke liye <b>{_fn}</b> join karo.\n\n"
                        f"{DIVIDER}\n"
                        f"<i>◌  Join ke baad neeche button pe tap karo.</i>",
                        reply_markup=get_join_keyboard())
                    return
                ensure_user(cid, ref)
                clear_session(cid)
                send_welcome_banner(cid)
            elif is_owner(cid) and text.startswith('/'):
                if not handle_admin_command(cid, text, msg):
                    handle_message(cid, text)
            else:
                handle_message(cid, text)
    except Exception as e:
        logger.error(f"update: {e}")

def get_updates(offset=None):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates"
    p = {'timeout': 10, 'allowed_updates': ['message', 'callback_query']}
    if offset: p['offset'] = offset
    try:
        r = get_telegram_session().get(url, params=p, timeout=13).json()
        return r.get('result', []) if r.get('ok') else []
    except Exception as e:
        logger.error(f"getUpdates: {e}")
        return []

# ============== MAIN ==============
def main():
    _init_db()
    print("━" * 52)
    print(f"  🇮🇳  {CONSOLE_BOT_NAME}")
    print("━" * 52)
    if not TELEGRAM_BOT_TOKEN:
        print("[ error ] TELEGRAM_BOT_TOKEN set nahi hai (.env mein daalo).")
        return
    try:
        j = get_telegram_session().get(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getMe", timeout=10
        ).json()
        if not j.get('ok'):
            print(f"[ error ] Bot auth fail: {j}")
            return
        global _bot_username
        _bot_username = j['result']['username']
        print(f"[ online ]  @{_bot_username}")
    except Exception as e:
        print(f"[ error ] {e}")
        return
    _p = get_proxy_config()
    _plist = _p.get("list", [])
    if _p.get("enabled") and _plist:
        print(f"[ proxy  ]  ON — {len(_plist)} proxies (rotate={_p.get('rotate', True)})")
    elif _p.get("enabled") and _p.get("url"):
        print(f"[ proxy  ]  ON — single: {_p['url']}")
    else:
        print(f"[ proxy  ]  direct")
    _fj = get_force_channel()
    _lc = get_log_channel()
    print(f"[ time   ]  {now_ist().strftime('%d/%m/%Y %H:%M:%S IST')}")
    print(f"[ storage]  MongoDB @ {MONGO_DB_NAME or 'aadhaar_bot'}")
    print(f"[ force  ]  {'ON — ' + (_fj.get('name') or _fj.get('chat_id')) if force_join_enabled() else 'OFF'}")
    print(f"[ logs   ]  {_lc.get('id') if log_channel_enabled() else 'disabled'}")
    print(f"[ cracker]  PyPDF2 / 4 threads")
    print(f"[ credits]  system active")
    print(f"[ owners ]  {', '.join(str(x) for x in OWNER_IDS) or 'none'}")
    print(f"[ mode   ]  Swadeshi · Made in India · CC0 (copyright waived)")

    def _sigint(sig, frame):
        print("\n[ stopped ]"); os._exit(0)
    signal.signal(signal.SIGINT, _sigint)

    threading.Thread(target=_cleanup_sessions, daemon=True).start()
    print("━" * 52)
    print("  Chal raha hai  —  Ctrl+C se band karo")
    print("━" * 52)

    last_id = 0
    while True:
        try:
            for u in get_updates(last_id + 1):
                last_id = u.get('update_id')
                _msg_executor.submit(_process_update, u)
        except KeyboardInterrupt:
            print("\n[ stopped ]"); os._exit(0)
        except Exception as e:
            logger.error(f"main loop: {e}")
            time.sleep(5)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n[ stopped ]"); os._exit(0)