import gspread
import logging
import threading
import time
import os
import json
from dotenv import load_dotenv
from google.oauth2.service_account import Credentials

SHEET_ID = "1e5YbzdgM2-orRa04sWtrgZiVXP8rql6JNZFZ-jcMVw4"

sheets_lock = threading.RLock()
_sheets_spreadsheet = None
_worksheet_cache = {}

SHEET_STRUCTURES = {
    "USERS": ["user_id", "username", "paid", "entry_amount", "joined_date"],
    "TEAMS": ["user_id", "team_players", "captain", "vice_captain"],
    "PAYMENTS": ["user_id", "amount", "upi_txn_id", "timestamps", "status"],
    "WITHDRAWALS": ["user_id", "amount", "upi_id", "timestamp", "status"],
    "RESULTS": ["contest_date", "user_id", "points", "rank", "prize"],
     "MATCHES": ["match_id", "name", "type", "deadline", "live_link"],
}

# Load env variables
load_dotenv()


def init_sheets():
    global _sheets_spreadsheet

    if _sheets_spreadsheet:
        return _sheets_spreadsheet

    with sheets_lock:
        try:
            scopes = [
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive"
            ]

            raw_creds = os.getenv("GOOGLE_CREDENTIALS")
            g_private_key = os.getenv("G_PRIVATE_KEY")
            creds_info = None

            # Method 1: Individual Variables (Render ke liye sabse best)
            if g_private_key:
                logging.info("🔑 Loading credentials from individual environment variables.")
                creds_info = {
                    "type": os.getenv("G_TYPE", "service_account"),
                    "project_id": os.getenv("G_PROJECT_ID"),
                    "private_key_id": os.getenv("G_PRIVATE_KEY_ID"),
                    "private_key": g_private_key.replace("\\n", "\n"),
                    "client_email": os.getenv("G_CLIENT_EMAIL"),
                    "client_id": os.getenv("G_CLIENT_ID"),
                    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                    "token_uri": "https://oauth2.googleapis.com/token",
                    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
                    "client_x509_cert_url": os.getenv("G_CERT_URL"),
                    "universe_domain": "googleapis.com"
                }
            elif raw_creds and isinstance(raw_creds, str):
                try:
                    creds_info = json.loads(raw_creds.strip().strip("'").strip('"'))
                except Exception as e:
                    logging.error(f"❌ Failed to parse GOOGLE_CREDENTIALS: {e}")

            if not creds_info and os.path.exists("credentials.json"):
                try:
                    if os.path.getsize("credentials.json") > 0:
                        with open("credentials.json", "r") as f:
                            creds_info = json.load(f)
                    else:
                        logging.error("❌ credentials.json is empty. Please paste your service account JSON.")
                except Exception as e:
                    logging.error(f"❌ Error reading local credentials.json: {e}")

            if not creds_info:
                logging.error("❌ No Google Credentials found! Check Environment Variables.")
                return None

            creds = Credentials.from_service_account_info(
                creds_info,
                scopes=scopes
            )

            gc = gspread.authorize(creds)
            _sheets_spreadsheet = gc.open_by_key(SHEET_ID)

            logging.info("✅ Google Sheets initialized successfully.")
            return _sheets_spreadsheet

        except Exception as e:
            logging.error(f"❌ GSheets Init Error: {e}")
            return None


def safe_api_call(func, *args, **kwargs):
    for _ in range(5):
        try:
            with sheets_lock:
                return func(*args, **kwargs)
        except Exception:
            time.sleep(1)
    return None


def get_or_create_sheet(sh, sheet_name, headers):
    global _worksheet_cache
    sheet_name = sheet_name.upper()

    if sheet_name in _worksheet_cache:
        return _worksheet_cache[sheet_name]

    with sheets_lock:
        existing = [ws.title.upper() for ws in sh.worksheets()]

        if sheet_name in existing:
            sheet = sh.worksheet(sheet_name)
        else:
            sheet = sh.add_worksheet(
                title=sheet_name,
                rows="1000",
                cols=len(headers)
            )
            sheet.append_row(headers)

        _worksheet_cache[sheet_name] = sheet
        return sheet


def format_players(data):
    if isinstance(data, dict):
        all_p = []
        for r in ['bat', 'wk', 'ar', 'bowl', 'sub']:
            all_p.extend(data.get(r, []))
        return ",".join(filter(None, all_p))
    return str(data)


def append_row_safe(sheet, headers, data_dict):
    with sheets_lock:
        mapping = {
            "team_players": "players",
            "timestamps": "timestamp",
            "entry_amount": "balance"
        }

        row = []
        for h in headers:
            val = data_dict.get(h)
            if val is None:
                val = data_dict.get(mapping.get(h), "")

            if h == "players":
                val = format_players(val)

            row.append(str(val).strip())

        all_rows = safe_api_call(sheet.get_all_values)

        unique_id_map = {
            "USERS": [0],
            "PAYMENTS": [0, 2],
            "TEAMS": [0],
            "WITHDRAWALS": [0, 3]
        }

        keys_to_check = unique_id_map.get(sheet.title.upper(), [0])

        row_index = -1

        if all_rows and len(all_rows) > 1:
            for idx, existing_row in enumerate(all_rows[1:], start=2):
                if all(
                    str(existing_row[k]) == str(row[k])
                    for k in keys_to_check
                    if k < len(existing_row)
                ):
                    row_index = idx
                    break

        if row_index != -1:
            if sheet.title.upper() == "PAYMENTS":
                return

            range_label = f"A{row_index}:{chr(64 + len(headers))}{row_index}"
            safe_api_call(sheet.update, range_label, [row])
        else:
            safe_api_call(sheet.append_row, row)


def sync_to_sheets(user_data, sheet_type="USERS"):
    headers = SHEET_STRUCTURES.get(sheet_type.upper())
    sh = init_sheets()

    if sh:
        sheet = get_or_create_sheet(sh, sheet_type, headers)
        if sheet:
            append_row_safe(sheet, headers, user_data)


def sync_wrapper(user_data, sheet_type):
    threading.Thread(
        target=sync_to_sheets,
        args=(user_data, sheet_type),
        daemon=True
    ).start()


def get_all_rows_safe(sheet_type):
    headers = SHEET_STRUCTURES.get(sheet_type.upper())
    sh = init_sheets()

    if sh:
        sheet = get_or_create_sheet(sh, sheet_type, headers)
        if sheet:
            # Pass the function reference, not the result of the call
            return safe_api_call(sheet.get_all_records)

    return []
