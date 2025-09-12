import gspread
from google.oauth2.service_account import Credentials

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

def client_from_sa(sa_json_path: str):
    creds = Credentials.from_service_account_file(sa_json_path, scopes=SCOPES)
    return gspread.authorize(creds)

def open_ws(gc, sheet_id: str, ws_name: str):
    sh = gc.open_by_key(sheet_id)
    try:
        return sh.worksheet(ws_name)
    except gspread.WorksheetNotFound:
        return sh.add_worksheet(ws_name, rows=1000, cols=26)

def ensure_header(ws, header: list[str]):
    cur = ws.row_values(1)
    if cur == header:
        return
    if not cur:
        ws.append_row(header)
    else:
        # 간단: 헤더 다르면 덮지 않고 일단 사용자가 정렬 후 재실행 권장
        missing = [h for h in header if h not in cur]
        if missing:
            raise RuntimeError(f"시트 헤더 불일치. 누락: {missing}")

def list_existing_keys(ws, key_col="approval_no") -> set[str]:
    header = ws.row_values(1)
    if key_col not in header:
        return set()
    idx = header.index(key_col) + 1
    vals = ws.col_values(idx)[1:]
    return set(v.strip() for v in vals if v.strip())

def read_column_as_int(ws, col_name="clncTestSn") -> list[int]:
    header = ws.row_values(1)
    if col_name not in header:
        return []
    idx = header.index(col_name) + 1
    vals = ws.col_values(idx)[1:]
    out = []
    for v in vals:
        try:
            out.append(int(v))
        except:
            pass
    return out

def append_rows(ws, rows: list[dict], header: list[str]) -> int:
    if not rows:
        return 0
    values = [[row.get(h, "") for h in header] for row in rows]
    ws.append_rows(values, value_input_option="RAW")
    return len(values)
