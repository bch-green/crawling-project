from pipeline.sheets_io import client_from_sa, open_ws, list_existing_keys, read_column_as_int

def load_keys_and_max_sn(cfg) -> tuple[set[str], int | None]:
    gc = client_from_sa(cfg["service_account_json"])
    ws = open_ws(gc, cfg["sheet_id"], cfg["worksheet"])
    existing_keys = list_existing_keys(ws, "approval_no")
    sn_list = read_column_as_int(ws, "clncTestSn")
    max_sn = max(sn_list) if sn_list else None
    return existing_keys, max_sn, gc, ws
