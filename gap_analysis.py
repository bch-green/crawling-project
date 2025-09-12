# gap_analysis.py
import gspread
import yaml
from google.oauth2.service_account import Credentials

def find_missing_sns():
    # Google Sheets 연결
    cfg = yaml.safe_load(open('config/settings.yaml'))
    creds = Credentials.from_service_account_file(cfg['service_account_json'], 
                                                scopes=['https://www.googleapis.com/auth/spreadsheets'])
    gc = gspread.authorize(creds)
    ws = gc.open_by_key(cfg['sheet_id']).worksheet(cfg['worksheet'])
    
    # clncTestSn 컬럼 가져오기
    header = ws.row_values(1)
    clnc_col_idx = header.index('clncTestSn') + 1
    sns = [int(sn) for sn in ws.col_values(clnc_col_idx)[1:] if sn.strip()]
    sns.sort()
    
    # 연속성 확인
    missing_ranges = []
    for i in range(len(sns) - 1):
        gap = sns[i+1] - sns[i]
        if gap > 1:
            missing_start = sns[i] + 1
            missing_end = sns[i+1] - 1
            missing_count = gap - 1
            missing_ranges.append((missing_start, missing_end, missing_count))
    
    print(f"현재 수집된 SN 개수: {len(sns)}")
    print(f"최소 SN: {min(sns)}, 최대 SN: {max(sns)}")
    print(f"빠진 구간 개수: {len(missing_ranges)}")
    
    total_missing = sum(count for _, _, count in missing_ranges)
    print(f"총 빠진 SN 개수: {total_missing}")
    
    # 큰 구간부터 표시
    missing_ranges.sort(key=lambda x: x[2], reverse=True)
    print("\n빠진 구간 (큰 순서):")
    for start, end, count in missing_ranges[:10]:
        print(f"  {start} ~ {end} ({count}개)")
    
    return missing_ranges

if __name__ == "__main__":
    find_missing_sns()