# year_analysis.py
import yaml
import gspread
from google.oauth2.service_account import Credentials

def analyze_by_year():
    # Google Sheets 연결
    cfg = yaml.safe_load(open('config/settings.yaml'))
    creds = Credentials.from_service_account_file(cfg['service_account_json'], 
                                                scopes=['https://www.googleapis.com/auth/spreadsheets'])
    gc = gspread.authorize(creds)
    ws = gc.open_by_key(cfg['sheet_id']).worksheet(cfg['worksheet'])
    
    # clncTestSn 가져오기
    header = ws.row_values(1)
    clnc_col_idx = header.index('clncTestSn') + 1
    sns = [int(sn) for sn in ws.col_values(clnc_col_idx)[1:] if sn.strip()]
    
    # 연도별 분석
    year_stats = {}
    for sn in sns:
        year = int(str(sn)[:4])
        if year not in year_stats:
            year_stats[year] = []
        year_stats[year].append(sn)
    
    print("연도별 상세 분석:")
    print("=" * 50)
    
    for year in sorted(year_stats.keys()):
        sns_in_year = sorted(year_stats[year])
        count = len(sns_in_year)
        min_sn = min(sns_in_year)
        max_sn = max(sns_in_year)
        expected_range = max_sn - min_sn + 1
        missing = expected_range - count
        
        print(f"{year}년: {count}개 수집 (범위: {min_sn}~{max_sn})")
        print(f"  예상 범위: {expected_range}개, 빠짐: {missing}개")
        
        # 각 연도의 갭 찾기
        gaps = []
        for i in range(len(sns_in_year) - 1):
            gap = sns_in_year[i+1] - sns_in_year[i]
            if gap > 1:
                gaps.append((sns_in_year[i] + 1, sns_in_year[i+1] - 1, gap - 1))
        
        if gaps:
            print(f"  빠진 구간: {len(gaps)}개")
            for start, end, gap_count in gaps[:5]:  # 상위 5개만
                print(f"    {start}~{end} ({gap_count}개)")
        print()

if __name__ == "__main__":
    analyze_by_year()