# 임상시험 데이터 자동 수집 시스템

한국 임상시험 정보를 자동으로 수집하고 Google Sheets에 저장하는 완전 자동화 시스템입니다.

## 📋 시스템 개요

이 시스템은 매일 오전 10시에 자동으로 실행되어 새로운 임상시험 정보를 수집하고, 데이터를 정제한 후 Google Sheets에 업데이트합니다. 또한 다양한 조건으로 필터링된 별도의 시트들을 자동으로 생성하여 효율적인 데이터 관리를 지원합니다.

### 🔄 자동화 파이프라인

```
매일 10:00 AM (cron)
     ↓
1️⃣ 증분 크롤링 (2c.py)
     ↓
2️⃣ 데이터 정제 (clean_trials.py)  
     ↓
3️⃣ Google Sheets 업데이트 (daily_update_2c.py)
     ├─ 매일 업데이트되는 전체 임상시험 시트에 데이터 추가
     ├─ 진행상태 드롭다운 속성 제거 (일반 텍스트로 유지)
     ├─ clncTestSn 기준 자동 정렬
     ↓
4️⃣ 필터링된 시트 생성 (sheets_filter.py)
     ├─ filtered_premium: 고급 필터링 (2상 이상, 10명 이상 등)
     ├─ filtered_recruiting: 모집중인 임상시험
     ├─ filtered_approved: 승인완료된 임상시험
     └─ 컨택상태 컬럼 추가 & 기존 데이터 보존
     ↓
📝 로그 저장 (logs/daily_YYYYMMDD.log)
```

## 🏗️ 프로젝트 구조

```
project/
├── README.md                    # 이 문서
├── requirements.txt             # Python 의존성
├── config/                      # 설정 파일
│   ├── settings.yaml           # 메인 설정 (시트ID, URL 템플릿 등)
│   └── sa.json                 # Google Sheets 서비스 계정 인증 (비공개)
├── crawler/                     # 웹 크롤링 모듈
│   ├── 1c_fixed.py             # 전체 데이터 수집 크롤러
│   └── 2c.py                   # 증분 수집 크롤러 (매일 실행)
├── pipeline/                    # 데이터 처리 파이프라인
│   ├── clean_trials.py         # 데이터 정제 및 변환
│   ├── sheets_io.py            # Google Sheets 입출력
│   ├── sheets_filter.py        # 필터링된 시트 생성
│   └── sheet_keys.py           # 시트 키 관리
├── jobs/                        # 스케줄 작업
│   ├── daily_update_2c.py      # 매일 자동 업데이트 스크립트
│   └── init_load_from_csv.py   # 초기 데이터 로드
├── logs/                        # 실행 로그
│   └── daily_YYYYMMDD.log      # 일별 실행 로그
└── outputs/                     # 출력 파일
    └── increment_*.csv         # 수집된 원시/정제 데이터
```

## ⚙️ 설정 파일

### config/settings.yaml
```yaml
sheet_id: "1PncuqrcU1pmNIHsWNepaJe9IdFDMJnpN82e-Fgdn52Y"  # Google Sheets ID
worksheet: "매일 업데이트 되는 전체 임상시험 시트"                # 워크시트 탭 이름
service_account_json: "config/sa.json"                     # 서비스 계정 인증 파일
since_sn_buffer: 10                                        # SN 버퍼 (안전 마진)
output_dir: "outputs"                                      # 출력 디렉터리
wait_seconds: 8                                            # 페이지 로딩 대기시간
pause: 0.35                                                # 요청 간 지연시간
max_consecutive_miss: 20                                   # 연속 실패 허용 횟수

url_templates:                                             # 크롤링 대상 URL 템플릿
  - "https://trialforme.konect.or.kr/clnctest/view.do?pageNo=&clncTestSn={sn}&..."
  - "https://www.koreaclinicaltrials.org/clnctest/view.do?pageNo=&clncTestSn={sn}&..."
```

## 📊 Google Sheets 구성

시스템은 하나의 Google Sheets 문서 내에 여러 워크시트를 자동으로 생성하고 관리합니다:

### 📋 메인 시트
- **매일 업데이트 되는 전체 임상시험 시트**: 모든 수집된 임상시험 데이터
  - clncTestSn 기준 자동 정렬
  - 진행상태는 일반 텍스트 (승인완료, 모집중, 모집완료, 종료)

### 🔍 필터링된 시트들 (매일 자동 생성)
- **filtered_premium**: 고급 필터링 조건을 모두 만족하는 임상시험
  - ❌ 건강인 대상 시험 제외 (생동성 시험 등)
  - ❌ 연구자 임상시험 제외
  - ✅ 2상 이상만 포함 (2a상, 2/3상 등 포함)
  - ✅ 국내 모집인원 10명 이상
  - ✅ 모집기간 12개월 이상
  - 📋 컨택상태 컬럼 포함 (드롭다운: 데이터없음, 컨택필요, 컨택중, 컨택종료, 계약진행중, 계약완료)

- **filtered_recruiting**: 진행상태가 "모집중"인 임상시험만
  - 📋 컨택상태 컬럼 포함
- **filtered_approved**: 진행상태가 "승인완료"인 임상시험만
  - 📋 컨택상태 컬럼 포함

### 💡 컨택상태 관리 기능
- filtered 시트들에만 컨택상태 컬럼 자동 추가 (매일 업데이트되는 전체 임상시험 시트 제외)
- 기존 컨택상태 데이터 보존 (매일 업데이트 시 리셋되지 않음)
- 드롭다운 선택으로 진행 상황 체계적 관리
- 새 데이터는 기본값 "데이터없음"으로 설정

## 🤖 자동 실행 설정 (Cron)

시스템은 cron을 통해 매일 오전 10시에 자동 실행됩니다:

```bash
# 현재 설정된 cron job 확인
crontab -l

# 설정 내용:
# 매일 10:00 AM에 실행
0 10 * * * cd /Users/park/project && /Users/park/project/.venv/bin/python jobs/daily_update_2c.py config/settings.yaml >> logs/daily_$(date +\%Y\%m\%d).log 2>&1
```

### Cron 설정 이해하기
- `0 10 * * *`: 매일 10시 0분에 실행
- `cd /Users/park/project`: 프로젝트 디렉터리로 이동
- `jobs/daily_update_2c.py`: 메인 실행 스크립트
- `>> logs/daily_$(date +\%Y\%m\%d).log`: 로그를 날짜별 파일에 저장
- `2>&1`: 에러도 같은 로그 파일에 저장

## 📊 데이터 수집 과정

### 1단계: 증분 크롤링 (crawler/2c.py)
- Google Sheets에서 마지막 수집된 clncTestSn 확인
- 새로운 임상시험 정보만 증분 수집
- 두 개 사이트를 자동 감지하여 접근:
  - trialforme.konect.or.kr
  - koreaclinicaltrials.org
- 수집 데이터: `outputs/increment_YYYYMMDD_HHMMSS.csv`

### 2단계: 데이터 정제 (pipeline/clean_trials.py)
- 임상시험 기간을 시작월/종료월로 분리
- 제목에서 진행상태 (승인완료/모집중/종료 등) 추출
- 불필요한 컬럼 제거 (크롤링일시, 담당자 정보 등)
- 병원명 정규화 (실시기관1~N)
- 더미/가비지 데이터 제거
- 정제 데이터: `outputs/increment_YYYYMMDD_HHMMSS_clean.csv`

### 3단계: Google Sheets 업데이트 (pipeline/sheets_io.py)
- 기존 데이터와 중복 확인 (clncTestSn 기준)
- 새로운 데이터만 Google Sheets에 추가
- 헤더 자동 생성 및 검증
- 업데이트 통계 리포트 생성

## 📝 로그 시스템

### 로그 파일 위치
```
logs/daily_20250912.log  # 2025년 9월 12일 실행 로그
```

### 로그 내용 예시
```
🚀 자동화된 매일 업데이트 시작
⏰ 실행 시간: 2025-09-12 10:00:00
============================================================
📡 1단계: 2c.py 증분 크롤링
▶️ 실행: python crawler/2c.py --cfg config/settings.yaml
✅ 원시 데이터 수집 완료: outputs/increment_20250912_100211.csv

🔧 2단계: 데이터 정제
▶️ 실행: python pipeline/clean_trials.py -i ... -o ...
✅ 데이터 정제 완료: outputs/increment_20250912_100211_clean.csv

📊 3단계: Google Sheets 업데이트
📋 기존 데이터: 6717개 항목
📈 처리된 총 행 수: 20
🆕 새로운 데이터: 10개
✅ Google Sheets에 10개 행 추가됨
📋 컨택상태 드롭다운 설정 중...
📋 컨택상태 드롭다운 설정 완료
🔄 clncTestSn 기준으로 데이터 정렬 중...
✅ 데이터 정렬 완료

📂 4단계: 필터링된 시트 업데이트
✅ 필터링된 시트 업데이트 완료

============================================================
🎉 자동화 업데이트 완료!
📊 최종 결과: 10개 새 항목 추가
📝 필터링된 시트도 업데이트됨
```

## 🛠️ 수동 실행 방법

### 전체 파이프라인 수동 실행
```bash
cd /Users/park/project
source .venv/bin/activate
python jobs/daily_update_2c.py config/settings.yaml
```

### 개별 단계 실행
```bash
# 1. 증분 크롤링만 실행
python crawler/2c.py --cfg config/settings.yaml

# 2. 데이터 정제만 실행  
python pipeline/clean_trials.py -i outputs/increment_*.csv -o outputs/cleaned.csv

# 3. Google Sheets 업로드만 실행
python pipeline/sheets_io.py outputs/cleaned.csv

# 4. 필터링된 시트만 생성 실행
python pipeline/sheets_filter.py config/settings.yaml
```

## 🔧 개발자 도구

### 전체 데이터 재수집 (초기 설정시)
```bash
python crawler/1c_fixed.py  # 2019~2024년 전체 데이터 수집
```

### CSV를 Google Sheets로 초기 로드
```bash
python jobs/init_load_from_csv.py clinical_trials_full_clean.csv
```

### 데이터 분석 도구
```bash
python gap_analysis.py      # 데이터 공백 분석
python year_analysis.py     # 연도별 통계
```

## 📈 모니터링 및 문제 해결

### 로그 모니터링
```bash
# 최신 로그 확인
tail -f logs/daily_$(date +%Y%m%d).log

# 에러 패턴 검색
grep -i "error\|fail\|exception" logs/daily_*.log
```

### 주요 확인 사항
1. **cron 작업 상태**: `crontab -l`로 설정 확인
2. **가상환경 경로**: `.venv/bin/python`이 올바른지 확인
3. **Google Sheets 인증**: `config/sa.json` 파일 권한
4. **네트워크 접근**: 임상시험 사이트 접근 가능 여부
5. **디스크 공간**: `outputs/`, `logs/` 디렉터리 용량

### 문제 해결
- **크롤링 실패**: 사이트 구조 변경 또는 네트워크 이슈
- **인증 오류**: Google Sheets 서비스 계정 키 만료
- **데이터 중복**: clncTestSn 중복 체크 로직 확인
- **로그 증가**: 오래된 로그 파일 정리 필요

## 📋 의존성 (requirements.txt)

```
selenium              # 웹 크롤링
webdriver-manager     # Chrome 드라이버 자동 관리
beautifulsoup4        # HTML 파싱
requests              # HTTP 요청
gspread               # Google Sheets API
google-auth           # Google 인증
PyYAML                # YAML 설정 파일
pandas                # 데이터 처리
```

## 🏃‍♂️ 빠른 시작

1. **가상환경 설정**
   ```bash
   cd /Users/park/project
   python -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```

2. **Google Sheets 인증 설정**
   - Google Cloud Console에서 서비스 계정 생성
   - 키를 `config/sa.json`에 저장
   - Google Sheets를 서비스 계정과 공유

3. **설정 파일 수정**
   ```bash
   vim config/settings.yaml  # sheet_id 등 수정
   ```

4. **cron 작업 등록**
   ```bash
   crontab -e  # 위의 cron 설정 추가
   ```

5. **첫 실행 테스트**
   ```bash
   python jobs/daily_update_2c.py config/settings.yaml
   ```

## 🆕 최신 업데이트 (2025-09-15)

### ✨ 새로 추가된 기능들

1. **📊 자동 정렬 기능**
   - 새 데이터 추가 후 clncTestSn 기준으로 자동 정렬
   - A열이 항상 오름차순으로 정리됨

2. **📋 컨택상태 관리 시스템**
   - 모든 시트에 "컨택상태" 컬럼 자동 추가 (clncTestSn과 진행상태 사이 위치)
   - 드롭다운 옵션: 데이터없음, 컨택필요, 컨택중, 컨택종료, 계약진행중, 계약완료
   - 새 데이터는 기본값 "데이터없음"으로 설정

3. **🔍 고급 필터링 시스템**
   - **filtered_premium**: 2상 이상 + 국내 10명 이상 + 12개월 이상 + 건강인/연구자시험 제외
   - **filtered_recruiting**: 모집중인 임상시험만
   - **filtered_approved**: 승인완료된 임상시험만
   - 매일 자동으로 필터링된 시트들이 갱신됨

4. **🤖 완전 자동화 파이프라인**
   - 기존 3단계 → 4단계로 확장
   - 컨택상태 드롭다운 자동 설정
   - 데이터 정렬 자동 적용
   - 필터링 시트 자동 생성

### 📈 시스템 성과 
- 전체 데이터: 6,700+ 개
- 기본 필터링: 1,700+ 개 (승인완료 + 모집중)
- 프리미엄 필터링: 700+ 개 (모든 고급 조건 만족)

## 📞 지원

시스템 관련 문의사항이나 오류 발생시 로그 파일(`logs/daily_*.log`)을 확인하시기 바랍니다.