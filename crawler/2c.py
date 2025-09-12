#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
임상시험 증분 크롤링 도구 (2c.py)

이 스크립트는 매일 자동으로 실행되어 새로운 임상시험 데이터만 수집하는 증분 크롤러입니다.

주요 기능:
- Google Sheets에서 최대 clncTestSn을 자동 감지
- 마지막 수집 이후 새로운 임상시험만 증분 수집
- 두 개 사이트 자동 감지 및 스위칭
- 1c_fixed.py와 동일한 데이터 추출 로직 사용
- 지수 백오프 및 에러 복구 지원

지원 사이트:
- trialforme.konect.or.kr (우선순위)
- koreaclinicaltrials.org (대체 사이트)

사용법:
  python crawler/2c.py --cfg config/settings.yaml

작성자: 자동화 시스템
최종 수정: 2025-09-12
"""

import os
import re
import time
import random
import argparse
from datetime import datetime
from typing import Optional, Set, List
from urllib.parse import urlparse, urlunparse

import pandas as pd
import yaml
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


class IncrementalClinicalTrialCrawler:
    """
    임상시험 증분 크롤링 클래스
    
    Google Sheets에서 마지막으로 수집된 clncTestSn을 확인하고,
    그 이후의 새로운 임상시험 데이터만 증분 수집하는 클래스입니다.
    
    주요 메소드:
    - get_max_clnc_sn_from_sheet(): Google Sheets에서 최대 SN 조회
    - detect_working_domain(): 사용 가능한 도메인 감지
    - crawl_incremental(): 증분 크롤링 실행
    - extract_detail_data(): 상세 데이터 추출 (1c_fixed.py와 동일)
    """
    def __init__(self, config_path: str = "config/settings.yaml"):
        """
        크롤러 초기화
        
        Args:
            config_path (str): YAML 설정 파일 경로
        """
        self.load_config(config_path)
        
        # 지원되는 사이트 목록 (우선순위에 따라 정렬)
        self.candidate_bases = [
            "https://trialforme.konect.or.kr",    # 주 사이트
            "https://www.koreaclinicaltrials.org",  # 대체 사이트
        ]
        
        self.base_url = self.candidate_bases[0]  # 초기 기본 URL
        self.driver = None                       # Selenium WebDriver 객체
        self.all_data = []                       # 수집된 데이터 저장
        self.probe_sn = 202499968               # 도메인 감지용 테스트 SN

    def load_config(self, config_path: str):
        """
        YAML 설정 파일에서 크롤링 설정 값들을 로드
        
        설정 항목:
        - sheet_id: Google Sheets 문서 ID
        - worksheet: 워크시트 탭 이름
        - service_account_json: 인증 파일 경로
        - since_sn_buffer: SN 버퍼 (안전 마진)
        - output_dir: 출력 파일 디렉터리
        - wait_seconds: 페이지 로딩 대기 시간
        - pause: 요청 간 지연 시간
        - max_consecutive_miss: 연속 실패 허용 횟수
        - url_templates: 크롤링 대상 URL 템플릿 목록
        
        Args:
            config_path (str): YAML 설정 파일 경로
        """
        with open(config_path, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)
        
        # Google Sheets 설정
        self.sheet_id = config.get("sheet_id", "")                    # Sheets 문서 ID
        self.worksheet = config.get("worksheet", "")                  # 워크시트 탭 이름
        self.service_account_json = config.get("service_account_json", "")  # 인증 파일
        
        # 크롤링 설정
        self.since_sn_buffer = config.get("since_sn_buffer", 10)      # SN 버퍼 (놓치지 않기 위해)
        self.output_dir = config.get("output_dir", "outputs")         # 출력 디렉터리
        self.wait_seconds = config.get("wait_seconds", 8)            # 페이지 로딩 대기시간
        self.pause = config.get("pause", 0.35)                       # 요청 간 지연
        self.max_consecutive_miss = config.get("max_consecutive_miss", 20)  # 연속 실패 허용
        # URL 템플릿 목록 (설정 파일에서 로드, 기본값 제공)
        self.url_templates = config.get("url_templates", [
            "https://trialforme.konect.or.kr/clnctest/view.do?pageNo=&clncTestSn={sn}&relatedSearchKeyword=&searchText=&recruitStartDate=&recruitEndDate=&status=",
            "https://www.koreaclinicaltrials.org/clnctest/view.do?pageNo=&clncTestSn={sn}&relatedSearchKeyword=&searchText=&recruitStartDate=&recruitEndDate=&status="
        ])

    def get_max_sn_from_sheet(self) -> Optional[int]:
        """Google Sheets에서 최대 clncTestSn 조회"""
        try:
            import gspread
            from google.oauth2.service_account import Credentials

            if not os.path.exists(self.service_account_json):
                print(f"⚠️ 서비스 계정 파일을 찾을 수 없습니다: {self.service_account_json}")
                return None

            creds = Credentials.from_service_account_file(
                self.service_account_json,
                scopes=["https://www.googleapis.com/auth/spreadsheets"]
            )
            gc = gspread.authorize(creds)
            sh = gc.open_by_key(self.sheet_id)
            ws = sh.worksheet(self.worksheet)

            # 헤더에서 clncTestSn 컬럼 찾기
            header = ws.row_values(1)
            clnc_col_idx = None
            for i, col in enumerate(header):
                if col.strip().lower() == "clnctestsn":
                    clnc_col_idx = i + 1
                    break

            if clnc_col_idx is None:
                print("⚠️ clncTestSn 컬럼을 찾을 수 없습니다.")
                return None

            # 해당 컬럼의 모든 값 가져오기
            values = ws.col_values(clnc_col_idx)[1:]  # 헤더 제외
            sn_list = []
            for v in values:
                try:
                    sn_list.append(int(str(v).strip()))
                except:
                    pass

            return max(sn_list) if sn_list else None

        except Exception as e:
            print(f"⚠️ Google Sheets 조회 실패: {e}")
            return None

    def _normalize_path(self, path: str) -> str:
        """중복 슬래시 제거"""
        return re.sub(r'/{2,}', '/', path)

    def build_url(self, base: str, path: str, query: str = "") -> str:
        """URL 안전 생성"""
        path = self._normalize_path(path)
        if query and not query.startswith("?"):
            query = "?" + query
        parts = urlparse(base)
        return urlunparse((parts.scheme, parts.netloc, path, "", query.lstrip("?"), ""))

    def setup_driver(self, headless: bool = True):
        """Selenium 드라이버 설정"""
        options = Options()
        if headless:
            options.add_argument("--headless=new")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)
        options.add_argument("--window-size=1280,1600")
        options.add_argument(
            "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        )
        
        self.driver = webdriver.Chrome(options=options)
        self.driver.set_page_load_timeout(30)
        print("✅ 드라이버 설정 완료")

        # 도메인 자동 감지
        self.detect_and_set_base_url()

    def detect_and_set_base_url(self, timeout: int = 8) -> None:
        """동작하는 베이스 URL 감지"""
        for cand in self.candidate_bases:
            try:
                url = self.build_url(cand, "/clnctest/view.do", f"clncTestSn={self.probe_sn}")
                self.driver.get(url)
                WebDriverWait(self.driver, timeout).until(
                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                )
                cur = urlparse(self.driver.current_url)
                detected = f"{cur.scheme}://{cur.netloc}"
                self.base_url = detected
                print(f"🌐 베이스 도메인 설정: {self.base_url}")
                return
            except Exception:
                continue
        print("⚠️ 베이스 도메인 자동 감지 실패. 기본값 사용:", self.base_url)

    def _looks_garbage_page(self, text: str) -> bool:
        """가비지 페이지 감지 (1c_fixed.py와 동일)"""
        if not text:
            return True
        t = text.replace(",", " ").strip()
        bad_keys = [
            "임상시험 정보", "식약처 승인 목록", "목록으로", "의약품 정보",
            "실시기관 정보", "대상자 선정기준", "대상자 제외기준",
            "연구설계 및 수행방법", "최초 사람대상 연구여부"
        ]
        hit = sum(1 for k in bad_keys if k in t)
        if hit >= 2:
            return True
        if len(t) > 300:
            return True
        return False

    def extract_detail_data(self, clnc_test_sn: int, wait_sec: int = 8) -> Optional[dict]:
        """
        상세 페이지 데이터 추출 (1c_fixed.py의 extract_detail_data와 동일)
        """
        url = self.build_url(self.base_url, "/clnctest/view.do", f"clncTestSn={clnc_test_sn}")
        try:
            self.driver.get(url)
            cur = urlparse(self.driver.current_url)
            cur_base = f"{cur.scheme}://{cur.netloc}"
            if cur_base != self.base_url:
                self.base_url = cur_base
        except Exception as e:
            print(f"❌ {clnc_test_sn} 페이지 로드 실패: {e}")
            return None

        try:
            WebDriverWait(self.driver, wait_sec).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
        except TimeoutException:
            print(f"⚠️ {clnc_test_sn} body 로드 타임아웃")
            return None

        data = {
            "clncTestSn": str(clnc_test_sn),
            "진행상태": "",
            "크롤링일시": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }

        # 1) 임상시험명 추출
        title = self._find_first_text([
            (By.CSS_SELECTOR, "div.recruit-group2 > div.box"),
            (By.CSS_SELECTOR, "div.recruit-group2 .box .tit, div.recruit-group2 .box .title"),
            (By.CSS_SELECTOR, "div.recruit-detail h3"),
            (By.CSS_SELECTOR, "div.view-tit, .view-tit, .view_title"),
            (By.CSS_SELECTOR, "h2.tit, h3.tit, h1.tit"),
            (By.CSS_SELECTOR, "div.recruit-detail, #contents, .contents, .container")
        ])
        
        if not title or self._looks_garbage_page(title):
            return None
        data["임상시험명"] = title

        # 2) 상세 정보 추출
        self._extract_from_txt_groups(data, [
            "div.recruit-detail div.txt-group",
            "div.txt-group",
            "section.detail .txt-group",
        ])
        self._extract_from_tables(data, [
            "table.view, table.tbl-view, table.tbl, table.table, .view table, .tbl table",
            "div.recruit-detail table",
            "table"
        ])
        self._extract_from_definition_lists(data, [
            "dl.view, dl.list, dl.info, .view dl, .info dl, dl"
        ])

        # 3) 실시기관 추출
        self._open_institution_tab()
        self._extract_institutions(data)

        return data

    def _get_text_safe(self, el) -> str:
        """안전한 텍스트 추출"""
        try:
            t = el.text.strip()
            return re.sub(r"\s+", " ", t)
        except Exception:
            return ""

    def _find_first_text(self, candidates) -> str:
        """첫 번째 유효한 텍스트 찾기"""
        for by, sel in candidates:
            try:
                el = self.driver.find_element(by, sel)
                text = self._get_text_safe(el)
                if text:
                    if len(text) > 500 and "\n" in text:
                        first_line = text.split("\n", 1)[0].strip()
                        if first_line:
                            return first_line
                    return text
            except NoSuchElementException:
                continue
            except Exception:
                continue
        return ""

    def _extract_from_txt_groups(self, data: dict, groups_selectors):
        """txt-group에서 정보 추출"""
        for sel in groups_selectors:
            try:
                groups = self.driver.find_elements(By.CSS_SELECTOR, sel)
                for g in groups:
                    try:
                        key_el = None
                        val_el = None
                        for ksel in [".tit", ".title", "strong", "b", "h4", "h5"]:
                            try:
                                key_el = g.find_element(By.CSS_SELECTOR, ksel)
                                if key_el:
                                    break
                            except NoSuchElementException:
                                continue
                        for vsel in [".txt", ".desc", ".cont", "p", "div"]:
                            try:
                                val_el = g.find_element(By.CSS_SELECTOR, vsel)
                                if val_el:
                                    break
                            except NoSuchElementException:
                                continue

                        key = self._get_text_safe(key_el) if key_el else ""
                        val = self._get_text_safe(val_el) if val_el else ""
                        if key and val and key not in ("임상시험명",):
                            data[key] = val
                    except Exception:
                        continue
            except Exception:
                continue

    def _extract_from_tables(self, data: dict, table_selectors):
        """테이블에서 정보 추출"""
        for sel in table_selectors:
            try:
                tables = self.driver.find_elements(By.CSS_SELECTOR, sel)
            except Exception:
                continue
            for tb in tables:
                try:
                    rows = tb.find_elements(By.CSS_SELECTOR, "tr")
                    for r in rows:
                        ths = r.find_elements(By.CSS_SELECTOR, "th")
                        tds = r.find_elements(By.CSS_SELECTOR, "td")
                        if ths and tds:
                            pairs = min(len(ths), len(tds))
                            for i in range(pairs):
                                key = self._get_text_safe(ths[i])
                                val = self._get_text_safe(tds[i])
                                if key and val and key not in ("임상시험명",):
                                    data[key] = val
                except Exception:
                    continue

    def _extract_from_definition_lists(self, data: dict, dl_selectors):
        """정의 리스트에서 정보 추출"""
        for sel in dl_selectors:
            try:
                dls = self.driver.find_elements(By.CSS_SELECTOR, sel)
            except Exception:
                continue
            for dl in dls:
                try:
                    dts = dl.find_elements(By.TAG_NAME, "dt")
                    dds = dl.find_elements(By.TAG_NAME, "dd")
                    pairs = min(len(dts), len(dds))
                    for i in range(pairs):
                        key = self._get_text_safe(dts[i])
                        val = self._get_text_safe(dds[i])
                        if key and val and key not in ("임상시험명",):
                            data[key] = val
                except Exception:
                    continue

    def _open_institution_tab(self) -> None:
        """실시기관 탭 열기"""
        try:
            tab_btn = None
            for xp in [
                "//a[contains(., '실시기관')]",
                "//button[contains(., '실시기관')]",
                "//li[a[contains(., '실시기관')]]/a",
            ]:
                try:
                    tab_btn = self.driver.find_element(By.XPATH, xp)
                    if tab_btn:
                        break
                except NoSuchElementException:
                    continue
            if tab_btn:
                self.driver.execute_script("arguments[0].scrollIntoView({block:'center'});", tab_btn)
                tab_btn.click()
                try:
                    WebDriverWait(self.driver, 2).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, "#tab2, #tab02"))
                    )
                except TimeoutException:
                    pass
                return
        except Exception:
            pass

        # 강제 표시
        for sel in ["#tab2", "#tab02"]:
            try:
                el = self.driver.find_element(By.CSS_SELECTOR, sel)
                self.driver.execute_script("arguments[0].style.display='block';", el)
                return
            except Exception:
                continue

    def _extract_institutions(self, data: dict, max_rows: int = 30) -> None:
        """실시기관 정보 추출"""
        containers = []
        for sel in ["#tab2", "#tab02", "section.institution, .institution, .org-list"]:
            try:
                containers.extend(self.driver.find_elements(By.CSS_SELECTOR, sel))
            except Exception:
                continue

        def _scan_tables(root, start_idx=1):
            idx = start_idx
            tables = root.find_elements(By.TAG_NAME, "table")
            for tb in tables:
                try:
                    rows = tb.find_elements(By.CSS_SELECTOR, "tbody tr") or tb.find_elements(By.CSS_SELECTOR, "tr")
                    for r in rows:
                        cols = r.find_elements(By.TAG_NAME, "td")
                        if not cols:
                            continue
                        name = self._get_text_safe(cols[0])
                        if not name:
                            continue
                        data[f"실시기관{idx}"] = name
                        if len(cols) > 1:
                            data[f"실시기관{idx}_담당자"] = self._get_text_safe(cols[1])
                        if len(cols) > 2:
                            extra = [self._get_text_safe(c) for c in cols[2:]]
                            extra = [x for x in extra if x]
                            if extra:
                                data[f"실시기관{idx}_기타"] = " | ".join(extra)
                        idx += 1
                        if idx > start_idx + max_rows - 1:
                            return idx
                except Exception:
                    continue
            return idx

        next_idx = 1
        for c in containers:
            next_idx = _scan_tables(c, next_idx)
            if next_idx > max_rows:
                return

    def crawl_incremental(self, since_sn: int, limit: Optional[int] = None, 
                         headless: bool = True, verbose: bool = False) -> List[dict]:
        """증분 크롤링 실행"""
        
        rows = []
        misses = 0
        sn = since_sn + 1
        
        self.setup_driver(headless=headless)
        
        try:
            while True:
                if limit is not None and len(rows) >= limit:
                    break
                
                if verbose:
                    print(f"🔍 SN={sn} 크롤링 시도")
                
                data = self.extract_detail_data(sn)
                
                if data:
                    rows.append(data)
                    misses = 0
                    if verbose:
                        title_preview = data.get('임상시험명', '')[:50]
                        print(f"✅ SN={sn} 수집: {title_preview}...")
                else:
                    misses += 1
                    if verbose:
                        print(f"❌ SN={sn} 미존재 (연속: {misses})")
                    
                    if misses >= self.max_consecutive_miss:
                        print(f"🛑 연속 미존재 {misses}회 → 크롤링 종료")
                        break
                
                sn += 1
                time.sleep(self.pause + random.uniform(0, 0.2))
                
        finally:
            if self.driver:
                try:
                    self.driver.quit()
                except Exception:
                    pass
        
        return rows

    def save_to_csv(self, rows: List[dict], filename: str = None) -> str:
        """CSV 저장 (1c_fixed.py와 동일한 형식)"""
        if not rows:
            print("⚠️ 저장할 데이터가 없습니다.")
            return ""
        
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"increment_{timestamp}.csv"
        
        output_path = os.path.join(self.output_dir, filename)
        os.makedirs(self.output_dir, exist_ok=True)
        
        # 1c_fixed.py와 동일한 컬럼 순서
        all_columns = set()
        for row in rows:
            all_columns.update(row.keys())
        
        # 기본 컬럼 순서
        base_columns = [
            "clncTestSn", "진행상태", "크롤링일시", "임상시험명",
            "임상시험 의뢰자", "소재지", "대상질환", "대상질환명",
            "임상시험 단계", "임상시험 기간", "성별", "나이",
            "목표 대상자 수(국내)", "임상시험 승인일자", "최근 변경일자", "이용문의"
        ]
        
        # 실시기관 컬럼들
        institution_columns = []
        for i in range(1, 31):
            for suffix in ["", "_담당자", "_기타"]:
                col = f"실시기관{i}{suffix}"
                if col in all_columns:
                    institution_columns.append(col)
        
        # 나머지 컬럼들
        remaining_columns = [col for col in sorted(all_columns) 
                           if col not in base_columns + institution_columns]
        
        final_columns = base_columns + institution_columns + remaining_columns
        
        # DataFrame 생성 및 저장
        df = pd.DataFrame(rows)
        df = df.reindex(columns=final_columns, fill_value="")
        df.to_csv(output_path, index=False, encoding='utf-8-sig')
        
        print(f"✅ CSV 저장 완료: {output_path}")
        print(f"📊 {len(rows)}개 항목, {len(final_columns)}개 컬럼")
        
        return output_path


def main():
    parser = argparse.ArgumentParser(description="증분 임상시험 크롤러 (1c_fixed.py 호환)")
    parser.add_argument("--cfg", default="config/settings.yaml", help="설정 파일 경로")
    parser.add_argument("--since-sn", type=int, help="시작 SN (지정하지 않으면 시트에서 자동 계산)")
    parser.add_argument("--limit", type=int, help="최대 수집 건수")
    parser.add_argument("--output", help="출력 CSV 파일명")
    parser.add_argument("--headless", action="store_true", default=True, help="헤드리스 모드")
    parser.add_argument("--verbose", action="store_true", help="상세 로그")
    
    args = parser.parse_args()
    
    try:
        crawler = IncrementalClinicalTrialCrawler(args.cfg)
        
        # 시작 SN 결정
        if args.since_sn is not None:
            since_sn = args.since_sn
            print(f"📍 수동 지정 since_sn: {since_sn}")
        else:
            print("🔍 Google Sheets에서 최대 SN 조회 중...")
            max_sn = crawler.get_max_sn_from_sheet()
            if max_sn is None:
                print("❌ 시트에서 최대 SN을 찾을 수 없습니다. --since-sn 옵션을 사용하세요.")
                return 1
            since_sn = max(0, max_sn - crawler.since_sn_buffer)
            print(f"📊 시트 최대 SN: {max_sn}, 버퍼: {crawler.since_sn_buffer} → since_sn: {since_sn}")
        
        print(f"🚀 증분 크롤링 시작: SN {since_sn + 1}부터")
        
        # 크롤링 실행
        rows = crawler.crawl_incremental(
            since_sn=since_sn,
            limit=args.limit,
            headless=args.headless,
            verbose=args.verbose
        )
        
        if rows:
            output_path = crawler.save_to_csv(rows, args.output)
            print(output_path)  # 파이프라인에서 사용할 경로 출력
            return 0
        else:
            print("❌ 수집된 데이터가 없습니다.")
            return 1
            
    except Exception as e:
        print(f"❌ 크롤링 실패: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    exit(main())