"""
1c.py - 임상시험 전체 데이터 크롤링 (상세 페이지 방식, 최종본)
- 연도별 정순(오름차순) 크롤링: 2019 -> 2020 -> ... -> 2024
- 연속 미존재 임계치 10회로 연도 종료 자동 판단 (빠른 종료)
- 2019~2024는 '기존에 있어도' 재수집, 그 외 연도는 기존 데이터 스킵
- 도메인 자동 감지 (trialforme.konect.or.kr / koreaclinicaltrials.org)
- URL 정규화(중복 슬래시 제거) 및 리다이렉트 베이스 갱신
- 레거시(2019~2024) + 신규(2025) 템플릿 모두 대응(txt-group/table/dl)
"""

import os
import re
import time
from datetime import datetime
from typing import Optional, Set, Iterable
from urllib.parse import urlparse, urlunparse

import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


# =========================================================
# 크롤러
# =========================================================
class ClinicalTrialCrawler:
    def __init__(self):
        # 실제 서비스가 제공되는 후보 도메인 (우선순위)
        self.candidate_bases = [
            "https://trialforme.konect.or.kr",
            "https://www.koreaclinicaltrials.org",
        ]
        # 초기 베이스 (감지 후 갱신됨)
        self.base_url = self.candidate_bases[0]
        self.driver = None
        self.all_data = []
        self.csv_path = 'clinical_trials_full.csv'
        # 도메인 감지용 프로브 SN (예시)
        self.probe_sn = 202499968

    # -------------------------------
    # 공통 유틸
    # -------------------------------
    def _normalize_path(self, path: str) -> str:
        """중복 슬래시 제거: //clnctest → /clnctest"""
        return re.sub(r'/{2,}', '/', path)

    def build_url(self, base: str, path: str, query: str = "") -> str:
        """base + path + query로 안전하게 URL 생성"""
        path = self._normalize_path(path)
        if query and not query.startswith("?"):
            query = "?" + query
        parts = urlparse(base)
        return urlunparse((parts.scheme, parts.netloc, path, "", query.lstrip("?"), ""))

    def sn_to_year(self, sn: int) -> int:
        """SN 앞 4자리를 연도로 해석 (예: 201900001 -> 2019)"""
        try:
            return int(str(sn)[:4])
        except Exception:
            return -1

    # -------------------------------
    # 드라이버/도메인
    # -------------------------------
    def setup_driver(self, headless: bool = False):
        """Selenium 드라이버 설정 + 도메인 자동 감지"""
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
        """후보 도메인을 순차 시도해 실제 동작하는 base_url로 갱신"""
        for cand in self.candidate_bases:
            try:
                url = self.build_url(cand, "/clnctest/view.do", f"clncTestSn={self.probe_sn}")
                self.driver.get(url)
                WebDriverWait(self.driver, timeout).until(
                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                )
                # 리다이렉트 감지 시 현재 호스트로 교체
                cur = urlparse(self.driver.current_url)
                detected = f"{cur.scheme}://{cur.netloc}"
                self.base_url = detected
                print(f"🌐 베이스 도메인 설정: {self.base_url}")
                return
            except Exception:
                continue
        print("⚠️ 베이스 도메인 자동 감지 실패. 기본값 사용:", self.base_url)

    # -------------------------------
    # 가비지 타이틀 감지
    # -------------------------------
    def _looks_garbage_page(self, text: str) -> bool:
        """레이아웃/목록 텍스트가 제목으로 추출된 경우를 미존재로 간주"""
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
        if len(t) > 300:  # 비정상적으로 긴 덤프
            return True
        return False

    # -------------------------------
    # 상세 페이지 파서 (다중 템플릿)
    # -------------------------------
    def extract_detail_data(self, clnc_test_sn: int, wait_sec: int = 8) -> Optional[dict]:
        """상세 페이지에서 데이터 추출 (2019~2025 모든 템플릿 대응)"""
        url = self.build_url(self.base_url, "/clnctest/view.do", f"clncTestSn={clnc_test_sn}")
        try:
            self.driver.get(url)
            # 리다이렉트 시 base 갱신
            cur = urlparse(self.driver.current_url)
            cur_base = f"{cur.scheme}://{cur.netloc}"
            if cur_base != self.base_url:
                print(f"🔁 도메인 변경 감지: {self.base_url} → {cur_base}")
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

        # 1) 임상시험명 (필수)
        title = self._find_first_text([
            (By.CSS_SELECTOR, "div.recruit-group2 > div.box"),  # 2025
            (By.CSS_SELECTOR, "div.recruit-group2 .box .tit, div.recruit-group2 .box .title"),
            (By.CSS_SELECTOR, "div.recruit-detail h3"),
            (By.CSS_SELECTOR, "div.view-tit, .view-tit, .view_title"),
            (By.CSS_SELECTOR, "h2.tit, h3.tit, h1.tit"),
            (By.CSS_SELECTOR, "div.recruit-detail, #contents, .contents, .container")
        ])
        if not title or self._looks_garbage_page(title):
            # 제목이 없거나 레이아웃 덤프면 미존재로 처리
            # print(f"⚠️ {clnc_test_sn} 비정상/가비지 페이지로 판단 → 미존재")
            return None
        data["임상시험명"] = title

        # 2) 상세 정보 (txt-group / table / dl 모두 시도)
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

        # 3) 실시기관 (탭)
        self._open_institution_tab()
        self._extract_institutions(data)

        return data

    # -------------------------------
    # 연도별 정순 크롤링
    # -------------------------------
    def crawl_years(
        self,
        years: Iterable[int],
        headless: bool = False,
        refresh_years: Optional[Set[int]] = None,
        year_missing_threshold: int = 10,   # 🔽 10으로 낮춤
    ) -> None:
        """
        연도별로 시작 SN(YYYY00001)부터 1씩 증가하며 수집.
        미존재 SN이 연속으로 year_missing_threshold 회 나오면 해당 연도 종료.
        - refresh_years에 포함된 연도는 기존 CSV에 있어도 재수집.
        """
        try:
            self.setup_driver(headless=headless)

            # 정규화
            years = list(sorted(set(years)))
            if refresh_years is not None and not isinstance(refresh_years, set):
                refresh_years = set(refresh_years)

            print(f"📅 대상 연도: {years}")
            if refresh_years:
                print(f"🔁 재수집 연도: {sorted(refresh_years)}")
            print("=" * 50)

            # 기존 CSV 중복 회피 집합
            existing_sns: Set[str] = set()
            if os.path.exists(self.csv_path):
                existing_df = pd.read_csv(self.csv_path, dtype={'clncTestSn': str})
                existing_sns = set(existing_df['clncTestSn'].astype(str).values)
                print(f"📂 기존 파일에서 {len(existing_sns)}개 항목 확인")

            success_count = fail_count = skip_count = 0
            processed_in_year = 0

            for year in years:
                start_sn = year * 100000 + 1
                end_sn_exclusive = (year + 1) * 100000  # 이 값은 포함하지 않음
                print(f"\n🗓️ {year}년 크롤링 시작: SN {start_sn} → < {end_sn_exclusive} (정순)")
                consecutive_missing = 0
                processed_in_year = 0

                sn = start_sn
                while sn < end_sn_exclusive:
                    # 스킵 판단
                    if not (refresh_years and year in refresh_years) and (str(sn) in existing_sns):
                        skip_count += 1
                        if skip_count % 200 == 0:
                            print(f"⏭️ 누적 스킵 {skip_count}개 (최근 스킵 SN: {sn})")
                        sn += 1
                        continue

                    # 진행 로그(과다 출력 방지)
                    if processed_in_year % 100 == 0:
                        print(f"[{year}] 진행 SN: {sn} (연속 미존재: {consecutive_missing}/{year_missing_threshold})")

                    data = self.extract_detail_data(sn)

                    if data:
                        self.all_data.append(data)
                        success_count += 1
                        processed_in_year += 1
                        consecutive_missing = 0
                        # 100개마다 백업
                        if success_count % 100 == 0:
                            self.save_backup(success_count)
                    else:
                        fail_count += 1
                        processed_in_year += 1
                        consecutive_missing += 1

                        # 연속 미존재 임계 도달 → 해당 연도 종료
                        if consecutive_missing >= year_missing_threshold:
                            print(f"🧭 {year}년 종료 추정: 연속 미존재 {consecutive_missing}회 (마지막 시도 SN: {sn})")
                            break

                        # 안정성: 10회 연속 실패마다 드라이버 리셋
                        if consecutive_missing % 10 == 0:
                            print("🔧 안정화: 드라이버 재시작 (연속 실패 누적)")
                            try:
                                self.driver.quit()
                            except Exception:
                                pass
                            time.sleep(2)
                            self.setup_driver(headless=headless)

                    # 500개 처리마다 드라이버 재시작(메모리 관리)
                    if processed_in_year > 0 and processed_in_year % 500 == 0:
                        print("🧹 500개 처리 - 드라이버 재시작(메모리 정리)")
                        try:
                            self.driver.quit()
                        except Exception:
                            pass
                        time.sleep(3)
                        self.setup_driver(headless=headless)

                    # 서버 부하 방지
                    import random
                    time.sleep(random.uniform(1.5, 4.0))

                    sn += 1

                # 연도 요약
                print(f"✅ {year}년 완료(추정) | 누적 성공:{success_count}, 실패:{fail_count}, 스킵:{skip_count}")

            # 전체 요약
            print("\n" + "=" * 50)
            print("📊 전체 크롤링 완료")
            print(f"  - 총 성공: {success_count}")
            print(f"  - 총 실패: {fail_count}")
            print(f"  - 총 스킵: {skip_count}")

        except KeyboardInterrupt:
            print("\n⚠️ 사용자가 중단했습니다.")
        except Exception as e:
            print(f"❌ 크롤링 중 오류: {e}")
        finally:
            if self.all_data:
                try:
                    self.save_final()
                except Exception as e:
                    print(f"❌ 최종 저장 실패: {e}")
            if self.driver:
                try:
                    self.driver.quit()
                except Exception:
                    pass
                print("🔚 드라이버 종료")

    # -------------------------------
    # 저장/검증
    # -------------------------------
    def save_backup(self, count: int) -> None:
        """중간 백업 저장"""
        backup_file = f"backup_1c_{count}.csv"
        df = pd.DataFrame(self.all_data)
        df.to_csv(backup_file, index=False, encoding='utf-8-sig')
        print(f"💾 백업 저장: {backup_file}")

    def save_final(self) -> Optional[str]:
        """최종 CSV 저장 (기존 데이터와 병합)"""
        if not self.all_data:
            print("⚠️ 저장할 새 데이터가 없습니다")
            return None

        new_df = pd.DataFrame(self.all_data)
        if os.path.exists(self.csv_path):
            existing_df = pd.read_csv(self.csv_path, dtype={'clncTestSn': str})
            combined_df = pd.concat([new_df, existing_df], ignore_index=True)
        else:
            combined_df = new_df

        combined_df.drop_duplicates(subset='clncTestSn', keep='first', inplace=True)
        combined_df['clncTestSn_int'] = pd.to_numeric(combined_df['clncTestSn'], errors='coerce')
        combined_df.sort_values(by='clncTestSn_int', ascending=True, inplace=True, na_position='last')  # 정순 저장
        combined_df.drop(columns=['clncTestSn_int'], inplace=True)

        combined_df.to_csv(self.csv_path, index=False, encoding='utf-8-sig')
        print(f"\n✅ 최종 저장 완료: {self.csv_path}")
        print(f"📊 총 {len(combined_df)}개 항목")

        try:
            max_sn = pd.to_numeric(combined_df['clncTestSn'], errors='coerce').max()
            if pd.notna(max_sn):
                with open('last_clnc_test_sn.txt', 'w') as f:
                    f.write(str(int(max_sn)))
                print(f"🔢 최대 clncTestSn: {int(max_sn)} (last_clnc_test_sn.txt에 저장)")
        except Exception as e:
            print(f"⚠️ 최대 SN 저장 실패: {e}")

        return self.csv_path

    def verify_csv(self, filename: str) -> bool:
        """CSV 파일 검증"""
        try:
            df = pd.read_csv(filename, dtype={'clncTestSn': str})
            print("\n📋 CSV 검증 결과:")
            print(f"  - 총 행 수: {len(df)}")
            print(f"  - 총 컬럼 수: {len(df.columns)}")
            print(f"  - 첫 5행 clncTestSn: {', '.join(df['clncTestSn'].head(5))}")
            return True
        except Exception as e:
            print(f"❌ CSV 검증 실패: {e}")
            return False

    # -------------------------------
    # 파싱 헬퍼들
    # -------------------------------
    def _get_text_safe(self, el) -> str:
        try:
            t = el.text.strip()
            return re.sub(r"\s+", " ", t)
        except Exception:
            return ""

    def _find_first_text(self, candidates) -> str:
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

    def _extract_from_txt_groups(self, data: dict, groups_selectors) -> None:
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

    def _extract_from_tables(self, data: dict, table_selectors) -> None:
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

    def _extract_from_definition_lists(self, data: dict, dl_selectors) -> None:
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
        """'실시기관' 탭을 여는 안전한 방법: 클릭 → 실패 시 JS 강제 노출"""
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
        """테이블 기반으로 실시기관/담당자 형태를 최대치까지 수집"""
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

        # caption에 '실시기관' 포함 테이블 백업 스캔
        try:
            tables = self.driver.find_elements(By.TAG_NAME, "table")
            for tb in tables:
                cap_text = ""
                try:
                    cap_text = tb.find_element(By.TAG_NAME, "caption").text
                except Exception:
                    pass
                if "실시기관" in cap_text:
                    next_idx = _scan_tables(tb, next_idx)
                    if next_idx > max_rows:
                        return
        except Exception:
            pass


# =========================================================
# 메인
# =========================================================
def main():
    """메인 실행 함수"""
    print("🚀 임상시험 연도별 정순 크롤링 시작")
    print(f"📅 실행 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 50)

    crawler = ClinicalTrialCrawler()

    try:
        # ✅ 2019~2024 연도만 정순으로 수집 (해당 연도는 기존 CSV에 있어도 재수집)
        years = range(2021, 2023)          # 2019,2020,...,2024
        refresh = set(range(2021, 2023))   # 재수집 대상 연도
        # 운영 시 headless=True 권장, 미존재 임계치는 10 (필요시 튜닝)
        crawler.crawl_years(
            years=years,
            headless=False,
            refresh_years=refresh,
            year_missing_threshold=10
        )

        # 검증
        if os.path.exists(crawler.csv_path):
            crawler.verify_csv(crawler.csv_path)

    except Exception as e:
        print(f"\n❌ 실행 오류: {e}")
        if crawler.all_data:
            crawler.save_final()


if __name__ == "__main__":
    main()
