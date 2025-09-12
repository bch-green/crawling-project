"""
임상시험 시급성 스코어링 (최종버전)
- 정책
  * RFS 제외
  * 모집완료/종료 = 최종 0점 (하드룰)
  * 가중치 시나리오( baseline / speed / risk_avoid / late_stage / auto )
  * 모집 압박(B): 연속형 점수화(계단식 완화)
  * 난이도(C): 희귀/신경·면역/전략적 1상(FIH) 가점(소폭)
  * 기간 누락/이상치 시 보수적 처리(기본 24개월 가정 후 연속형 압박 계산)
  * 경고(warnings) 표출 + 소폭 감쇄(total_adjusted) 지원

- 필수 컬럼
  진행상태, 임상시험 단계, 임상시험 기간, 목표 대상자 수(국내),
  대상질환명, 성별, 나이, 임상시험 승인일자(YYYY-MM-DD)

- 출력
  total_score (원점수, 0~100)
  total_score_adjusted (경고 기반 소폭 감쇄 적용, 선택)
  breakdown (A/B/C/D)
  warnings (리스트)
  mode (적용 가중치 시나리오)
"""

from __future__ import annotations
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Any, Optional, List
from dateutil.relativedelta import relativedelta

# =========================
# 설정: 최대값 & 가중치 시나리오
# =========================

MAXES = {"A": 30, "B": 25, "C": 25, "D": 20}

WEIGHTS: Dict[str, Dict[str, float]] = {
    # 균형형(기본)
    "baseline":   {"A": 0.35, "B": 0.25, "C": 0.20, "D": 0.20},
    # 실행 개입 최적화(모집압박/임박도 ↑)
    "speed":      {"A": 0.20, "B": 0.35, "C": 0.15, "D": 0.30},
    # 모집 실패 예방(난이도 ↑)
    "risk_avoid": {"A": 0.20, "B": 0.25, "C": 0.35, "D": 0.20},
    # 후기단계/상업성 중시(단계/상태 ↑)
    "late_stage": {"A": 0.45, "B": 0.20, "C": 0.15, "D": 0.20},
}

# =========================
# 유틸
# =========================

def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))

def _safe_int(x: Any, default: int = 0) -> int:
    try:
        return int(str(x).replace(",", "").strip())
    except Exception:
        return default

def parse_period(period_str: str):
    """'YYYY년 M월 ~ YYYY년 M월' → (start_date, end_date) or (None, None)"""
    m = re.findall(r'(\d{4})년\s*(\d{1,2})월', str(period_str or ""))
    if len(m) >= 2:
        s = datetime(int(m[0][0]), int(m[0][1]), 1)
        e = datetime(int(m[1][0]), int(m[1][1]), 1)
        return s, e
    return None, None

def months_between(period_str: str) -> int:
    s, e = parse_period(period_str)
    if s and e:
        return (e.year - s.year) * 12 + (e.month - s.month)
    return -1  # 파싱 실패

# =========================
# 경고(무결성) 생성
# =========================

def build_warnings(row: Dict[str, Any], now: Optional[datetime] = None) -> List[str]:
    """타임스탬프 없이도 잡아낼 수 있는 데이터/논리 이상 경고."""
    now = now or datetime.now()
    warns: List[str] = []

    status = (row.get("진행상태") or "").strip()
    start, end = parse_period(row.get("임상시험 기간"))

    # 1) 기간 무결성
    if not start or not end or (end <= start):
        warns.append("PERIOD_INVALID")

    # 2) 상태-기간 논리
    if start and end:
        if status == "모집완료" and now < start:
            warns.append("IMPOSSIBLE_STATUS_RECRUITED_BEFORE_START")
        if status == "승인완료" and now >= start:
            warns.append("STATUS_STUCK_POST_START")   # 시작했는데 아직 승인완료
        if status == "모집중" and now > end:
            warns.append("RECRUITMENT_OVERDUE_AFTER_END")

    # 3) 목표 인원 파싱
    tgt = str(row.get("목표 대상자 수(국내)", "")).replace(",", "")
    if not re.search(r'\d', tgt):
        warns.append("TARGET_MISSING_OR_NONNUMERIC")

    # 4) 질환 정보밀도
    disease = str(row.get("대상질환명", "")).strip()
    if len(disease) < 6 or len(disease.split()) == 1:
        warns.append("DISEASE_GENERIC_INFO")

    return warns

def apply_adjustment(total_raw: float, warnings: List[str]) -> float:
    """
    경고 개수에 따라 소폭 감쇄(최대 15%).
    - 경고 1개당 3% 감쇄, 최대 5개 = 15%
    """
    attenuation = min(0.15, 0.03 * len(warnings))
    return round(total_raw * (1.0 - attenuation), 1)

# =========================
# 점수 계산: A/B/C/D
# =========================

# --- A: 상태/중요도 (0~30)
def calculate_status_and_importance(status: str, phase: str) -> int:
    score = 0
    status = (status or "").strip()
    phase = (phase or "")

    if status == "모집중":
        score += 15
    elif status == "승인완료":
        score += 10

    if "3상" in phase:
        score += 15
    elif "2상" in phase:
        score += 10
    elif "1상" in phase:
        score += 5
    elif "생동" in phase:
        score += 3

    return int(_clamp(score, 0, MAXES["A"]))

# --- B: 모집 압박 (0~25) - 연속형 매핑
def pressure_score_continuous(spm: float) -> float:
    """
    subjects per month를 0~10 구간으로 선형 매핑:
    spm=0 → 5점, spm=10 → 25점, 그 사이 선형 보간 (클램프)
    """
    spm_c = _clamp(spm, 0, 10)
    return 5 + (spm_c / 10.0) * 20

def calculate_recruitment_pressure(target_subjects: Any, period_str: str) -> int:
    target = _safe_int(target_subjects, 0)
    months = months_between(period_str)

    if months <= 0:
        # 기간 파싱 실패/0개월: 보수적 24개월 가정, 연속형 점수 산출
        spm_proxy = target / 24.0
        return int(round(pressure_score_continuous(spm_proxy)))

    spm = target / months if months > 0 else 0.0
    return int(round(pressure_score_continuous(spm)))

# --- C: 모집 난이도 (0~25) + 보너스(최대 +5 내)
RARE_KWS = ["희귀", "rare", "orphan"]
NEURO_IMMUNE_KWS = ["신경", "neurolog", "면역", "immun"]
FIH_KWS = ["first-in-human", "fih", "최초 인체", "초회 인체"]

def difficulty_base(disease_name: str) -> int:
    dn = (disease_name or "").lower()
    if any(k in dn for k in ["암", "종양", "malign", "cancer", "oncology", "희귀"]):
        return 15
    if any(k in dn for k in ["신경", "neurolog", "면역", "immun"]):
        return 10
    return 5

def difficulty_bonus(disease_name: str, phase: str) -> int:
    dn = (disease_name or "").lower()
    bonus = 0
    if any(k in dn for k in RARE_KWS):
        bonus += 3
    if any(k in dn for k in NEURO_IMMUNE_KWS):
        bonus += 2
    if "1상" in str(phase) and any(k in dn for k in FIH_KWS):
        bonus += 5  # 전략적 1상(FIH) 상향
    return min(bonus, 5)

def calculate_recruitment_difficulty(disease_name: str, gender: str, age_str: str, phase: str) -> int:
    score = difficulty_base(disease_name)
    score += difficulty_bonus(disease_name, phase)

    g = (gender or "")
    has_m, has_f = ("남" in g), ("여" in g)
    if (has_m and not has_f) or (has_f and not has_m):
        score += 5  # 단일 성별 제한

    nums = [int(n) for n in re.findall(r'\d+', str(age_str or ""))]
    if len(nums) >= 2:
        lo, hi = nums[0], nums[1]
        if hi - lo <= 30:
            score += 5  # 좁은 연령대

    return int(_clamp(score, 0, MAXES["C"]))

# --- D: 시간 민감도 (0~20)
def calculate_time_sensitivity(
    status: str,
    period_str: str,
    approval_date_str: str,
    current_date: Optional[datetime] = None,
) -> int:
    now = current_date or datetime.now()

    start_date, end_date = parse_period(period_str)
    if not start_date or not end_date:
        return 5  # 안전 기본값

    status = (status or "").strip()

    if status == "모집중":
        total_days = max(1, (end_date - start_date).days)
        elapsed = (now - start_date).days
        progress_ratio = elapsed / total_days
        if progress_ratio >= 0.5:
            return 20
        elif progress_ratio >= 0.25:
            return 15
        else:
            return 10

    if status == "승인완료":
        if now > start_date:
            # 시작일 지났는데 여전히 승인완료 → 지연
            try:
                approval_date = datetime.strptime((approval_date_str or "").strip(), "%Y-%m-%d")
            except Exception:
                approval_date = None

            if approval_date:
                rd = relativedelta(now, approval_date)
                months_since_approval = rd.years * 12 + rd.months
            else:
                months_since_approval = 0

            if months_since_approval > 6:
                return 15
            elif 3 <= months_since_approval <= 6:
                return 10
            else:
                return 5
        else:
            # 시작 전: 임박도
            days_to_start = (start_date - now).days
            if days_to_start <= 30:
                return 15
            elif days_to_start <= 90:
                return 10
            else:
                return 5

    return 0

# =========================
# 통합 계산
# =========================

@dataclass
class ScoreBreakdown:
    A: int
    B: int
    C: int
    D: int

def _combine_with_weights(bd: ScoreBreakdown, weights: Dict[str, float]) -> float:
    a = bd.A / MAXES["A"] if MAXES["A"] else 0
    b = bd.B / MAXES["B"] if MAXES["B"] else 0
    c = bd.C / MAXES["C"] if MAXES["C"] else 0
    d = bd.D / MAXES["D"] if MAXES["D"] else 0
    score = 100.0 * (weights["A"] * a + weights["B"] * b + weights["C"] * c + weights["D"] * d)
    return float(_clamp(score, 0, 100))

def calculate_total_urgency_score(
    trial_data: Dict[str, Any],
    mode: str = "baseline",
    current_date: Optional[datetime] = None,
    with_adjustment: bool = True,
) -> Dict[str, Any]:
    """
    단일 과제의 최종 시급성 점수 계산.
    - mode: "baseline" | "speed" | "risk_avoid" | "late_stage" | "auto"
            "auto" → 진행상태 '승인완료'면 late_stage, '모집중'이면 speed
    - 모집완료/종료: 하드룰 0점 (경고는 함께 반환)
    - with_adjustment: warnings 기반 소폭 감쇄 적용 여부
    """
    status = (trial_data.get("진행상태") or "").strip()
    warnings = build_warnings(trial_data, now=current_date)

    # 하드룰: 모집완료/종료는 즉시 0
    if status in {"모집완료", "종료"}:
        out = {
            "total_score": 0.0,
            "total_score_adjusted": 0.0 if with_adjustment else 0.0,
            "breakdown": {"1_상태_및_중요도": 0, "2_모집_압박_강도": 0, "3_모집_난이도": 0, "4_시간적_민감도": 0},
            "warnings": warnings,
            "mode": mode if mode != "auto" else "speed",  # 의미 없음이지만 키 유지
        }
        return out

    # A~D 산출
    A = calculate_status_and_importance(status, trial_data.get("임상시험 단계", ""))
    B = calculate_recruitment_pressure(trial_data.get("목표 대상자 수(국내)", 0), trial_data.get("임상시험 기간", ""))
    C = calculate_recruitment_difficulty(
        trial_data.get("대상질환명", ""), trial_data.get("성별", ""), trial_data.get("나이", ""), trial_data.get("임상시험 단계", "")
    )
    D = calculate_time_sensitivity(status, trial_data.get("임상시험 기간", ""), trial_data.get("임상시험 승인일자", ""), current_date=current_date)
    bd = ScoreBreakdown(A, B, C, D)

    # 상태 기반 자동 시나리오 전환
    selected_mode = mode
    if mode == "auto":
        selected_mode = "late_stage" if status == "승인완료" else "speed"

    weights = WEIGHTS.get(selected_mode, WEIGHTS["baseline"])
    total_raw = round(_combine_with_weights(bd, weights), 1)
    total_adj = apply_adjustment(total_raw, warnings) if with_adjustment else total_raw

    return {
        "total_score": total_raw,
        "total_score_adjusted": total_adj,
        "breakdown": {
            "1_상태_및_중요도": bd.A,
            "2_모집_압박_강도": bd.B,
            "3_모집_난이도": bd.C,
            "4_시간적_민감도": bd.D,
        },
        "warnings": warnings,
        "mode": selected_mode,
    }

# =========================
# DataFrame 일괄 계산 헬퍼 (선택)
# =========================

def score_dataframe(df, mode: str = "baseline", current_date: Optional[datetime] = None, with_adjustment: bool = True):
    """
    pandas.DataFrame 각 행에 점수 계산 → A/B/C/D, total_score, total_score_adjusted, warnings, mode 컬럼 추가
    """
    import pandas as pd
    rows = []
    for _, row in df.iterrows():
        data = {
            "진행상태": row.get("진행상태", ""),
            "임상시험 단계": row.get("임상시험 단계", ""),
            "임상시험 기간": row.get("임상시험 기간", ""),
            "목표 대상자 수(국내)": row.get("목표 대상자 수(국내)", 0),
            "대상질환명": row.get("대상질환명", ""),
            "성별": row.get("성별", ""),
            "나이": row.get("나이", ""),
            "임상시험 승인일자": row.get("임상시험 승인일자", ""),
        }
        res = calculate_total_urgency_score(data, mode=mode, current_date=current_date, with_adjustment=with_adjustment)
        rows.append({
            "A": res["breakdown"]["1_상태_및_중요도"],
            "B": res["breakdown"]["2_모집_압박_강도"],
            "C": res["breakdown"]["3_모집_난이도"],
            "D": res["breakdown"]["4_시간적_민감도"],
            "total_score": res["total_score"],
            "total_score_adjusted": res["total_score_adjusted"],
            "warnings": ";".join(res["warnings"]),
            "mode": res["mode"],
        })
    import pandas as pd
    aux = pd.DataFrame(rows, index=df.index)
    return pd.concat([df, aux], axis=1)

# =========================
# 예시 실행
# =========================
if __name__ == "__main__":
    example = {
        "진행상태": "승인완료",
        "임상시험 단계": "1상",
        "임상시험 기간": "2025년 01월 ~ 2026년 06월",
        "목표 대상자 수(국내)": "120",
        "대상질환명": "희귀 신경면역 질환, first-in-human",
        "성별": "■남 ■여",
        "나이": "18세 이상~65세 미만",
        "임상시험 승인일자": "2025-01-24",
    }
    res = calculate_total_urgency_score(example, mode="auto", with_adjustment=True)
    print("🔬 임상시험 시급성 스코어")
    print("- total_score:", res["total_score"])
    print("- total_score_adjusted:", res["total_score_adjusted"])
    print("- breakdown:", res["breakdown"])
    print("- warnings:", res["warnings"])
    print("- mode:", res["mode"])
