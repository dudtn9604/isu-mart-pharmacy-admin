"""
네이버 키워드 트렌드 검증 + 연관 검색어 모듈

[핵심 목적]
홈쇼핑모아에서 발견된 개별 키워드의 네이버 검색량 트렌드를 검증하고,
연관 검색어를 통해 "진짜 뜨고 있는 키워드"를 발굴한다.

[기존과 다른 점]
- 기존(trend_api.py): 미리 정의된 18개 카테고리 그룹의 트렌드를 추적
- 신규(이 모듈): 개별 키워드 단위로 네이버 검색 트렌드를 실시간 검증
                + 연관 검색어 확장 탐색
"""

import json
import time
import urllib.parse
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
import requests

from trend_config import (
    NAVER_CLIENT_ID,
    NAVER_CLIENT_SECRET,
    TREND_DATA_DIR,
)

BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / TREND_DATA_DIR

DATALAB_URL = "https://openapi.naver.com/v1/datalab/search"
SUGGEST_URL = "https://ac.search.naver.com/nx/ac"


def is_api_configured():
    return bool(NAVER_CLIENT_ID) and bool(NAVER_CLIENT_SECRET)


def ensure_data_dir():
    DATA_DIR.mkdir(exist_ok=True)


# ──────────────────────────────────────
# 1. 개별 키워드 네이버 검색 트렌드 조회
# ──────────────────────────────────────

def _call_datalab(keyword_groups, start_date, end_date, time_unit="month"):
    """네이버 Datalab API 단일 호출"""
    if not is_api_configured():
        return None

    headers = {
        "X-Naver-Client-Id": NAVER_CLIENT_ID,
        "X-Naver-Client-Secret": NAVER_CLIENT_SECRET,
        "Content-Type": "application/json",
    }
    body = {
        "startDate": start_date,
        "endDate": end_date,
        "timeUnit": time_unit,
        "keywordGroups": keyword_groups,
    }
    try:
        resp = requests.post(DATALAB_URL, headers=headers, json=body, timeout=10)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        print("[Datalab API 오류] {}".format(e))
        return None


def fetch_keyword_trend(
    keyword: str,
    synonyms: Optional[List[str]] = None,
    months_back: int = 6,
) -> Optional[pd.DataFrame]:
    """
    단일 키워드(+동의어)의 네이버 검색량 트렌드를 조회

    키워드를 하나의 그룹으로 쿼리 → 자기 자신의 시계열 패턴을 확인.
    가장 높은 시점=100 기준 상대값 반환.

    Args:
        keyword: 메인 키워드
        synonyms: 동의어 리스트 (같은 그룹으로 묶어서 검색량 합산)
        months_back: 조회 기간 (개월)

    Returns:
        DataFrame[period, ratio, keyword]
    """
    # 캐시 체크
    ensure_data_dir()
    cache_key = urllib.parse.quote(keyword.lower(), safe="")
    cache_path = DATA_DIR / "kw_{}.json".format(cache_key)

    if cache_path.exists():
        try:
            mtime = datetime.fromtimestamp(cache_path.stat().st_mtime)
            if (datetime.now() - mtime) < timedelta(hours=24):
                with open(cache_path, "r", encoding="utf-8") as f:
                    cached = json.load(f)
                return pd.DataFrame(cached["data"])
        except Exception:
            pass

    if not is_api_configured():
        return None

    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=months_back * 30)).strftime("%Y-%m-%d")

    kw_list = [keyword]
    if synonyms:
        kw_list = list(set(kw_list + synonyms))

    group = {"groupName": keyword, "keywords": kw_list[:20]}  # API max 20
    result = _call_datalab([group], start_date, end_date, "month")

    if result is None or "results" not in result:
        return None

    rows = []
    for item in result.get("results", []):
        for d in item.get("data", []):
            rows.append({
                "period": d["period"],
                "ratio": d["ratio"],
                "keyword": keyword,
            })

    if not rows:
        return None

    df = pd.DataFrame(rows)

    # 캐시 저장
    cache_data = {
        "fetched_at": datetime.now().isoformat(),
        "keyword": keyword,
        "data": rows,
    }
    with open(cache_path, "w", encoding="utf-8") as f:
        json.dump(cache_data, f, ensure_ascii=False, indent=2)

    return df


def fetch_multiple_keyword_trends(
    keywords_with_synonyms: Dict[str, List[str]],
    months_back: int = 6,
) -> Dict[str, pd.DataFrame]:
    """
    여러 키워드의 트렌드를 개별 조회 (캐시 활용, 적절한 딜레이)

    Args:
        keywords_with_synonyms: {"키워드": ["동의어1", "동의어2", ...]}

    Returns:
        {"키워드": DataFrame, ...}
    """
    results = {}

    for keyword, synonyms in keywords_with_synonyms.items():
        df = fetch_keyword_trend(keyword, synonyms, months_back)
        if df is not None and not df.empty:
            results[keyword] = df
        time.sleep(0.3)  # 네이버 API 레이트 리밋

    return results


def analyze_keyword_trend(trend_df: pd.DataFrame) -> dict:
    """
    단일 키워드의 트렌드 DataFrame → 트렌드 지표 계산

    Returns:
        {
            keyword, latest_ratio, avg_recent_3m, growth_rate,
            trend_direction, momentum_score, trend_data
        }
    """
    if trend_df is None or trend_df.empty:
        return {
            "keyword": "",
            "latest_ratio": 0,
            "avg_recent_3m": 0,
            "growth_rate": 0,
            "trend_direction": "데이터없음",
            "momentum_score": 0,
            "trend_data": [],
        }

    keyword = trend_df["keyword"].iloc[0]

    # 현재 월 제외 (불완전 데이터)
    current_month = datetime.now().strftime("%Y-%m")
    clean = trend_df[~trend_df["period"].str.startswith(current_month)].copy()
    if clean.empty:
        clean = trend_df.copy()

    clean = clean.sort_values("period")
    ratios = clean["ratio"].values

    latest = float(ratios[-1]) if len(ratios) > 0 else 0
    avg_recent_3m = float(ratios[-3:].mean()) if len(ratios) >= 3 else float(ratios.mean())

    if len(ratios) >= 4:
        avg_prev = float(ratios[:-3].mean()) if len(ratios) > 3 else float(ratios[0])
        growth = ((avg_recent_3m - avg_prev) / avg_prev * 100) if avg_prev > 0 else 0
    else:
        avg_prev = 0
        growth = 0

    # 트렌드 방향 판정
    if growth >= 15:
        direction = "급상승"
    elif growth >= 5:
        direction = "상승"
    elif growth <= -15:
        direction = "급하락"
    elif growth <= -5:
        direction = "하락"
    else:
        direction = "유지"

    # 모멘텀 점수 (0~100)
    volume_score = min(latest / 100 * 50, 50) if latest > 0 else 0
    growth_score = min(max(growth, 0) / 30 * 50, 50)
    momentum = round(volume_score + growth_score, 1)

    return {
        "keyword": keyword,
        "latest_ratio": round(latest, 1),
        "avg_recent_3m": round(avg_recent_3m, 1),
        "growth_rate": round(growth, 1),
        "trend_direction": direction,
        "momentum_score": momentum,
        "trend_data": clean[["period", "ratio"]].to_dict("records"),
    }


# ──────────────────────────────────────
# 2. 네이버 연관 검색어 (자동완성 API)
# ──────────────────────────────────────

MOBILE_AC_URL = "https://mac.search.naver.com/mobile/ac"


def fetch_related_keywords(keyword: str, max_results: int = 10) -> List[str]:
    """
    네이버 자동완성 API로 연관 검색어 수집 (모바일 AC 엔드포인트)

    Args:
        keyword: 검색 키워드
        max_results: 최대 결과 수

    Returns:
        연관 키워드 리스트
    """
    params = {
        "q": keyword,
        "st": "100",
        "r_format": "json",
        "r_enc": "UTF-8",
        "q_enc": "UTF-8",
    }

    try:
        resp = requests.get(
            MOBILE_AC_URL, params=params,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                ),
                "Referer": "https://search.naver.com/",
            },
            timeout=5,
        )
        data = resp.json()

        items = data.get("items", [])
        related = []

        for item_group in items:
            if not isinstance(item_group, list):
                continue
            for item in item_group:
                term = None
                if isinstance(item, list) and len(item) > 0:
                    term = str(item[0]).strip()
                elif isinstance(item, str):
                    term = item.strip()

                if term and term.lower() != keyword.lower() and len(term) >= 2:
                    related.append(term)

        # 중복 제거 (순서 유지)
        seen = set()
        unique = []
        for r in related:
            rl = r.lower()
            if rl not in seen:
                seen.add(rl)
                unique.append(r)

        return unique[:max_results]

    except Exception as e:
        print("[연관검색어 오류] {}: {}".format(keyword, e))
        return []


def batch_fetch_related_keywords(
    keywords: List[str],
    max_per_keyword: int = 8,
) -> Dict[str, List[str]]:
    """
    여러 키워드의 연관 검색어를 배치 수집 (캐시 포함)

    Returns:
        {"키워드": ["연관1", "연관2", ...], ...}
    """
    ensure_data_dir()
    cache_path = DATA_DIR / "related_keywords_cache.json"

    # 캐시 로드
    cached = {}
    if cache_path.exists():
        try:
            mtime = datetime.fromtimestamp(cache_path.stat().st_mtime)
            if (datetime.now() - mtime) < timedelta(hours=24):
                with open(cache_path, "r", encoding="utf-8") as f:
                    cached = json.load(f).get("data", {})
        except Exception:
            pass

    results = {}
    for kw in keywords:
        if kw in cached:
            results[kw] = cached[kw]
            continue

        related = fetch_related_keywords(kw, max_per_keyword)
        results[kw] = related
        time.sleep(0.15)

    # 캐시 저장 (기존 + 새로운 결과 병합)
    merged = {**cached, **results}
    cache_data = {"fetched_at": datetime.now().isoformat(), "data": merged}
    with open(cache_path, "w", encoding="utf-8") as f:
        json.dump(cache_data, f, ensure_ascii=False, indent=2)

    return results


# ──────────────────────────────────────
# 3. 건강 키워드 필터
# ──────────────────────────────────────

HEALTH_SIGNAL_WORDS = {
    "영양제", "비타민", "유산균", "건강", "효능", "추천", "성분", "먹는",
    "약국", "약사", "복용", "부작용", "보충제", "건기식", "건강기능",
    "면역", "관절", "수면", "다이어트", "탈모", "피부", "장건강",
    "갱년기", "혈당", "혈압", "눈건강", "오메가", "콜라겐", "프로바이오",
    "마그네슘", "아연", "칼슘", "철분", "비오틴", "루테인", "밀크씨슬",
    "글루타치온", "코엔자임", "홍삼", "프로폴리스", "보스웰리아",
    "식품", "원료", "기능식품",
}


def is_health_related(keyword: str) -> bool:
    """키워드가 건강/영양 관련인지 간단 판정"""
    kw_lower = keyword.lower()
    for signal in HEALTH_SIGNAL_WORDS:
        if signal in kw_lower:
            return True
    return False


def filter_health_related_keywords(keywords: List[str]) -> List[str]:
    """건강/영양 관련 키워드만 필터링"""
    return [kw for kw in keywords if is_health_related(kw)]


# ──────────────────────────────────────
# 4. 데모 데이터 (API 미설정 시)
# ──────────────────────────────────────

def generate_demo_keyword_trends(keywords: List[str]) -> Dict[str, pd.DataFrame]:
    """API 키 없을 때 사용할 데모 트렌드 데이터"""
    import numpy as np
    np.random.seed(42)

    periods = pd.date_range(
        start=(datetime.now() - timedelta(days=180)),
        periods=6, freq="MS",
    ).strftime("%Y-%m-01").tolist()

    results = {}
    for i, kw in enumerate(keywords):
        np.random.seed(42 + i)
        base = np.random.uniform(30, 80)
        trend_slope = np.random.uniform(-2, 5)
        noise = np.random.normal(0, 3, len(periods))

        rows = []
        for j, period in enumerate(periods):
            ratio = base + trend_slope * j + noise[j]
            ratio = max(5, min(100, ratio))
            rows.append({
                "period": period,
                "ratio": round(ratio, 1),
                "keyword": kw,
            })

        results[kw] = pd.DataFrame(rows)

    return results


def generate_demo_related_keywords(keywords: List[str]) -> Dict[str, List[str]]:
    """API 미설정 시 데모 연관 검색어"""
    demo_related = {
        "프로바이오틱스": ["프로바이오틱스 추천", "여성유산균", "장건강 유산균", "모유유산균", "프로바이오틱스 효능"],
        "유산균": ["유산균 추천 약국", "질유산균", "아이유산균", "장용성유산균", "유산균 효능"],
        "비타민d": ["비타민D 추천", "비타민D3 5000IU", "비타민D 효능", "비타민D 부족증상", "비타민D 약국"],
        "콜라겐": ["콜라겐 추천", "먹는콜라겐", "저분자콜라겐", "피쉬콜라겐", "콜라겐 효능"],
        "오메가3": ["오메가3 추천", "알티지오메가3", "식물성오메가3", "오메가3 효능", "크릴오일"],
        "루테인": ["루테인 추천", "루테인지아잔틴", "눈영양제 추천", "루테인 효능", "아스타잔틴"],
        "밀크씨슬": ["밀크씨슬 추천", "실리마린", "간영양제 추천", "밀크씨슬 효능", "간건강"],
        "마그네슘": ["마그네슘 추천", "마그네슘 효능", "마그네슘 부작용", "산화마그네슘", "킬레이트마그네슘"],
        "글루타치온": ["글루타치온 추천", "먹는글루타치온", "글루타치온 효능", "환원형글루타치온", "미백영양제"],
        "보스웰리아": ["보스웰리아 추천", "관절영양제", "보스웰리아 효능", "MSM", "관절건강"],
    }

    results = {}
    for kw in keywords:
        kw_lower = kw.lower()
        if kw_lower in demo_related:
            results[kw] = demo_related[kw_lower]
        else:
            results[kw] = [
                "{} 추천".format(kw),
                "{} 효능".format(kw),
                "{} 부작용".format(kw),
            ]

    return results


# ──────────────────────────────────────
# 테스트
# ──────────────────────────────────────

if __name__ == "__main__":
    print("=" * 60)
    print("네이버 키워드 트렌드 API 테스트")
    print("=" * 60)

    test_keywords = ["유산균", "비타민D", "콜라겐", "오메가3", "글루타치온"]

    if is_api_configured():
        print("\n[1] 개별 키워드 트렌드 조회")
        for kw in test_keywords[:3]:
            df = fetch_keyword_trend(kw)
            if df is not None:
                metrics = analyze_keyword_trend(df)
                print("  {} | {} | 성장:{:+.1f}% | 모멘텀:{:.1f}".format(
                    kw, metrics["trend_direction"], metrics["growth_rate"],
                    metrics["momentum_score"],
                ))
            time.sleep(0.5)

        print("\n[2] 연관 검색어 수집")
        for kw in test_keywords[:3]:
            related = fetch_related_keywords(kw, max_results=5)
            print("  '{}' → {}".format(kw, related))
            time.sleep(0.3)
    else:
        print("API 키 미설정 → 데모 데이터 사용")
        demo_trends = generate_demo_keyword_trends(test_keywords)
        for kw, df in demo_trends.items():
            metrics = analyze_keyword_trend(df)
            print("  {} | {} | 성장:{:+.1f}% | 모멘텀:{:.1f}".format(
                kw, metrics["trend_direction"], metrics["growth_rate"],
                metrics["momentum_score"],
            ))

        demo_related = generate_demo_related_keywords(test_keywords)
        for kw, related in demo_related.items():
            print("  '{}' → {}".format(kw, related[:3]))
