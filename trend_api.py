"""
트렌드 분석 모듈 - 네이버 Datalab API 연동
시장 트렌드를 넓게 탐색하고, 현재 SKU에 없는 것을 발견 → SKU 추가 제안

[핵심 로직]
1. 네이버 Datalab으로 의약품/건강기능식품 검색 트렌드 수집
2. 트렌드 상승 키워드 식별
3. 현재 SKU와 대조하여 "빠져 있는" 트렌드 발견
4. 구체적인 SKU 추가 제안 생성
"""

import json
import os
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
import requests

from trend_config import (
    API_BATCH_SIZE,
    API_CALL_DELAY,
    NAVER_CLIENT_ID,
    NAVER_CLIENT_SECRET,
    TREND_CACHE_HOURS,
    TREND_DATA_DIR,
    TREND_KEYWORD_GROUPS,
)

from sku_enrichment import enriched_keyword_match, enriched_keyword_check

BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / TREND_DATA_DIR

# ──────────────────────────────────────
# 유틸리티
# ──────────────────────────────────────


def is_api_configured() -> bool:
    return bool(NAVER_CLIENT_ID) and bool(NAVER_CLIENT_SECRET)


def ensure_data_dir():
    DATA_DIR.mkdir(exist_ok=True)


def get_cache_path(group_key: str) -> Path:
    return DATA_DIR / "trend_{}.json".format(group_key)


def is_cache_valid(group_key: str) -> bool:
    cache_path = get_cache_path(group_key)
    if not cache_path.exists():
        return False
    mtime = datetime.fromtimestamp(cache_path.stat().st_mtime)
    return (datetime.now() - mtime) < timedelta(hours=TREND_CACHE_HOURS)


# ──────────────────────────────────────
# 네이버 Datalab API 호출
# ──────────────────────────────────────

DATALAB_URL = "https://openapi.naver.com/v1/datalab/search"


def call_naver_datalab(keyword_groups: List[Dict], start_date: str, end_date: str, time_unit: str = "month") -> Optional[Dict]:
    """네이버 Datalab 검색어 트렌드 API 호출"""
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
    except requests.exceptions.RequestException as e:
        print("[API 오류] {}".format(e))
        return None


# ──────────────────────────────────────
# 개별 키워드 그룹 트렌드 수집
# ──────────────────────────────────────


def fetch_category_trend(group_key: str, months_back: int = 12, force_refresh: bool = False) -> Optional[pd.DataFrame]:
    """특정 키워드 그룹의 검색 트렌드를 수집 (캐시 지원)"""
    if group_key not in TREND_KEYWORD_GROUPS:
        return None

    ensure_data_dir()

    if not force_refresh and is_cache_valid(group_key):
        cache_path = get_cache_path(group_key)
        with open(cache_path, "r", encoding="utf-8") as f:
            cached = json.load(f)
        return pd.DataFrame(cached["data"])

    group_info = TREND_KEYWORD_GROUPS[group_key]
    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=months_back * 30)).strftime("%Y-%m-%d")

    keyword_group = {
        "groupName": group_info["display_name"],
        "keywords": group_info["keywords"],
    }

    result = call_naver_datalab([keyword_group], start_date, end_date, time_unit="month")
    if result is None or "results" not in result:
        return None

    data_rows = []
    for item in result["results"]:
        for d in item.get("data", []):
            data_rows.append({
                "period": d["period"],
                "ratio": d["ratio"],
                "group_key": group_key,
                "display_name": group_info["display_name"],
            })

    df = pd.DataFrame(data_rows)

    cache_data = {
        "fetched_at": datetime.now().isoformat(),
        "group_key": group_key,
        "data": data_rows,
    }
    cache_path = get_cache_path(group_key)
    with open(cache_path, "w", encoding="utf-8") as f:
        json.dump(cache_data, f, ensure_ascii=False, indent=2)

    return df


# ──────────────────────────────────────
# 전체 트렌드 수집
# ──────────────────────────────────────


def fetch_all_trends(months_back: int = 12, force_refresh: bool = False) -> pd.DataFrame:
    """모든 키워드 그룹의 트렌드를 수집"""
    all_dfs = []
    errors = []

    for group_key in TREND_KEYWORD_GROUPS:
        df = fetch_category_trend(group_key, months_back, force_refresh)
        if df is not None and len(df) > 0:
            all_dfs.append(df)
        else:
            errors.append(group_key)

        if force_refresh or not is_cache_valid(group_key):
            time.sleep(API_CALL_DELAY)

    if errors:
        print("[경고] 수집 실패: {}".format(errors))

    if all_dfs:
        return pd.concat(all_dfs, ignore_index=True)
    else:
        return pd.DataFrame(columns=["period", "ratio", "group_key", "display_name"])


# ──────────────────────────────────────
# 트렌드 지표 계산
# ──────────────────────────────────────


def calculate_trend_metrics(trend_df: pd.DataFrame) -> pd.DataFrame:
    """
    각 키워드 그룹의 트렌드 지표를 계산
    - 현재 월(불완전 데이터)은 자동 제외
    """
    if trend_df.empty:
        return pd.DataFrame()

    # 현재 월 제외 (불완전 데이터)
    current_month = datetime.now().strftime("%Y-%m")
    clean = trend_df[~trend_df["period"].str.startswith(current_month)].copy()
    if clean.empty:
        clean = trend_df.copy()

    metrics_list = []

    for group_key, gdf in clean.groupby("group_key"):
        gdf = gdf.sort_values("period")
        display_name = gdf["display_name"].iloc[0]
        ratios = gdf["ratio"].values
        related_cat = TREND_KEYWORD_GROUPS.get(group_key, {}).get("related_category")

        latest = ratios[-1] if len(ratios) > 0 else 0
        avg_3m = ratios[-3:].mean() if len(ratios) >= 3 else ratios.mean()
        avg_6m = ratios[-6:].mean() if len(ratios) >= 6 else ratios.mean()
        avg_all = ratios.mean()

        # 성장률 계산
        if len(ratios) >= 4:
            recent_3m = ratios[-3:].mean()
            prev_3m = ratios[-6:-3].mean() if len(ratios) >= 6 else ratios[:-3].mean()
            growth_3m = ((recent_3m - prev_3m) / prev_3m * 100) if prev_3m > 0 else 0
        else:
            growth_3m = 0

        if len(ratios) >= 7:
            recent_6m = ratios[-6:].mean()
            prev_6m = ratios[:-6].mean() if len(ratios) > 6 else ratios[0]
            growth_6m = ((recent_6m - prev_6m) / prev_6m * 100) if prev_6m > 0 else 0
        else:
            growth_6m = 0

        # 트렌드 방향 판정
        if growth_3m >= 10:
            direction = "급상승"
        elif growth_3m >= 3:
            direction = "상승"
        elif growth_3m <= -10:
            direction = "급하락"
        elif growth_3m <= -3:
            direction = "하락"
        else:
            direction = "유지"

        # 변동성
        volatility = ratios.std() if len(ratios) > 1 else 0

        # 모멘텀 점수: 최근 검색량 + 성장세 종합 (0~100)
        # 검색량 기여(50%) + 성장률 기여(50%)
        volume_score = min(latest / 100 * 50, 50) if latest > 0 else 0
        growth_score = min(max(growth_3m, 0) / 30 * 50, 50)
        momentum = round(volume_score + growth_score, 1)

        metrics_list.append({
            "group_key": group_key,
            "display_name": display_name,
            "related_category": related_cat,
            "latest_ratio": round(latest, 1),
            "avg_ratio_3m": round(avg_3m, 1),
            "growth_rate_3m": round(growth_3m, 1),
            "growth_rate_6m": round(growth_6m, 1),
            "trend_direction": direction,
            "volatility": round(volatility, 2),
            "momentum_score": momentum,
        })

    result = pd.DataFrame(metrics_list)
    return result.sort_values("momentum_score", ascending=False).reset_index(drop=True)


# ──────────────────────────────────────
# 핵심: SKU 추가 제안 생성
# ──────────────────────────────────────


def generate_sku_recommendations(trend_metrics: pd.DataFrame, sku_df: pd.DataFrame) -> pd.DataFrame:
    """
    트렌드 데이터와 현재 SKU를 대조하여 구체적인 SKU 추가 제안 생성

    [로직]
    1. 트렌드 상승 중인 키워드 그룹 식별
    2. 해당 키워드가 현재 SKU 상품명에 포함되어 있는지 검사
    3. 매칭률이 낮으면 = 트렌드는 뜨는데 우리가 안 갖고 있음 → SKU 추가 제안

    Args:
        trend_metrics: calculate_trend_metrics 결과
        sku_df: load_data 결과 (상품명, 카테고리, 매출 등 포함)

    Returns:
        SKU 추가 제안 DataFrame
    """
    if trend_metrics.empty or sku_df.empty:
        return pd.DataFrame()

    recommendations = []

    for _, row in trend_metrics.iterrows():
        group_key = row["group_key"]
        group_info = TREND_KEYWORD_GROUPS.get(group_key, {})
        keywords = group_info.get("keywords", [])
        related_cat = row.get("related_category")

        if not keywords:
            continue

        # 키워드별 현재 SKU 매칭 검사 (동의어 + 성분 매핑 적용)
        matched_keywords = []
        unmatched_keywords = []

        for kw in keywords:
            # enriched_keyword_match: 동의어 + 성분 매핑 포함 매칭
            matched_products = enriched_keyword_match(kw, sku_df)

            if len(matched_products) > 0:
                matched_keywords.append({
                    "keyword": kw,
                    "sku_count": len(matched_products),
                    "total_sales": matched_products["매출합계"].sum() if "매출합계" in matched_products.columns else 0,
                })
            else:
                unmatched_keywords.append(kw)

        total_kw = len(keywords)
        matched_count = len(matched_keywords)
        coverage = matched_count / total_kw * 100 if total_kw > 0 else 0

        # SKU 커버리지가 낮고 트렌드가 상승/유지 → 제안 생성
        direction = row["trend_direction"]
        growth = row["growth_rate_3m"]
        momentum = row["momentum_score"]

        # 제안 우선순위 결정
        if direction in ("급상승", "상승") and coverage < 30:
            priority = "높음"
            priority_icon = "🔴"
            reason = "트렌드 {} (3개월 {:+.1f}%) + SKU 커버리지 낮음 ({:.0f}%)".format(direction, growth, coverage)
        elif direction in ("급상승", "상승") and coverage < 60:
            priority = "중간"
            priority_icon = "🟡"
            reason = "트렌드 {} (3개월 {:+.1f}%) + 일부 SKU 보유 ({:.0f}%)".format(direction, growth, coverage)
        elif direction == "유지" and coverage < 20:
            priority = "중간"
            priority_icon = "🟡"
            reason = "안정적 수요 + SKU 커버리지 매우 낮음 ({:.0f}%)".format(coverage)
        elif direction in ("급상승",) and coverage >= 60:
            priority = "낮음"
            priority_icon = "🔵"
            reason = "트렌드 급상승 중이나 이미 SKU 보유 ({:.0f}%)".format(coverage)
        else:
            # 하락 추세이거나 이미 충분히 보유 → 제안 안 함
            priority = None

        if priority is None:
            continue

        # 관련 카테고리 현재 매출 정보
        cat_sales_info = ""
        if related_cat and "카테고리" in sku_df.columns:
            cat_products = sku_df[sku_df["카테고리"] == related_cat]
            if len(cat_products) > 0 and "매출합계" in cat_products.columns:
                cat_total_sales = cat_products["매출합계"].sum()
                cat_avg_sales = cat_products["매출합계"].mean()
                cat_sales_info = "관련 카테고리({}) SKU {}개, 총매출 {:.0f}만원".format(
                    related_cat, len(cat_products), cat_total_sales / 10000
                )

        # 추가 제안 키워드 (상위 5개)
        suggest_keywords = unmatched_keywords[:5]
        suggest_text = ", ".join(suggest_keywords) if suggest_keywords else "기존 SKU 강화 필요"

        recommendations.append({
            "우선순위": "{} {}".format(priority_icon, priority),
            "트렌드 영역": row["display_name"],
            "모멘텀": momentum,
            "3개월 성장률": "{:+.1f}%".format(growth),
            "트렌드 방향": direction,
            "최근 검색량": row["latest_ratio"],
            "보유 키워드": "{}/{}".format(matched_count, total_kw),
            "커버리지": "{:.0f}%".format(coverage),
            "미보유 키워드": suggest_text,
            "제안 근거": reason,
            "관련 카테고리": related_cat or "-",
            "카테고리 현황": cat_sales_info or "-",
            "_priority_sort": {"높음": 0, "중간": 1, "낮음": 2}.get(priority, 3),
            "_momentum": momentum,
        })

    if not recommendations:
        return pd.DataFrame()

    result = pd.DataFrame(recommendations)
    result = result.sort_values(["_priority_sort", "_momentum"], ascending=[True, False])
    result = result.drop(columns=["_priority_sort", "_momentum"]).reset_index(drop=True)
    return result


# ──────────────────────────────────────
# SKU 키워드 매칭 상세 분석
# ──────────────────────────────────────


def analyze_keyword_coverage(group_key: str, sku_df: pd.DataFrame) -> pd.DataFrame:
    """
    특정 트렌드 영역의 키워드별 SKU 매칭 상세 분석

    Returns:
        키워드별 매칭 현황 DataFrame
    """
    group_info = TREND_KEYWORD_GROUPS.get(group_key, {})
    keywords = group_info.get("keywords", [])

    if not keywords or sku_df.empty:
        return pd.DataFrame()

    results = []
    for kw in keywords:
        # enriched_keyword_check: 동의어 + 성분 매핑 적용 상세 분석
        info = enriched_keyword_check(kw, sku_df)
        matched = enriched_keyword_match(kw, sku_df)

        if info["has_match"]:
            total_sales = matched["매출합계"].sum() if "매출합계" in matched.columns else 0
            top_product = matched.sort_values("매출합계", ascending=False).iloc[0]["상품명"] if "매출합계" in matched.columns and len(matched) > 0 else (matched.iloc[0]["상품명"] if len(matched) > 0 else "-")
            status = "보유"
            status_icon = "✅"
            # 매칭 유형 표시 (직접/동의어/성분)
            types = info["match_types"]
            match_detail = []
            if types["direct"] > 0:
                match_detail.append("직접{}".format(types["direct"]))
            if types["synonym"] > 0:
                match_detail.append("동의어{}".format(types["synonym"]))
            if types["ingredient"] > 0:
                match_detail.append("성분{}".format(types["ingredient"]))
            match_info = "({})".format("+".join(match_detail)) if match_detail else ""
        else:
            total_sales = 0
            top_product = "-"
            status = "미보유"
            status_icon = "❌"
            match_info = ""

        results.append({
            "상태": "{} {}".format(status_icon, status),
            "키워드": kw,
            "매칭 SKU 수": info["matched_count"],
            "매칭 유형": match_info,
            "총 매출": total_sales,
            "대표 상품": top_product,
        })

    return pd.DataFrame(results)


# ──────────────────────────────────────
# 데모 데이터 (API 키 없을 때)
# ──────────────────────────────────────


def generate_demo_trend_data() -> pd.DataFrame:
    """API 키 없을 때 사용할 데모 데이터"""
    import numpy as np
    np.random.seed(42)
    periods = pd.date_range(start="2025-04-01", periods=12, freq="MS").strftime("%Y-%m-01").tolist()

    patterns = {
        "핫성분_비타민미네랄": {"base": 65, "trend": 1.5, "season": [0, -3, -5, -2, 3, 5, 3, -2, -3, 5, 8, 10]},
        "핫성분_장건강": {"base": 60, "trend": 2.0, "season": [2, 0, -2, -3, 0, 3, 5, 3, 0, -2, 0, 2]},
        "핫성분_항노화": {"base": 40, "trend": 5.0, "season": [0, 2, 5, 3, 0, -2, 0, 2, 5, 8, 5, 3]},
        "핫성분_콜라겐뷰티": {"base": 55, "trend": 3.5, "season": [5, 8, 10, 8, 3, -2, -5, -3, 2, 5, 3, 0]},
        "핫성분_오메가3지방산": {"base": 50, "trend": 1.0, "season": [0, -2, -3, 0, 2, 3, 2, 0, -2, 0, 2, 3]},
        "고민_수면스트레스": {"base": 55, "trend": 4.0, "season": [0, 2, 5, 3, 0, -2, 0, 2, 5, 8, 5, 3]},
        "고민_다이어트체중": {"base": 60, "trend": 6.0, "season": [3, 5, 8, 5, 3, 0, -3, -5, -3, 0, 3, 5]},
        "고민_혈당혈압": {"base": 45, "trend": 3.0, "season": [0, 2, 3, 2, 0, -2, -3, -2, 0, 2, 3, 2]},
        "고민_면역력": {"base": 50, "trend": 1.5, "season": [-5, -8, -5, 0, 5, 8, 10, 8, 5, 0, -5, -8]},
        "고민_탈모두피": {"base": 50, "trend": 3.0, "season": [3, 5, 8, 5, 3, 0, -3, -5, -3, 0, 3, 5]},
        "신트렌드_간건강": {"base": 45, "trend": 2.5, "season": [0, 0, 2, 3, 2, 0, -2, -3, -2, 0, 2, 3]},
        "신트렌드_갱년기": {"base": 40, "trend": 3.5, "season": [0, 2, 3, 5, 3, 2, 0, -2, 0, 2, 3, 5]},
        "신트렌드_눈건강": {"base": 45, "trend": 2.0, "season": [3, 5, 8, 5, 3, 0, -3, -5, -3, 0, 3, 5]},
        "신트렌드_관절연골": {"base": 45, "trend": 1.5, "season": [0, -2, -3, 0, 2, 3, 2, 0, -2, 0, 2, 3]},
        "신트렌드_어린이성장": {"base": 45, "trend": 2.5, "season": [3, 5, 3, 0, -3, -5, -3, 0, 3, 5, 8, 5]},
        "신트렌드_반려동물건강": {"base": 40, "trend": 3.5, "season": [0, 2, 3, 5, 3, 2, 0, -2, 0, 2, 3, 5]},
        "구매행동_약국추천": {"base": 55, "trend": 2.0, "season": [0, -2, 0, 2, 3, 2, 0, -2, 0, 2, 3, 5]},
        "구매행동_연령대별": {"base": 50, "trend": 1.0, "season": [0, 0, 2, 3, 2, 0, -2, -3, -2, 0, 2, 3]},
    }

    data_rows = []
    for group_key, pattern in patterns.items():
        display_name = TREND_KEYWORD_GROUPS.get(group_key, {}).get("display_name", group_key)
        for i, period in enumerate(periods):
            ratio = pattern["base"] + pattern["trend"] * i + pattern["season"][i] + np.random.normal(0, 2)
            ratio = max(0, min(100, ratio))
            data_rows.append({
                "period": period,
                "ratio": round(ratio, 1),
                "group_key": group_key,
                "display_name": display_name,
            })

    return pd.DataFrame(data_rows)


# ──────────────────────────────────────
# 테스트
# ──────────────────────────────────────
if __name__ == "__main__":
    if is_api_configured():
        print("API 키 확인 OK — 전체 트렌드 수집 중...")
        all_trends = fetch_all_trends(months_back=12)
        metrics = calculate_trend_metrics(all_trends)
        print("\n=== 트렌드 모멘텀 TOP 10 ===")
        for _, r in metrics.head(10).iterrows():
            print("{:5.1f} | {} | {:+.1f}% | {}".format(
                r["momentum_score"], r["display_name"], r["growth_rate_3m"], r["trend_direction"]
            ))
    else:
        print("API 키 미설정 → 데모 데이터 사용")
        demo = generate_demo_trend_data()
        metrics = calculate_trend_metrics(demo)
        print(metrics[["display_name", "momentum_score", "growth_rate_3m", "trend_direction"]].to_string(index=False))
