"""
약국 화장품 매니저 전용 분석 모듈
- 일별/주간/월간 판매 성과 분석
- SKU 랭킹, 객단가, 시간대별 패턴
- 퇴출 후보 SKU 감지
"""

import pandas as pd
import numpy as np
from typing import Dict, Optional, Set
from datetime import timedelta


# ──────────────────────────────────────────────
# 헬퍼
# ──────────────────────────────────────────────

def get_cosmetics_product_ids(products: pd.DataFrame) -> Set:
    """약국 화장품 세부분류의 product_id 세트 반환"""
    if products.empty or "erp_subcategory" not in products.columns:
        return set()
    cos = products[products["erp_subcategory"] == "약국 화장품"]
    return set(cos["id"].tolist())


def _filter_cosmetics(items: pd.DataFrame, cos_ids: Set) -> pd.DataFrame:
    """order_items에서 약국 화장품만 필터링"""
    if items.empty or not cos_ids:
        return pd.DataFrame()
    return items[items["product_id"].isin(cos_ids)].copy()


# ──────────────────────────────────────────────
# 일별 트렌드 (전체 기간)
# ──────────────────────────────────────────────

def _build_daily_trend(cos_items: pd.DataFrame) -> pd.DataFrame:
    """일별 매출/건수/객단가/수량 시계열"""
    if cos_items.empty:
        return pd.DataFrame()
    daily = cos_items.groupby("order_date").agg(
        매출=("total_price", "sum"),
        건수=("order_id", "nunique"),
        수량=("quantity", "sum"),
        상품종류=("product_name", "nunique"),
    ).reset_index()
    daily["객단가"] = (daily["매출"] / daily["건수"].replace(0, 1)).round(0)
    daily["인당수량"] = (daily["수량"] / daily["건수"].replace(0, 1)).round(1)
    daily["요일"] = daily["order_date"].dt.day_name()
    daily["date_str"] = daily["order_date"].dt.strftime("%m/%d(%a)")
    return daily.sort_values("order_date")


# ──────────────────────────────────────────────
# 일일 리포트
# ──────────────────────────────────────────────

def _daily_report(
    cos_items: pd.DataFrame,
    daily_trend: pd.DataFrame,
    target_date: pd.Timestamp,
) -> Dict:
    """특정 날짜의 일일 성과 + 전일/전주동요일 비교"""
    result = {
        "date": target_date,
        "date_str": target_date.strftime("%m/%d(%a)"),
    }

    today_data = daily_trend[daily_trend["order_date"] == target_date]
    yesterday = target_date - timedelta(days=1)
    last_week_same = target_date - timedelta(days=7)

    yesterday_data = daily_trend[daily_trend["order_date"] == yesterday]
    lastweek_data = daily_trend[daily_trend["order_date"] == last_week_same]

    for label, df in [("today", today_data), ("yesterday", yesterday_data), ("lastweek", lastweek_data)]:
        if not df.empty:
            row = df.iloc[0]
            result[label] = {
                "매출": row["매출"],
                "건수": row["건수"],
                "객단가": row["객단가"],
                "수량": row["수량"],
                "상품종류": row["상품종류"],
            }
        else:
            result[label] = {"매출": 0, "건수": 0, "객단가": 0, "수량": 0, "상품종류": 0}

    # 오늘의 SKU별 판매 TOP
    today_items = cos_items[cos_items["order_date"] == target_date]
    if not today_items.empty:
        sku_today = today_items.groupby("product_name").agg(
            매출=("total_price", "sum"),
            수량=("quantity", "sum"),
        ).sort_values("매출", ascending=False).reset_index()
        result["sku_top"] = sku_today
    else:
        result["sku_top"] = pd.DataFrame()

    # 오늘의 시간대별 패턴
    if not today_items.empty and "order_datetime_kst" in today_items.columns:
        ti = today_items.copy()
        ti["hour"] = pd.to_datetime(ti["order_datetime_kst"]).dt.hour
        hourly = ti.groupby("hour").agg(
            매출=("total_price", "sum"),
            건수=("order_id", "nunique"),
        ).reset_index()
        result["hourly"] = hourly
    else:
        result["hourly"] = pd.DataFrame()

    return result


# ──────────────────────────────────────────────
# 주간 리포트
# ──────────────────────────────────────────────

def _weekly_report(
    cos_items: pd.DataFrame,
    daily_trend: pd.DataFrame,
    target_date: pd.Timestamp,
) -> Dict:
    """target_date가 속한 주(월~일) vs 전주 비교"""
    # 이번 주 월~일
    weekday = target_date.weekday()  # 0=Mon
    week_start = target_date - timedelta(days=weekday)
    week_end = week_start + timedelta(days=6)

    prev_start = week_start - timedelta(days=7)
    prev_end = week_end - timedelta(days=7)

    this_week = daily_trend[
        (daily_trend["order_date"] >= week_start) & (daily_trend["order_date"] <= week_end)
    ]
    prev_week = daily_trend[
        (daily_trend["order_date"] >= prev_start) & (daily_trend["order_date"] <= prev_end)
    ]

    def _summarize(df):
        if df.empty:
            return {"매출": 0, "건수": 0, "객단가": 0, "수량": 0, "일수": 0}
        return {
            "매출": df["매출"].sum(),
            "건수": df["건수"].sum(),
            "객단가": (df["매출"].sum() / max(df["건수"].sum(), 1)),
            "수량": df["수량"].sum(),
            "일수": len(df),
        }

    result = {
        "this_week": _summarize(this_week),
        "prev_week": _summarize(prev_week),
        "this_week_daily": this_week,
        "prev_week_daily": prev_week,
        "week_range": f"{week_start.strftime('%m/%d')}~{week_end.strftime('%m/%d')}",
        "prev_range": f"{prev_start.strftime('%m/%d')}~{prev_end.strftime('%m/%d')}",
    }

    # 이번 주 SKU 랭킹
    week_items = cos_items[
        (cos_items["order_date"] >= week_start) & (cos_items["order_date"] <= week_end)
    ]
    if not week_items.empty:
        sku_week = week_items.groupby("product_name").agg(
            매출=("total_price", "sum"),
            수량=("quantity", "sum"),
        ).sort_values("매출", ascending=False).reset_index()
        result["sku_ranking"] = sku_week
    else:
        result["sku_ranking"] = pd.DataFrame()

    return result


# ──────────────────────────────────────────────
# 월간 리포트
# ──────────────────────────────────────────────

def _monthly_report(
    cos_items: pd.DataFrame,
    daily_trend: pd.DataFrame,
    all_cos_product_names: set,
    year_month: str,   # "2026-02"
) -> Dict:
    """월간 종합 리포트"""
    # 이번 달 / 전월
    ym_parts = year_month.split("-")
    y, m = int(ym_parts[0]), int(ym_parts[1])
    if m == 1:
        prev_ym = f"{y-1}-12"
    else:
        prev_ym = f"{y}-{m-1:02d}"

    daily_trend["ym"] = daily_trend["order_date"].dt.strftime("%Y-%m")
    this_month = daily_trend[daily_trend["ym"] == year_month]
    prev_month = daily_trend[daily_trend["ym"] == prev_ym]

    def _summarize(df):
        if df.empty:
            return {"매출": 0, "건수": 0, "객단가": 0, "수량": 0, "일수": 0, "일평균매출": 0}
        total_rev = df["매출"].sum()
        total_cnt = df["건수"].sum()
        return {
            "매출": total_rev,
            "건수": total_cnt,
            "객단가": total_rev / max(total_cnt, 1),
            "수량": df["수량"].sum(),
            "일수": len(df),
            "일평균매출": total_rev / max(len(df), 1),
        }

    result = {
        "this_month": _summarize(this_month),
        "prev_month": _summarize(prev_month),
        "this_month_daily": this_month,
        "year_month": year_month,
        "prev_ym": prev_ym,
    }

    # 이번 달 SKU 랭킹
    cos_items["ym"] = cos_items["order_date"].dt.strftime("%Y-%m")
    month_items = cos_items[cos_items["ym"] == year_month]
    prev_items = cos_items[cos_items["ym"] == prev_ym]

    if not month_items.empty:
        sku_month = month_items.groupby("product_name").agg(
            매출=("total_price", "sum"),
            수량=("quantity", "sum"),
            건수=("order_id", "nunique"),
        ).sort_values("매출", ascending=False).reset_index()
        sku_month["순위"] = range(1, len(sku_month) + 1)
        result["sku_top10"] = sku_month.head(10)
        result["sku_bottom10"] = sku_month.tail(10).sort_values("매출", ascending=True)
        result["sku_full"] = sku_month
    else:
        result["sku_top10"] = pd.DataFrame()
        result["sku_bottom10"] = pd.DataFrame()
        result["sku_full"] = pd.DataFrame()

    # 판매 0인 SKU (퇴출 후보)
    sold_names = set(month_items["product_name"].unique()) if not month_items.empty else set()
    zero_sales = all_cos_product_names - sold_names
    result["zero_sales_skus"] = sorted(zero_sales)
    result["zero_sales_count"] = len(zero_sales)
    result["total_sku_count"] = len(all_cos_product_names)
    result["sold_sku_count"] = len(sold_names)

    # 요일별 패턴
    if not this_month.empty:
        dow_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
        dow_kr = {"Monday": "월", "Tuesday": "화", "Wednesday": "수",
                  "Thursday": "목", "Friday": "금", "Saturday": "토", "Sunday": "일"}
        dow = this_month.groupby("요일").agg(
            일평균매출=("매출", "mean"),
            일평균건수=("건수", "mean"),
        ).reset_index()
        dow["요일순서"] = dow["요일"].map({d: i for i, d in enumerate(dow_order)})
        dow = dow.sort_values("요일순서")
        dow["요일_kr"] = dow["요일"].map(dow_kr)
        result["day_of_week"] = dow
    else:
        result["day_of_week"] = pd.DataFrame()

    # 시간대별 패턴 (월간)
    if not month_items.empty and "order_datetime_kst" in month_items.columns:
        mi = month_items.copy()
        mi["hour"] = pd.to_datetime(mi["order_datetime_kst"]).dt.hour
        n_days = max(result["this_month"]["일수"], 1)
        hourly_m = mi.groupby("hour").agg(
            총매출=("total_price", "sum"),
            총건수=("order_id", "nunique"),
        ).reset_index()
        hourly_m["일평균매출"] = (hourly_m["총매출"] / n_days).round(0)
        hourly_m["일평균건수"] = (hourly_m["총건수"] / n_days).round(1)
        result["hourly"] = hourly_m
    else:
        result["hourly"] = pd.DataFrame()

    # 객단가 분포 (가격대별)
    if not month_items.empty:
        order_totals = month_items.groupby("order_id")["total_price"].sum().reset_index()
        order_totals.columns = ["order_id", "주문금액"]
        bins = [0, 10000, 20000, 30000, 50000, 100000, float("inf")]
        labels = ["~1만", "1~2만", "2~3만", "3~5만", "5~10만", "10만+"]
        order_totals["가격대"] = pd.cut(order_totals["주문금액"], bins=bins, labels=labels)
        price_dist = order_totals["가격대"].value_counts().sort_index().reset_index()
        price_dist.columns = ["가격대", "건수"]
        result["price_distribution"] = price_dist
    else:
        result["price_distribution"] = pd.DataFrame()

    return result


# ──────────────────────────────────────────────
# 메인 분석 함수
# ──────────────────────────────────────────────

def get_available_months(data: Dict, products: pd.DataFrame) -> list:
    """데이터에서 약국 화장품 판매가 있는 월 목록 반환 (최신순)"""
    cos_ids = get_cosmetics_product_ids(products)
    items = data.get("order_items", pd.DataFrame())
    cos_items = _filter_cosmetics(items, cos_ids)
    if cos_items.empty:
        return []
    months = sorted(cos_items["order_date"].dt.strftime("%Y-%m").unique(), reverse=True)
    return list(months)


def run_cosmetics_analysis(
    data: Dict,
    products: pd.DataFrame,
    reference_date: Optional[str] = None,
    year_month_override: Optional[str] = None,
) -> Dict:
    """
    약국 화장품 종합 분석

    Args:
        data: {"order_items": pd.DataFrame}
        products: 상품 마스터 (erp_subcategory 포함)
        reference_date: 기준일 (없으면 데이터 최신일)
        year_month_override: 월간 리포트 기준월 오버라이드 ("2026-02" 형태)

    Returns:
        daily_trend, daily_report, weekly_report, monthly_report
    """
    cos_ids = get_cosmetics_product_ids(products)
    items = data.get("order_items", pd.DataFrame())
    cos_items = _filter_cosmetics(items, cos_ids)

    if cos_items.empty:
        return {"error": "약국 화장품 판매 데이터가 없습니다."}

    # 기준일 결정
    if reference_date:
        ref = pd.Timestamp(reference_date)
    else:
        ref = cos_items["order_date"].max()

    # 전체 약국 화장품 상품명 세트
    cos_products = products[products["erp_subcategory"] == "약국 화장품"]
    all_cos_names = set(cos_products["name"].tolist())

    # 일별 트렌드
    daily_trend = _build_daily_trend(cos_items)

    # 월간 리포트 기준월: 오버라이드 > 기준일 월
    year_month = year_month_override if year_month_override else ref.strftime("%Y-%m")

    return {
        "daily_trend": daily_trend,
        "daily_report": _daily_report(cos_items, daily_trend, ref),
        "weekly_report": _weekly_report(cos_items, daily_trend, ref),
        "monthly_report": _monthly_report(cos_items, daily_trend, all_cos_names, year_month),
        "reference_date": ref,
        "year_month": year_month,
        "total_products": len(all_cos_names),
        "cos_items": cos_items,
    }
