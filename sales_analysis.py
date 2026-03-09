"""
매출 분석 모듈 (Supabase ERP 기반)
이수마트약국의 실시간 매출 데이터를 분석하여 인사이트를 제공

[데이터 소스]
- toss_orders: 주문 헤더 + 임베디드 order_items (JSON)
- sale_cost_records: 매출원가 기록 (FIFO 원가, 매출총이익)
- products: 상품 마스터 (erp_category, erp_subcategory)

[주요 분석 기능]
1. 일별/주별/월별 매출 트렌드
2. 카테고리별 매출 분석
3. 상품별 매출 순위 (Top/Bottom)
4. 매출총이익(GP) 분석
5. 시간대별 판매 패턴
6. 요일별 판매 패턴
"""

import pandas as pd
import numpy as np
from typing import Optional, Dict, Tuple
from datetime import datetime, timedelta

from supabase_client import (
    fetch_products,
    fetch_orders,
    fetch_sale_cost_records,
    flatten_order_items,
    is_supabase_configured,
)


# ──────────────────────────────────────
# 데이터 로드 및 전처리
# ──────────────────────────────────────

def load_sales_data(
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
) -> Dict[str, pd.DataFrame]:
    """
    매출 분석에 필요한 전체 데이터를 로드하고 전처리

    Returns: {
        "products": 상품 마스터,
        "orders": 주문 헤더,
        "order_items": 플래트닝된 주문 상세,
        "cost_records": 매출원가 기록,
        "daily_summary": 일별 매출 요약,
    }
    """
    if not is_supabase_configured():
        return {"error": "Supabase 설정이 필요합니다."}

    # 1. 상품 마스터
    products = fetch_products()

    # 2. 주문 데이터
    orders = fetch_orders(date_from=date_from, date_to=date_to)

    # 3. 주문 아이템 플래트닝 (products 매핑 포함)
    order_items = flatten_order_items(orders, products_df=products)

    # 4. 매출원가 기록
    cost_records = fetch_sale_cost_records(date_from=date_from, date_to=date_to)

    # 5. 일별 매출 요약 (주문 기반, KST)
    daily_summary = pd.DataFrame()
    if not orders.empty:
        orders_copy = orders.copy()
        # UTC → KST (+9시간) 후 날짜 추출
        orders_copy["date"] = (pd.to_datetime(orders_copy["order_date"]) + timedelta(hours=9)).dt.date
        daily_summary = orders_copy.groupby("date").agg(
            주문수=("id", "count"),
            총매출=("actual_amount", "sum"),
            총할인=("discount_amount", "sum"),
            상품수=("item_count", "sum"),
        ).reset_index()
        daily_summary["date"] = pd.to_datetime(daily_summary["date"])

    # 6. 매출원가에 상품 정보 조인
    if not cost_records.empty and not products.empty:
        cost_with_product = cost_records.merge(
            products[["id", "name", "erp_category", "erp_subcategory", "selling_price"]],
            left_on="product_id",
            right_on="id",
            how="left",
            suffixes=("", "_product"),
        )
        cost_records = cost_with_product

    return {
        "products": products,
        "orders": orders,
        "order_items": order_items,
        "cost_records": cost_records,
        "daily_summary": daily_summary,
    }


# ──────────────────────────────────────
# 1. 매출 KPI 계산
# ──────────────────────────────────────

def calculate_kpis(data: Dict[str, pd.DataFrame]) -> Dict:
    """핵심 매출 KPI 계산"""
    orders = data.get("orders", pd.DataFrame())
    order_items = data.get("order_items", pd.DataFrame())
    cost_records = data.get("cost_records", pd.DataFrame())

    if orders.empty:
        return {}

    total_revenue = orders["actual_amount"].sum()
    total_orders = len(orders)
    avg_order_value = total_revenue / total_orders if total_orders > 0 else 0

    # 기간 계산
    order_dates = pd.to_datetime(orders["order_date"])
    date_min = order_dates.min()
    date_max = order_dates.max()
    days = max((date_max - date_min).days, 1)

    daily_avg_revenue = total_revenue / days
    daily_avg_orders = total_orders / days

    # GP (매출총이익) 계산
    total_gp = 0
    gp_rate = 0
    if not cost_records.empty:
        total_gp = cost_records["gross_profit"].sum()
        total_selling = cost_records["selling_price_total"].sum()
        gp_rate = (total_gp / total_selling * 100) if total_selling > 0 else 0

    # 고유 상품 수
    unique_products = 0
    if not order_items.empty:
        unique_products = order_items["product_id"].nunique()

    return {
        "total_revenue": total_revenue,
        "total_orders": total_orders,
        "avg_order_value": avg_order_value,
        "daily_avg_revenue": daily_avg_revenue,
        "daily_avg_orders": daily_avg_orders,
        "total_gp": total_gp,
        "gp_rate": gp_rate,
        "unique_products": unique_products,
        "days": days,
        "date_from": date_min.strftime("%Y-%m-%d"),
        "date_to": date_max.strftime("%Y-%m-%d"),
    }


# ──────────────────────────────────────
# 2. 일별/주별/월별 매출 트렌드
# ──────────────────────────────────────

def analyze_daily_trend(data: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """일별 매출 트렌드"""
    daily = data.get("daily_summary", pd.DataFrame())
    if daily.empty:
        return pd.DataFrame()

    daily = daily.sort_values("date")
    daily["7일이평"] = daily["총매출"].rolling(window=7, min_periods=1).mean()
    return daily


def analyze_weekly_trend(data: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """주별 매출 트렌드"""
    orders = data.get("orders", pd.DataFrame())
    if orders.empty:
        return pd.DataFrame()

    orders_copy = orders.copy()
    orders_copy["week"] = pd.to_datetime(orders_copy["order_date"]).dt.isocalendar().week.astype(int)
    orders_copy["year"] = pd.to_datetime(orders_copy["order_date"]).dt.isocalendar().year.astype(int)
    orders_copy["year_week"] = orders_copy["year"].astype(str) + "-W" + orders_copy["week"].astype(str).str.zfill(2)

    weekly = orders_copy.groupby("year_week").agg(
        주문수=("id", "count"),
        총매출=("actual_amount", "sum"),
    ).reset_index()
    weekly = weekly.sort_values("year_week")
    return weekly


def analyze_monthly_trend(data: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """월별 매출 트렌드"""
    orders = data.get("orders", pd.DataFrame())
    if orders.empty:
        return pd.DataFrame()

    orders_copy = orders.copy()
    orders_copy["month"] = pd.to_datetime(orders_copy["order_date"]).dt.to_period("M").astype(str)

    monthly = orders_copy.groupby("month").agg(
        주문수=("id", "count"),
        총매출=("actual_amount", "sum"),
        평균주문가=("actual_amount", "mean"),
    ).reset_index()
    monthly = monthly.sort_values("month")
    return monthly


# ──────────────────────────────────────
# 3. 카테고리별 매출 분석
# ──────────────────────────────────────

def analyze_category_sales(data: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """카테고리별 매출 분석"""
    order_items = data.get("order_items", pd.DataFrame())
    if order_items.empty:
        return pd.DataFrame()

    cat_summary = order_items.groupby("erp_category").agg(
        판매건수=("quantity", "sum"),
        총매출=("total_price", "sum"),
        상품종류=("product_name", "nunique"),
        주문건수=("order_id", "nunique"),
    ).reset_index()

    cat_summary["SKU당 평균매출"] = (cat_summary["총매출"] / cat_summary["상품종류"]).round(0)
    cat_summary["건당 평균매출"] = (cat_summary["총매출"] / cat_summary["판매건수"]).round(0)
    cat_summary = cat_summary.sort_values("총매출", ascending=False)
    cat_summary["매출비중"] = (cat_summary["총매출"] / cat_summary["총매출"].sum() * 100).round(1)

    return cat_summary


def analyze_category_gp(data: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """카테고리별 매출총이익(GP) 분석"""
    cost_records = data.get("cost_records", pd.DataFrame())
    if cost_records.empty or "erp_category" not in cost_records.columns:
        return pd.DataFrame()

    gp_summary = cost_records.groupby("erp_category").agg(
        총매출=("selling_price_total", "sum"),
        총원가=("fifo_cost_total", "sum"),
        총이익=("gross_profit", "sum"),
        거래건수=("id", "count"),
    ).reset_index()

    gp_summary["GP율"] = (gp_summary["총이익"] / gp_summary["총매출"] * 100).round(1)
    gp_summary = gp_summary.sort_values("총이익", ascending=False)
    gp_summary["이익비중"] = (gp_summary["총이익"] / gp_summary["총이익"].sum() * 100).round(1)

    return gp_summary


# ──────────────────────────────────────
# 4. 상품별 매출 순위
# ──────────────────────────────────────

def analyze_top_products(
    data: Dict[str, pd.DataFrame],
    top_n: int = 30,
) -> pd.DataFrame:
    """매출 TOP 상품"""
    order_items = data.get("order_items", pd.DataFrame())
    if order_items.empty:
        return pd.DataFrame()

    product_summary = order_items.groupby(["product_id", "product_name", "erp_category"]).agg(
        판매수량=("quantity", "sum"),
        총매출=("total_price", "sum"),
        주문건수=("order_id", "nunique"),
    ).reset_index()

    product_summary["건당 평균"] = (product_summary["총매출"] / product_summary["주문건수"]).round(0)
    product_summary = product_summary.sort_values("총매출", ascending=False)

    return product_summary.head(top_n)


def analyze_bottom_products(
    data: Dict[str, pd.DataFrame],
    min_orders: int = 1,
    bottom_n: int = 30,
) -> pd.DataFrame:
    """매출 하위 상품"""
    order_items = data.get("order_items", pd.DataFrame())
    if order_items.empty:
        return pd.DataFrame()

    product_summary = order_items.groupby(["product_id", "product_name", "erp_category"]).agg(
        판매수량=("quantity", "sum"),
        총매출=("total_price", "sum"),
        주문건수=("order_id", "nunique"),
    ).reset_index()

    product_summary = product_summary[product_summary["주문건수"] >= min_orders]
    product_summary = product_summary.sort_values("총매출", ascending=True)

    return product_summary.head(bottom_n)


def analyze_top_gp_products(
    data: Dict[str, pd.DataFrame],
    top_n: int = 30,
) -> pd.DataFrame:
    """매출총이익 TOP 상품"""
    cost_records = data.get("cost_records", pd.DataFrame())
    if cost_records.empty or "name" not in cost_records.columns:
        return pd.DataFrame()

    gp_by_product = cost_records.groupby(["product_id", "name", "erp_category"]).agg(
        판매수량=("quantity_sold", "sum"),
        총매출=("selling_price_total", "sum"),
        총원가=("fifo_cost_total", "sum"),
        총이익=("gross_profit", "sum"),
    ).reset_index()

    gp_by_product["GP율"] = (gp_by_product["총이익"] / gp_by_product["총매출"] * 100).round(1)
    gp_by_product = gp_by_product.sort_values("총이익", ascending=False)

    return gp_by_product.head(top_n)


# ──────────────────────────────────────
# 5. 시간대/요일별 분석
# ──────────────────────────────────────

def analyze_hourly_pattern(data: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """시간대별 판매 패턴 (KST 기준)"""
    orders = data.get("orders", pd.DataFrame())
    if orders.empty:
        return pd.DataFrame()

    orders_copy = orders.copy()
    # UTC → KST (+9시간)
    orders_copy["hour_kst"] = (pd.to_datetime(orders_copy["order_date"]) + timedelta(hours=9)).dt.hour

    hourly = orders_copy.groupby("hour_kst").agg(
        주문수=("id", "count"),
        총매출=("actual_amount", "sum"),
    ).reset_index()
    hourly.rename(columns={"hour_kst": "hour"}, inplace=True)

    hourly["평균매출"] = (hourly["총매출"] / hourly["주문수"]).round(0)
    return hourly


def analyze_weekday_pattern(data: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """요일별 판매 패턴 (KST 기준)"""
    orders = data.get("orders", pd.DataFrame())
    if orders.empty:
        return pd.DataFrame()

    orders_copy = orders.copy()
    # UTC → KST (+9시간)
    kst_dt = pd.to_datetime(orders_copy["order_date"]) + timedelta(hours=9)
    orders_copy["weekday"] = kst_dt.dt.dayofweek
    orders_copy["weekday_name"] = orders_copy["weekday"].map({
        0: "월", 1: "화", 2: "수", 3: "목", 4: "금", 5: "토", 6: "일",
    })

    weekday = orders_copy.groupby(["weekday", "weekday_name"]).agg(
        주문수=("id", "count"),
        총매출=("actual_amount", "sum"),
    ).reset_index().sort_values("weekday")

    # 주 수로 나누어 평균 구하기
    date_range = pd.to_datetime(orders["order_date"])
    weeks = max((date_range.max() - date_range.min()).days / 7, 1)
    weekday["주평균 주문수"] = (weekday["주문수"] / weeks).round(1)
    weekday["주평균 매출"] = (weekday["총매출"] / weeks).round(0)

    return weekday


# ──────────────────────────────────────
# 6. 기간 비교 분석
# ──────────────────────────────────────

def compare_periods(
    data_current: Dict[str, pd.DataFrame],
    data_previous: Dict[str, pd.DataFrame],
) -> Dict:
    """두 기간의 매출 비교"""
    kpi_current = calculate_kpis(data_current)
    kpi_previous = calculate_kpis(data_previous)

    if not kpi_current or not kpi_previous:
        return {}

    def _growth(current, previous):
        if previous == 0:
            return 0
        return ((current - previous) / previous * 100)

    return {
        "current": kpi_current,
        "previous": kpi_previous,
        "revenue_growth": _growth(
            kpi_current["total_revenue"],
            kpi_previous["total_revenue"],
        ),
        "orders_growth": _growth(
            kpi_current["total_orders"],
            kpi_previous["total_orders"],
        ),
        "aov_growth": _growth(
            kpi_current["avg_order_value"],
            kpi_previous["avg_order_value"],
        ),
        "gp_growth": _growth(
            kpi_current["total_gp"],
            kpi_previous["total_gp"],
        ),
    }


# ──────────────────────────────────────
# 7. 카테고리 트렌드 (월별)
# ──────────────────────────────────────

def analyze_category_monthly_trend(data: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """카테고리별 월별 매출 트렌드"""
    order_items = data.get("order_items", pd.DataFrame())
    if order_items.empty:
        return pd.DataFrame()

    items_copy = order_items.copy()
    items_copy["month"] = items_copy["order_date"].dt.to_period("M").astype(str)

    monthly_cat = items_copy.groupby(["month", "erp_category"]).agg(
        총매출=("total_price", "sum"),
    ).reset_index()

    # 피벗으로 카테고리별 월별 매출
    pivot = monthly_cat.pivot_table(
        index="month",
        columns="erp_category",
        values="총매출",
        fill_value=0,
    )

    return pivot


# ──────────────────────────────────────
# 8. 바쁜 날 vs 객단가 높은 날 분석
# ──────────────────────────────────────

def _filter_items(
    data: Dict[str, pd.DataFrame],
    exclude_categories: Optional[list] = None,
    exclude_products: Optional[list] = None,
) -> pd.DataFrame:
    """
    조제, 키인결제 등 특정 카테고리/상품을 제외한 아이템 반환
    """
    order_items = data.get("order_items", pd.DataFrame())
    if order_items.empty:
        return pd.DataFrame()

    items = order_items.copy()
    if exclude_categories:
        items = items[~items["erp_category"].isin(exclude_categories)]
    if exclude_products:
        items = items[~items["product_name"].isin(exclude_products)]
    return items


def _build_daily_item_stats(
    data: Dict[str, pd.DataFrame],
    exclude_categories: Optional[list] = None,
    exclude_products: Optional[list] = None,
) -> pd.DataFrame:
    """
    조제, 키인결제 등을 제외한 일별 주문 통계 생성
    Returns: date, 주문수, 총매출, 객단가
    """
    items = _filter_items(data, exclude_categories, exclude_products)
    if items.empty:
        return pd.DataFrame()

    daily = items.groupby("order_date").agg(
        주문수=("order_id", "nunique"),
        총매출=("total_price", "sum"),
        판매수량=("quantity", "sum"),
    ).reset_index()
    daily["객단가"] = (daily["총매출"] / daily["주문수"]).round(0)
    daily["평균구매수량"] = (daily["판매수량"] / daily["주문수"]).round(1)
    daily["date_str"] = daily["order_date"].dt.strftime("%m/%d(%a)")

    return daily


def _compare_outlier_days(
    items: pd.DataFrame,
    daily: pd.DataFrame,
    target_dates: set,
    normal_dates: set,
    target_label: str,
) -> Dict:
    """
    특이일 vs 보통날 비교 분석 공통 로직

    Args:
        items: 필터링된 주문 아이템
        daily: 일별 통계
        target_dates: 특이일 날짜 set
        normal_dates: 보통날 날짜 set
        target_label: 특이일 라벨 (예: "고객↑", "객단가↑")
    """
    n_target = len(target_dates)
    n_normal = len(normal_dates)

    items_tagged = items.copy()
    items_tagged["day_type"] = items_tagged["order_date"].apply(
        lambda d: target_label if d in target_dates else "보통날"
    )

    # ── 카테고리 비교 (매출 + 건수) ──
    cat_by_type = items_tagged.groupby(["day_type", "erp_category"]).agg(
        총매출=("total_price", "sum"),
        판매수량=("quantity", "sum"),
        주문건수=("order_id", "nunique"),
    ).reset_index()

    t_cat = cat_by_type[cat_by_type["day_type"] == target_label].copy()
    n_cat = cat_by_type[cat_by_type["day_type"] == "보통날"].copy()

    for df, n in [(t_cat, n_target), (n_cat, n_normal)]:
        df["일평균 매출"] = (df["총매출"] / max(n, 1)).round(0)
        df["매출비중"] = (df["총매출"] / df["총매출"].sum() * 100).round(1)
        df["일평균 건수"] = (df["주문건수"] / max(n, 1)).round(1)
        df["건수비중"] = (df["주문건수"] / df["주문건수"].sum() * 100).round(1)

    # 매출 기반 비교
    cat_compare = t_cat[["erp_category", "일평균 매출", "매출비중"]].merge(
        n_cat[["erp_category", "일평균 매출", "매출비중"]],
        on="erp_category", how="outer",
        suffixes=("_특이일", "_보통날"),
    ).fillna(0)
    cat_compare["매출증가율"] = np.where(
        cat_compare["일평균 매출_보통날"] > 0,
        ((cat_compare["일평균 매출_특이일"] - cat_compare["일평균 매출_보통날"])
         / cat_compare["일평균 매출_보통날"] * 100).round(1),
        0,
    )
    cat_compare["비중변화"] = (
        cat_compare["매출비중_특이일"] - cat_compare["매출비중_보통날"]
    ).round(1)
    cat_compare = cat_compare.sort_values("매출증가율", ascending=False)

    # 건수 기반 비교
    cat_compare_count = t_cat[["erp_category", "일평균 건수", "건수비중"]].merge(
        n_cat[["erp_category", "일평균 건수", "건수비중"]],
        on="erp_category", how="outer",
        suffixes=("_특이일", "_보통날"),
    ).fillna(0)
    cat_compare_count["건수증가율"] = np.where(
        cat_compare_count["일평균 건수_보통날"] > 0,
        ((cat_compare_count["일평균 건수_특이일"] - cat_compare_count["일평균 건수_보통날"])
         / cat_compare_count["일평균 건수_보통날"] * 100).round(1),
        0,
    )
    cat_compare_count["비중변화"] = (
        cat_compare_count["건수비중_특이일"] - cat_compare_count["건수비중_보통날"]
    ).round(1)
    cat_compare_count = cat_compare_count.sort_values("건수증가율", ascending=False)

    # ── 상품 비교 ──
    prod_by_type = items_tagged.groupby(
        ["day_type", "product_id", "product_name", "erp_category"]
    ).agg(총매출=("total_price", "sum"), 판매수량=("quantity", "sum")).reset_index()

    t_prods = prod_by_type[prod_by_type["day_type"] == target_label].copy()
    n_prods = prod_by_type[prod_by_type["day_type"] == "보통날"].copy()

    t_prods["일평균 매출"] = (t_prods["총매출"] / max(n_target, 1)).round(0)
    t_prods["일평균 수량"] = (t_prods["판매수량"] / max(n_target, 1)).round(1)
    n_prods["일평균 매출"] = (n_prods["총매출"] / max(n_normal, 1)).round(0)
    n_prods["일평균 수량"] = (n_prods["판매수량"] / max(n_normal, 1)).round(1)

    # 매출 비중 계산 (특이일/보통날 각각의 전체 매출 대비 비중)
    t_total_revenue = t_prods["일평균 매출"].sum()
    n_total_revenue = n_prods["일평균 매출"].sum()
    t_prods["매출비중"] = (t_prods["일평균 매출"] / max(t_total_revenue, 1) * 100).round(2)
    n_prods["매출비중"] = (n_prods["일평균 매출"] / max(n_total_revenue, 1) * 100).round(2)

    prod_compare = t_prods[["product_name", "erp_category", "일평균 매출", "일평균 수량", "매출비중"]].merge(
        n_prods[["product_name", "erp_category", "일평균 매출", "일평균 수량", "매출비중"]],
        on=["product_name", "erp_category"], how="outer",
        suffixes=("_특이일", "_보통날"),
    ).fillna(0)
    prod_compare["비중변화"] = (
        prod_compare["매출비중_특이일"] - prod_compare["매출비중_보통날"]
    ).round(2)

    # 의미 있는 상품만 필터:
    #   1) 보통날에도 판매 실적 있는 상품 (0→1개 노이즈 제거)
    #   2) 비중이 상승한 상품만 (비중변화 > 0)
    # 정렬: 비중변화(%p) 내림차순 → 일일 매출 파이에서 눈에 띄게 올라간 상품 순
    prod_compare_significant = prod_compare[
        (prod_compare["일평균 매출_보통날"] > 0)
        & (prod_compare["비중변화"] > 0)
    ].sort_values("비중변화", ascending=False)

    # ── 시간대 비교 ──
    hourly_compare = pd.DataFrame()
    if "order_datetime_kst" in items_tagged.columns:
        items_tagged["hour"] = pd.to_datetime(
            items_tagged["order_datetime_kst"]
        ).dt.hour
        hourly = items_tagged.groupby(["day_type", "hour"]).agg(
            매출=("total_price", "sum"),
            건수=("order_id", "nunique"),
        ).reset_index()
        t_hourly = hourly[hourly["day_type"] == target_label].copy()
        n_hourly = hourly[hourly["day_type"] == "보통날"].copy()
        t_hourly["일평균 매출"] = (t_hourly["매출"] / max(n_target, 1)).round(0)
        t_hourly["일평균 건수"] = (t_hourly["건수"] / max(n_target, 1)).round(1)
        n_hourly["일평균 매출"] = (n_hourly["매출"] / max(n_normal, 1)).round(0)
        n_hourly["일평균 건수"] = (n_hourly["건수"] / max(n_normal, 1)).round(1)
        hourly_compare = t_hourly[["hour", "일평균 매출", "일평균 건수"]].merge(
            n_hourly[["hour", "일평균 매출", "일평균 건수"]],
            on="hour", how="outer",
            suffixes=("_특이일", "_보통날"),
        ).fillna(0).sort_values("hour")

    # ── 주문당 평균 구매 정보 ──
    order_stats = items_tagged.groupby(["day_type", "order_id"]).agg(
        상품수=("product_id", "count"),
        주문금액=("total_price", "sum"),
    ).reset_index()
    t_orders = order_stats[order_stats["day_type"] == target_label]
    n_orders = order_stats[order_stats["day_type"] == "보통날"]
    basket_compare = {
        "특이일_평균상품수": t_orders["상품수"].mean() if not t_orders.empty else 0,
        "보통날_평균상품수": n_orders["상품수"].mean() if not n_orders.empty else 0,
        "특이일_평균주문금액": t_orders["주문금액"].mean() if not t_orders.empty else 0,
        "보통날_평균주문금액": n_orders["주문금액"].mean() if not n_orders.empty else 0,
    }

    # ── 일별 상세 ──
    target_daily = daily[daily["order_date"].isin(target_dates)].copy()
    normal_daily = daily[daily["order_date"].isin(normal_dates)]

    daily_detail = pd.DataFrame()
    if not target_daily.empty:
        daily_detail = target_daily.sort_values("order_date")[
            ["order_date", "주문수", "객단가", "총매출", "평균구매수량"]
        ].copy()
        daily_detail["날짜"] = daily_detail["order_date"].dt.strftime("%m/%d(%a)")
        daily_detail = daily_detail.rename(columns={
            "주문수": "고객수", "객단가": "객단가(원)", "총매출": "매출(원)"
        })

    # ── 요약 KPI ──
    summary = {
        "n_target": n_target,
        "n_normal": n_normal,
        "target_avg_orders": target_daily["주문수"].mean() if not target_daily.empty else 0,
        "normal_avg_orders": normal_daily["주문수"].mean() if not normal_daily.empty else 0,
        "target_avg_revenue": target_daily["총매출"].mean() if not target_daily.empty else 0,
        "normal_avg_revenue": normal_daily["총매출"].mean() if not normal_daily.empty else 0,
        "target_avg_aov": target_daily["객단가"].mean() if not target_daily.empty else 0,
        "normal_avg_aov": normal_daily["객단가"].mean() if not normal_daily.empty else 0,
        "target_avg_qty": target_daily["평균구매수량"].mean() if not target_daily.empty else 0,
        "normal_avg_qty": normal_daily["평균구매수량"].mean() if not normal_daily.empty else 0,
    }

    return {
        "daily_detail": daily_detail,
        "target_dates": target_dates,
        "normal_dates": normal_dates,
        "category_comparison": cat_compare,
        "category_comparison_count": cat_compare_count,
        "product_comparison": prod_compare_significant.head(30),
        "hourly_comparison": hourly_compare,
        "basket_compare": basket_compare,
        "summary": summary,
    }


def analyze_outlier_days(
    data: Dict[str, pd.DataFrame],
    exclude_categories: Optional[list] = None,
    exclude_products: Optional[list] = None,
    z_threshold: float = 1.0,
    month_filter: Optional[str] = None,
) -> Dict:
    """
    특정 월 데이터에서 고객수/객단가가 '확 띈 날'을 z-score 기반으로 자동 감지하고
    보통날과의 차이를 심층 분석

    Args:
        data: load_sales_data 결과
        exclude_categories: 제외할 카테고리 (예: ["조제"])
        exclude_products: 제외할 상품명 (예: ["키인결제"])
        z_threshold: 특이일 판별 z-score 기준 (기본 1.0)
        month_filter: 특정 월 필터 (예: "2026-02"). None이면 전체 기간
    """
    if exclude_categories is None:
        exclude_categories = ["조제"]
    if exclude_products is None:
        exclude_products = ["키인결제"]

    items = _filter_items(data, exclude_categories, exclude_products)
    if items.empty:
        return {}

    # 특정 월 필터링
    if month_filter:
        items = items[items["order_date"].dt.strftime("%Y-%m") == month_filter]
        if items.empty:
            return {}

    daily = _build_daily_item_stats(data, exclude_categories, exclude_products)
    if daily.empty:
        return {}

    # 특정 월 필터링
    if month_filter:
        daily = daily[daily["order_date"].dt.strftime("%Y-%m") == month_filter]
        if daily.empty:
            return {}

    # z-score 계산
    mean_cust = daily["주문수"].mean()
    std_cust = daily["주문수"].std()
    mean_aov = daily["객단가"].mean()
    std_aov = daily["객단가"].std()

    daily["고객수_z"] = ((daily["주문수"] - mean_cust) / std_cust).round(2) if std_cust > 0 else 0
    daily["객단가_z"] = ((daily["객단가"] - mean_aov) / std_aov).round(2) if std_aov > 0 else 0

    # 특이일 감지
    busy_dates = set(daily[daily["고객수_z"] >= z_threshold]["order_date"].tolist())
    high_aov_dates = set(daily[daily["객단가_z"] >= z_threshold]["order_date"].tolist())
    all_outlier_dates = busy_dates | high_aov_dates
    normal_dates_for_busy = set(daily["order_date"].tolist()) - busy_dates
    normal_dates_for_aov = set(daily["order_date"].tolist()) - high_aov_dates

    # 바쁜 날 분석
    busy_analysis = _compare_outlier_days(
        items, daily, busy_dates, normal_dates_for_busy, "고객↑"
    ) if busy_dates else {}

    # 객단가 높은 날 분석
    aov_analysis = _compare_outlier_days(
        items, daily, high_aov_dates, normal_dates_for_aov, "객단가↑"
    ) if high_aov_dates else {}

    # 전체 일별 통계 (차트용)
    daily_overview = daily[[
        "order_date", "date_str", "주문수", "객단가", "총매출",
        "평균구매수량", "고객수_z", "객단가_z",
    ]].copy()

    return {
        "daily_overview": daily_overview,
        "busy_analysis": busy_analysis,
        "aov_analysis": aov_analysis,
        "stats": {
            "mean_cust": round(mean_cust, 0),
            "std_cust": round(std_cust, 0),
            "mean_aov": round(mean_aov, 0),
            "std_aov": round(std_aov, 0),
            "z_threshold": z_threshold,
            "overlap_dates": sorted([d.strftime("%m/%d") for d in (busy_dates & high_aov_dates)]),
        },
    }


# ──────────────────────────────────────
# 통합 실행 함수
# ──────────────────────────────────────

def run_sales_analysis(
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
) -> Dict:
    """
    전체 매출 분석 실행

    Returns: {
        "data": 원본 데이터,
        "kpis": KPI 딕셔너리,
        "daily_trend": 일별 트렌드,
        "weekly_trend": 주별 트렌드,
        "monthly_trend": 월별 트렌드,
        "category_sales": 카테고리별 매출,
        "category_gp": 카테고리별 GP,
        "top_products": TOP 상품,
        "bottom_products": 하위 상품,
        "top_gp_products": GP TOP 상품,
        "hourly_pattern": 시간대별 패턴,
        "weekday_pattern": 요일별 패턴,
        "category_monthly": 카테고리별 월별 트렌드,
    }
    """
    data = load_sales_data(date_from=date_from, date_to=date_to)

    if "error" in data:
        return data

    return {
        "data": data,
        "kpis": calculate_kpis(data),
        "daily_trend": analyze_daily_trend(data),
        "weekly_trend": analyze_weekly_trend(data),
        "monthly_trend": analyze_monthly_trend(data),
        "category_sales": analyze_category_sales(data),
        "category_gp": analyze_category_gp(data),
        "top_products": analyze_top_products(data, top_n=30),
        "bottom_products": analyze_bottom_products(data, bottom_n=30),
        "top_gp_products": analyze_top_gp_products(data, top_n=30),
        "hourly_pattern": analyze_hourly_pattern(data),
        "weekday_pattern": analyze_weekday_pattern(data),
        "category_monthly": analyze_category_monthly_trend(data),
        "outlier_analysis": analyze_outlier_days(
            data,
            exclude_categories=["조제"],
            exclude_products=["키인결제"],
            z_threshold=1.0,
            month_filter="2026-02",
        ),
    }
