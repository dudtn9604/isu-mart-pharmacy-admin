"""
성과 대시보드 분석 모듈
- 일/주/월 리포트 + 카테고리/상품 비교 함수
- sales_analysis.py 기반 로직 + 신규 함수
"""

import pandas as pd
import numpy as np
from typing import Dict, Optional
from datetime import timedelta

# 제외 카테고리/상품 (조제, 키인결제)
EXCLUDE_CATEGORIES = ["조제"]
EXCLUDE_PRODUCTS = ["키인결제"]


# ──────────────────────────────────────
# 데이터 필터링 헬퍼
# ──────────────────────────────────────

def filter_items(order_items: pd.DataFrame) -> pd.DataFrame:
    """조제/키인결제 제외"""
    if order_items.empty:
        return order_items
    items = order_items.copy()
    items = items[~items["erp_category"].isin(EXCLUDE_CATEGORIES)]
    items = items[~items["product_name"].isin(EXCLUDE_PRODUCTS)]
    return items


# ──────────────────────────────────────
# build_daily_summary: 모든 탭의 기반
# ──────────────────────────────────────

def build_daily_summary(data: Dict) -> pd.DataFrame:
    """
    일별 매출/주문수/객단가/GP 사전 계산
    Returns: DataFrame[date, 매출, 주문수, 객단가, GP, GP율]
    """
    orders = data.get("orders", pd.DataFrame())
    cost_records = data.get("cost_records", pd.DataFrame())
    order_items = data.get("order_items", pd.DataFrame())

    if orders.empty:
        return pd.DataFrame()

    # 주문 기반 일별 요약 (KST)
    oc = orders.copy()
    oc["date"] = (pd.to_datetime(oc["order_date"]).dt.tz_localize(None) + timedelta(hours=9)).dt.normalize()

    daily = oc.groupby("date").agg(
        매출=("actual_amount", "sum"),
        주문수=("id", "count"),
    ).reset_index()
    daily["객단가"] = (daily["매출"] / daily["주문수"].replace(0, 1)).round(0)

    # GP 계산 (cost_records 기반)
    if not cost_records.empty and "sale_date" in cost_records.columns:
        cr = cost_records.copy()
        cr["date"] = pd.to_datetime(cr["sale_date"]).dt.normalize()
        gp_daily = cr.groupby("date").agg(
            GP=("gross_profit", "sum"),
            매출원가기준=("selling_price_total", "sum"),
        ).reset_index()
        gp_daily["GP율"] = (gp_daily["GP"] / gp_daily["매출원가기준"].replace(0, 1) * 100).round(1)
        daily = daily.merge(gp_daily[["date", "GP", "GP율"]], on="date", how="left")
    else:
        daily["GP"] = 0
        daily["GP율"] = 0.0

    daily = daily.fillna(0)
    daily["요일"] = daily["date"].dt.dayofweek.map(
        {0: "월", 1: "화", 2: "수", 3: "목", 4: "금", 5: "토", 6: "일"}
    )
    daily = daily.sort_values("date")
    return daily


# ──────────────────────────────────────
# 일일 리포트
# ──────────────────────────────────────

def daily_report(data: Dict, summary: pd.DataFrame, date: pd.Timestamp) -> Dict:
    """당일 + 전일 + 전주동요일 KPI"""
    date = pd.Timestamp(date).normalize()
    yesterday = date - timedelta(days=1)
    last_week_same = date - timedelta(days=7)

    def _get_kpi(d):
        row = summary[summary["date"] == d]
        if row.empty:
            return {"매출": 0, "주문수": 0, "객단가": 0, "GP": 0, "GP율": 0.0}
        r = row.iloc[0]
        return {
            "매출": r["매출"],
            "주문수": int(r["주문수"]),
            "객단가": r["객단가"],
            "GP": r["GP"],
            "GP율": r["GP율"],
        }

    today_kpi = _get_kpi(date)
    yesterday_kpi = _get_kpi(yesterday)
    lastweek_kpi = _get_kpi(last_week_same)

    return {
        "today": today_kpi,
        "yesterday": yesterday_kpi,
        "lastweek": lastweek_kpi,
        "date": date,
    }


def daily_category_breakdown(data: Dict, date: pd.Timestamp) -> pd.DataFrame:
    """카테고리별 당일 매출/GP"""
    order_items = filter_items(data.get("order_items", pd.DataFrame()))
    cost_records = data.get("cost_records", pd.DataFrame())
    date = pd.Timestamp(date).normalize()

    if order_items.empty:
        return pd.DataFrame()

    day_items = order_items[order_items["order_date"].dt.normalize() == date]
    if day_items.empty:
        return pd.DataFrame()

    cat = day_items.groupby("erp_category").agg(
        매출=("total_price", "sum"),
        판매수량=("quantity", "sum"),
        주문건수=("order_id", "nunique"),
    ).reset_index()
    cat["매출비중"] = (cat["매출"] / cat["매출"].sum() * 100).round(1)
    cat = cat.sort_values("매출", ascending=False)

    # GP 조인
    if not cost_records.empty and "sale_date" in cost_records.columns and "erp_category" in cost_records.columns:
        cr = cost_records.copy()
        cr["date"] = pd.to_datetime(cr["sale_date"]).dt.normalize()
        cr_day = cr[cr["date"] == date]
        if not cr_day.empty:
            gp_cat = cr_day.groupby("erp_category").agg(
                GP=("gross_profit", "sum"),
                원가매출=("selling_price_total", "sum"),
            ).reset_index()
            gp_cat["GP율"] = (gp_cat["GP"] / gp_cat["원가매출"].replace(0, 1) * 100).round(1)
            cat = cat.merge(gp_cat[["erp_category", "GP", "GP율"]], on="erp_category", how="left")

    cat = cat.fillna(0)
    return cat


def daily_hourly_pattern(data: Dict, date: pd.Timestamp) -> pd.DataFrame:
    """시간대별 당일 매출/건수"""
    order_items = filter_items(data.get("order_items", pd.DataFrame()))
    date = pd.Timestamp(date).normalize()

    if order_items.empty:
        return pd.DataFrame()

    day_items = order_items[order_items["order_date"].dt.normalize() == date]
    if day_items.empty:
        return pd.DataFrame()

    day_items = day_items.copy()
    day_items["hour"] = pd.to_datetime(day_items["order_datetime_kst"]).dt.hour

    hourly = day_items.groupby("hour").agg(
        매출=("total_price", "sum"),
        건수=("order_id", "nunique"),
    ).reset_index()
    return hourly


def daily_top_products(data: Dict, date: pd.Timestamp, top_n: int = 10) -> pd.DataFrame:
    """매출 TOP N 상품"""
    order_items = filter_items(data.get("order_items", pd.DataFrame()))
    date = pd.Timestamp(date).normalize()

    if order_items.empty:
        return pd.DataFrame()

    day_items = order_items[order_items["order_date"].dt.normalize() == date]
    if day_items.empty:
        return pd.DataFrame()

    prods = day_items.groupby(["product_name", "erp_category"]).agg(
        매출=("total_price", "sum"),
        수량=("quantity", "sum"),
    ).reset_index().sort_values("매출", ascending=False)
    return prods.head(top_n)


def daily_product_anomalies(data: Dict, date: pd.Timestamp, lookback: int = 14) -> pd.DataFrame:
    """최근 N일 평균 대비 당일 매출 급변 상품 (z-score)"""
    order_items = filter_items(data.get("order_items", pd.DataFrame()))
    date = pd.Timestamp(date).normalize()

    if order_items.empty:
        return pd.DataFrame()

    start = date - timedelta(days=lookback)
    recent = order_items[(order_items["order_date"].dt.normalize() >= start) &
                         (order_items["order_date"].dt.normalize() < date)]
    today = order_items[order_items["order_date"].dt.normalize() == date]

    if recent.empty or today.empty:
        return pd.DataFrame()

    # 상품별 일평균 매출
    recent_daily = recent.groupby(["product_name", recent["order_date"].dt.normalize()]).agg(
        매출=("total_price", "sum"),
    ).reset_index()
    avg_by_prod = recent_daily.groupby("product_name").agg(
        평균매출=("매출", "mean"),
        표준편차=("매출", "std"),
        판매일수=("매출", "count"),
    ).reset_index()
    avg_by_prod["표준편차"] = avg_by_prod["표준편차"].fillna(0)

    today_prod = today.groupby("product_name").agg(
        당일매출=("total_price", "sum"),
    ).reset_index()

    merged = today_prod.merge(avg_by_prod, on="product_name", how="left")
    merged = merged[merged["판매일수"] >= 3]  # 최소 3일 이상 판매 이력

    if merged.empty:
        return pd.DataFrame()

    merged["z_score"] = np.where(
        merged["표준편차"] > 0,
        ((merged["당일매출"] - merged["평균매출"]) / merged["표준편차"]).round(2),
        0,
    )
    merged["변화율"] = ((merged["당일매출"] - merged["평균매출"]) / merged["평균매출"].replace(0, 1) * 100).round(1)

    # 급변 상품만 (|z| >= 1.5 또는 변화율 ±50%)
    anomalies = merged[(merged["z_score"].abs() >= 1.5) | (merged["변화율"].abs() >= 50)]
    anomalies = anomalies.sort_values("z_score", ascending=False)
    return anomalies


# ──────────────────────────────────────
# 주간 리포트
# ──────────────────────────────────────

def _get_week_range(date: pd.Timestamp):
    """월요일~일요일 주간 범위"""
    date = pd.Timestamp(date).normalize()
    weekday = date.dayofweek
    mon = date - timedelta(days=weekday)
    sun = mon + timedelta(days=6)
    return mon, sun


def weekly_report(data: Dict, summary: pd.DataFrame, date: pd.Timestamp) -> Dict:
    """이번주 + 전주 KPI + 일별 상세"""
    this_mon, this_sun = _get_week_range(date)
    last_mon = this_mon - timedelta(days=7)
    last_sun = this_sun - timedelta(days=7)

    this_week = summary[(summary["date"] >= this_mon) & (summary["date"] <= this_sun)]
    last_week = summary[(summary["date"] >= last_mon) & (summary["date"] <= last_sun)]

    def _agg(df):
        if df.empty:
            return {"매출": 0, "주문수": 0, "객단가": 0, "GP": 0, "GP율": 0.0}
        return {
            "매출": df["매출"].sum(),
            "주문수": int(df["주문수"].sum()),
            "객단가": round(df["매출"].sum() / max(df["주문수"].sum(), 1)),
            "GP": df["GP"].sum(),
            "GP율": round(df["GP"].sum() / max(df["매출"].sum(), 1) * 100, 1),
        }

    return {
        "this_week": _agg(this_week),
        "last_week": _agg(last_week),
        "this_week_daily": this_week.copy(),
        "last_week_daily": last_week.copy(),
        "this_mon": this_mon,
        "this_sun": this_sun,
    }


def weekly_category_comparison(data: Dict, date: pd.Timestamp) -> pd.DataFrame:
    """카테고리별 이번주 vs 전주 매출/GP"""
    order_items = filter_items(data.get("order_items", pd.DataFrame()))
    if order_items.empty:
        return pd.DataFrame()

    this_mon, this_sun = _get_week_range(date)
    last_mon = this_mon - timedelta(days=7)
    last_sun = this_sun - timedelta(days=7)

    this_items = order_items[(order_items["order_date"].dt.normalize() >= this_mon) &
                             (order_items["order_date"].dt.normalize() <= this_sun)]
    last_items = order_items[(order_items["order_date"].dt.normalize() >= last_mon) &
                             (order_items["order_date"].dt.normalize() <= last_sun)]

    def _cat_agg(items, suffix):
        if items.empty:
            return pd.DataFrame()
        cat = items.groupby("erp_category").agg(
            매출=("total_price", "sum"),
            건수=("order_id", "nunique"),
        ).reset_index()
        cat.columns = ["erp_category", f"매출_{suffix}", f"건수_{suffix}"]
        return cat

    this_cat = _cat_agg(this_items, "이번주")
    last_cat = _cat_agg(last_items, "전주")

    if this_cat.empty and last_cat.empty:
        return pd.DataFrame()

    if this_cat.empty:
        return last_cat
    if last_cat.empty:
        return this_cat

    merged = this_cat.merge(last_cat, on="erp_category", how="outer").fillna(0)
    merged["매출증감률"] = np.where(
        merged["매출_전주"] > 0,
        ((merged["매출_이번주"] - merged["매출_전주"]) / merged["매출_전주"] * 100).round(1),
        0,
    )
    merged = merged.sort_values("매출_이번주", ascending=False)
    return merged


def weekly_product_movers(data: Dict, date: pd.Timestamp, top_n: int = 10) -> Dict:
    """전주 대비 매출 급상승/급하락 상품"""
    order_items = filter_items(data.get("order_items", pd.DataFrame()))
    if order_items.empty:
        return {"top": pd.DataFrame(), "rising": pd.DataFrame()}

    this_mon, this_sun = _get_week_range(date)
    last_mon = this_mon - timedelta(days=7)
    last_sun = this_sun - timedelta(days=7)

    this_items = order_items[(order_items["order_date"].dt.normalize() >= this_mon) &
                             (order_items["order_date"].dt.normalize() <= this_sun)]
    last_items = order_items[(order_items["order_date"].dt.normalize() >= last_mon) &
                             (order_items["order_date"].dt.normalize() <= last_sun)]

    def _prod_agg(items):
        if items.empty:
            return pd.DataFrame(columns=["product_name", "erp_category", "매출"])
        return items.groupby(["product_name", "erp_category"]).agg(
            매출=("total_price", "sum"),
        ).reset_index()

    this_prod = _prod_agg(this_items)
    last_prod = _prod_agg(last_items)

    # TOP 10
    top = this_prod.sort_values("매출", ascending=False).head(top_n)

    # 급상승 (전주 대비)
    if last_prod.empty:
        return {"top": top, "rising": pd.DataFrame()}

    merged = this_prod.merge(
        last_prod[["product_name", "매출"]],
        on="product_name", how="left", suffixes=("_이번주", "_전주"),
    ).fillna(0)
    merged["증감액"] = merged["매출_이번주"] - merged["매출_전주"]
    merged["증감률"] = np.where(
        merged["매출_전주"] > 0,
        ((merged["매출_이번주"] - merged["매출_전주"]) / merged["매출_전주"] * 100).round(1),
        0,
    )
    # 전주에도 판매가 있었고 급상승한 상품
    rising = merged[(merged["매출_전주"] > 0) & (merged["증감률"] > 0)].sort_values("증감률", ascending=False).head(top_n)

    return {"top": top, "rising": rising}


# ──────────────────────────────────────
# 월간 리포트
# ──────────────────────────────────────

def monthly_report(data: Dict, summary: pd.DataFrame, ym: str) -> Dict:
    """월간 KPI + 전월 비교"""
    year, month = int(ym[:4]), int(ym[5:7])
    this_start = pd.Timestamp(year, month, 1)
    if month == 12:
        this_end = pd.Timestamp(year + 1, 1, 1) - timedelta(days=1)
        prev_start = pd.Timestamp(year, 11, 1)
    else:
        this_end = pd.Timestamp(year, month + 1, 1) - timedelta(days=1)
        if month == 1:
            prev_start = pd.Timestamp(year - 1, 12, 1)
        else:
            prev_start = pd.Timestamp(year, month - 1, 1)
    prev_end = this_start - timedelta(days=1)

    this_month = summary[(summary["date"] >= this_start) & (summary["date"] <= this_end)]
    prev_month = summary[(summary["date"] >= prev_start) & (summary["date"] <= prev_end)]

    def _agg(df):
        if df.empty:
            return {"매출": 0, "주문수": 0, "객단가": 0, "GP": 0, "GP율": 0.0, "영업일수": 0}
        return {
            "매출": df["매출"].sum(),
            "주문수": int(df["주문수"].sum()),
            "객단가": round(df["매출"].sum() / max(df["주문수"].sum(), 1)),
            "GP": df["GP"].sum(),
            "GP율": round(df["GP"].sum() / max(df["매출"].sum(), 1) * 100, 1),
            "영업일수": len(df),
        }

    return {
        "this_month": _agg(this_month),
        "prev_month": _agg(prev_month),
        "this_month_daily": this_month.copy(),
    }


def monthly_category_movement(data: Dict, ym: str) -> pd.DataFrame:
    """카테고리별 전월 대비 매출/GP/비중 변동"""
    order_items = filter_items(data.get("order_items", pd.DataFrame()))
    cost_records = data.get("cost_records", pd.DataFrame())

    if order_items.empty:
        return pd.DataFrame()

    year, month = int(ym[:4]), int(ym[5:7])
    this_start = pd.Timestamp(year, month, 1)
    if month == 12:
        this_end = pd.Timestamp(year + 1, 1, 1) - timedelta(days=1)
    else:
        this_end = pd.Timestamp(year, month + 1, 1) - timedelta(days=1)
    if month == 1:
        prev_start = pd.Timestamp(year - 1, 12, 1)
    else:
        prev_start = pd.Timestamp(year, month - 1, 1)
    prev_end = this_start - timedelta(days=1)

    this_items = order_items[(order_items["order_date"].dt.normalize() >= this_start) &
                             (order_items["order_date"].dt.normalize() <= this_end)]
    prev_items = order_items[(order_items["order_date"].dt.normalize() >= prev_start) &
                             (order_items["order_date"].dt.normalize() <= prev_end)]

    def _cat_agg(items, suffix):
        if items.empty:
            return pd.DataFrame(columns=["erp_category", f"매출_{suffix}", f"비중_{suffix}"])
        cat = items.groupby("erp_category").agg(매출=("total_price", "sum")).reset_index()
        cat[f"비중_{suffix}"] = (cat["매출"] / cat["매출"].sum() * 100).round(1)
        cat.columns = ["erp_category", f"매출_{suffix}", f"비중_{suffix}"]
        return cat

    this_cat = _cat_agg(this_items, "이번달")
    prev_cat = _cat_agg(prev_items, "전월")

    if this_cat.empty:
        return pd.DataFrame()

    merged = this_cat.merge(prev_cat, on="erp_category", how="outer").fillna(0)
    merged["매출증감률"] = np.where(
        merged["매출_전월"] > 0,
        ((merged["매출_이번달"] - merged["매출_전월"]) / merged["매출_전월"] * 100).round(1),
        0,
    )
    merged["비중변화"] = (merged["비중_이번달"] - merged["비중_전월"]).round(1)

    # GP 추가
    if not cost_records.empty and "erp_category" in cost_records.columns and "sale_date" in cost_records.columns:
        cr = cost_records.copy()
        cr["date"] = pd.to_datetime(cr["sale_date"]).dt.normalize()
        this_cr = cr[(cr["date"] >= this_start) & (cr["date"] <= this_end)]
        if not this_cr.empty:
            gp_cat = this_cr.groupby("erp_category").agg(
                GP=("gross_profit", "sum"),
                원가매출=("selling_price_total", "sum"),
            ).reset_index()
            gp_cat["GP율"] = (gp_cat["GP"] / gp_cat["원가매출"].replace(0, 1) * 100).round(1)
            merged = merged.merge(gp_cat[["erp_category", "GP", "GP율"]], on="erp_category", how="left")

    merged = merged.fillna(0)
    merged = merged.sort_values("매출_이번달", ascending=False)
    return merged


# ──────────────────────────────────────
# 월간 카테고리/상품 분석 (기존 sales_analysis 로직)
# ──────────────────────────────────────

def analyze_category_sales(data: Dict, ym: str) -> pd.DataFrame:
    """특정 월 카테고리별 매출"""
    order_items = filter_items(data.get("order_items", pd.DataFrame()))
    if order_items.empty:
        return pd.DataFrame()

    year, month = int(ym[:4]), int(ym[5:7])
    start = pd.Timestamp(year, month, 1)
    if month == 12:
        end = pd.Timestamp(year + 1, 1, 1) - timedelta(days=1)
    else:
        end = pd.Timestamp(year, month + 1, 1) - timedelta(days=1)

    items = order_items[(order_items["order_date"].dt.normalize() >= start) &
                        (order_items["order_date"].dt.normalize() <= end)]
    if items.empty:
        return pd.DataFrame()

    cat = items.groupby("erp_category").agg(
        판매건수=("quantity", "sum"),
        총매출=("total_price", "sum"),
        상품종류=("product_name", "nunique"),
        주문건수=("order_id", "nunique"),
    ).reset_index()
    cat["매출비중"] = (cat["총매출"] / cat["총매출"].sum() * 100).round(1)
    cat = cat.sort_values("총매출", ascending=False)
    return cat


def analyze_category_gp(data: Dict, ym: str) -> pd.DataFrame:
    """특정 월 카테고리별 GP 분석"""
    cost_records = data.get("cost_records", pd.DataFrame())
    if cost_records.empty or "erp_category" not in cost_records.columns:
        return pd.DataFrame()

    year, month = int(ym[:4]), int(ym[5:7])
    start = pd.Timestamp(year, month, 1)
    if month == 12:
        end = pd.Timestamp(year + 1, 1, 1) - timedelta(days=1)
    else:
        end = pd.Timestamp(year, month + 1, 1) - timedelta(days=1)

    cr = cost_records.copy()
    cr["date"] = pd.to_datetime(cr["sale_date"]).dt.normalize()
    cr = cr[(cr["date"] >= start) & (cr["date"] <= end)]
    if cr.empty:
        return pd.DataFrame()

    gp = cr.groupby("erp_category").agg(
        총매출=("selling_price_total", "sum"),
        총원가=("fifo_cost_total", "sum"),
        총이익=("gross_profit", "sum"),
    ).reset_index()
    gp["GP율"] = (gp["총이익"] / gp["총매출"].replace(0, 1) * 100).round(1)
    gp["이익비중"] = (gp["총이익"] / gp["총이익"].sum() * 100).round(1)
    gp = gp.sort_values("총이익", ascending=False)
    return gp


def analyze_top_products(data: Dict, ym: str, top_n: int = 15) -> pd.DataFrame:
    """특정 월 매출 TOP 상품"""
    order_items = filter_items(data.get("order_items", pd.DataFrame()))
    if order_items.empty:
        return pd.DataFrame()

    year, month = int(ym[:4]), int(ym[5:7])
    start = pd.Timestamp(year, month, 1)
    if month == 12:
        end = pd.Timestamp(year + 1, 1, 1) - timedelta(days=1)
    else:
        end = pd.Timestamp(year, month + 1, 1) - timedelta(days=1)

    items = order_items[(order_items["order_date"].dt.normalize() >= start) &
                        (order_items["order_date"].dt.normalize() <= end)]
    if items.empty:
        return pd.DataFrame()

    prods = items.groupby(["product_name", "erp_category"]).agg(
        판매수량=("quantity", "sum"),
        총매출=("total_price", "sum"),
        주문건수=("order_id", "nunique"),
    ).reset_index()
    prods = prods.sort_values("총매출", ascending=False)
    return prods.head(top_n)


def analyze_top_gp_products(data: Dict, ym: str, top_n: int = 15) -> pd.DataFrame:
    """특정 월 GP TOP 상품"""
    cost_records = data.get("cost_records", pd.DataFrame())
    if cost_records.empty or "name" not in cost_records.columns:
        return pd.DataFrame()

    year, month = int(ym[:4]), int(ym[5:7])
    start = pd.Timestamp(year, month, 1)
    if month == 12:
        end = pd.Timestamp(year + 1, 1, 1) - timedelta(days=1)
    else:
        end = pd.Timestamp(year, month + 1, 1) - timedelta(days=1)

    cr = cost_records.copy()
    cr["date"] = pd.to_datetime(cr["sale_date"]).dt.normalize()
    cr = cr[(cr["date"] >= start) & (cr["date"] <= end)]
    if cr.empty:
        return pd.DataFrame()

    gp = cr.groupby(["name", "erp_category"]).agg(
        판매수량=("quantity_sold", "sum"),
        총매출=("selling_price_total", "sum"),
        총이익=("gross_profit", "sum"),
    ).reset_index()
    gp["GP율"] = (gp["총이익"] / gp["총매출"].replace(0, 1) * 100).round(1)
    gp = gp.sort_values("총이익", ascending=False)
    return gp.head(top_n)


def analyze_hourly_pattern(data: Dict, ym: str) -> pd.DataFrame:
    """특정 월 시간대별 패턴"""
    orders = data.get("orders", pd.DataFrame())
    if orders.empty:
        return pd.DataFrame()

    oc = orders.copy()
    kst = pd.to_datetime(oc["order_date"]) + timedelta(hours=9)
    oc["month"] = kst.dt.strftime("%Y-%m")
    oc["hour"] = kst.dt.hour
    oc = oc[oc["month"] == ym]
    if oc.empty:
        return pd.DataFrame()

    hourly = oc.groupby("hour").agg(
        주문수=("id", "count"),
        총매출=("actual_amount", "sum"),
    ).reset_index()
    hourly["평균매출"] = (hourly["총매출"] / hourly["주문수"].replace(0, 1)).round(0)
    return hourly


def analyze_weekday_pattern(data: Dict, ym: str) -> pd.DataFrame:
    """특정 월 요일별 패턴"""
    orders = data.get("orders", pd.DataFrame())
    if orders.empty:
        return pd.DataFrame()

    oc = orders.copy()
    kst = pd.to_datetime(oc["order_date"]) + timedelta(hours=9)
    oc["month"] = kst.dt.strftime("%Y-%m")
    oc["weekday"] = kst.dt.dayofweek
    oc["weekday_name"] = oc["weekday"].map(
        {0: "월", 1: "화", 2: "수", 3: "목", 4: "금", 5: "토", 6: "일"}
    )
    oc = oc[oc["month"] == ym]
    if oc.empty:
        return pd.DataFrame()

    weekday = oc.groupby(["weekday", "weekday_name"]).agg(
        주문수=("id", "count"),
        총매출=("actual_amount", "sum"),
    ).reset_index().sort_values("weekday")
    return weekday


def analyze_outlier_days(data: Dict, ym: str) -> Dict:
    """z-score 기반 특이일 감지"""
    order_items = filter_items(data.get("order_items", pd.DataFrame()))
    if order_items.empty:
        return {}

    year, month = int(ym[:4]), int(ym[5:7])
    start = pd.Timestamp(year, month, 1)
    if month == 12:
        end = pd.Timestamp(year + 1, 1, 1) - timedelta(days=1)
    else:
        end = pd.Timestamp(year, month + 1, 1) - timedelta(days=1)

    items = order_items[(order_items["order_date"].dt.normalize() >= start) &
                        (order_items["order_date"].dt.normalize() <= end)]
    if items.empty:
        return {}

    daily = items.groupby(items["order_date"].dt.normalize()).agg(
        주문수=("order_id", "nunique"),
        총매출=("total_price", "sum"),
    ).reset_index()
    daily.columns = ["date", "주문수", "총매출"]
    daily["객단가"] = (daily["총매출"] / daily["주문수"].replace(0, 1)).round(0)

    if len(daily) < 5:
        return {}

    mean_rev = daily["총매출"].mean()
    std_rev = daily["총매출"].std()
    daily["매출_z"] = ((daily["총매출"] - mean_rev) / std_rev).round(2) if std_rev > 0 else 0
    daily["요일"] = daily["date"].dt.dayofweek.map(
        {0: "월", 1: "화", 2: "수", 3: "목", 4: "금", 5: "토", 6: "일"}
    )
    daily["날짜"] = daily["date"].dt.strftime("%m/%d") + "(" + daily["요일"] + ")"

    outliers = daily[daily["매출_z"].abs() >= 1.0].copy()
    outliers["유형"] = np.where(outliers["매출_z"] > 0, "호조", "부진")

    return {
        "daily": daily,
        "outliers": outliers,
        "mean_revenue": round(mean_rev),
        "std_revenue": round(std_rev),
    }


def get_available_months(data: Dict) -> list:
    """사용 가능한 월 목록 (최신순)"""
    orders = data.get("orders", pd.DataFrame())
    if orders.empty:
        return []
    kst = pd.to_datetime(orders["order_date"]) + timedelta(hours=9)
    months = sorted(kst.dt.strftime("%Y-%m").unique().tolist(), reverse=True)
    return months
