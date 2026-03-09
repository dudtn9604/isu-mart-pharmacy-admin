"""
Supabase ERP DB 연동 모듈 (성과 대시보드용)
- st.secrets에서 인증정보 로드
- 자체 완결형 (다른 프로젝트 의존 없음)
"""

import streamlit as st
import pandas as pd
from typing import Optional, List, Dict
from datetime import timedelta


# ──────────────────────────────────────
# Supabase 설정 (st.secrets 기반)
# ──────────────────────────────────────
def _get_config():
    return {
        "url": st.secrets["supabase"]["url"],
        "service_role_key": st.secrets["supabase"]["service_role_key"],
        "store_id": st.secrets["supabase"]["store_id"],
    }


_client = None


def _get_client():
    global _client
    if _client is None:
        from supabase import create_client
        cfg = _get_config()
        _client = create_client(cfg["url"], cfg["service_role_key"])
    return _client


def is_supabase_configured() -> bool:
    try:
        cfg = _get_config()
        return bool(cfg["url"] and cfg["service_role_key"] and cfg["store_id"])
    except Exception:
        return False


# ──────────────────────────────────────
# 페이지네이션 헬퍼
# ──────────────────────────────────────
def _fetch_all_rows(query_builder, page_size: int = 1000) -> List[Dict]:
    all_rows = []
    offset = 0
    while True:
        response = query_builder.range(offset, offset + page_size - 1).execute()
        rows = response.data
        all_rows.extend(rows)
        if len(rows) < page_size:
            break
        offset += page_size
    return all_rows


# ──────────────────────────────────────
# 데이터 조회
# ──────────────────────────────────────

def fetch_products() -> pd.DataFrame:
    client = _get_client()
    query = client.table("products").select("*")
    rows = _fetch_all_rows(query)
    return pd.DataFrame(rows) if rows else pd.DataFrame()


def fetch_orders(
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
) -> pd.DataFrame:
    client = _get_client()
    cfg = _get_config()
    query = (
        client.table("toss_orders")
        .select("id, store_id, toss_order_id, order_number, order_date, "
                "order_state, total_amount, discount_amount, actual_amount, "
                "item_count, order_items, synced_at")
        .eq("store_id", cfg["store_id"])
        .eq("order_state", "COMPLETED")
    )
    if date_from:
        query = query.gte("order_date", date_from)
    if date_to:
        query = query.lte("order_date", date_to + "T23:59:59")
    query = query.order("order_date", desc=True)
    rows = _fetch_all_rows(query)
    return pd.DataFrame(rows) if rows else pd.DataFrame()


def fetch_sale_cost_records(
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
) -> pd.DataFrame:
    client = _get_client()
    cfg = _get_config()
    query = (
        client.table("sale_cost_records")
        .select("*")
        .eq("store_id", cfg["store_id"])
    )
    if date_from:
        query = query.gte("sale_date", date_from)
    if date_to:
        query = query.lte("sale_date", date_to)
    rows = _fetch_all_rows(query)
    return pd.DataFrame(rows) if rows else pd.DataFrame()


# ──────────────────────────────────────
# Toss → ERP 카테고리 매핑
# ──────────────────────────────────────
TOSS_TO_ERP_CATEGORY = {
    "01 감기.해열.진통": "감기약",
    "02 알레르기/비염/안약": "알레르기・비염약",
    "03 소화. 장 건강": "위장 건강",
    "04 피로회복 에너지": "피로회복・종합영양",
    "05 근육.파스": "근육・파스",
    "06 상처/피부질환": "피부 건강",
    "07 여성건강": "여성건강",
    "08 생활건강": "생활건강",
    "09 한방케어": "한방 영양",
    "10 뇌건강/스트레스/수면": "뇌건강・수면・스트레스",
    "12 반려동물": "반려동물",
    "13 구강": "구강",
    "13 눈.관절": "관절",
    "14 다이어트/혈당/뷰티": "이너뷰티",
    "16 어린이 건강": "어린이 건강",
    "동물약": "반려동물",
    "변비/정맥순환": "위장 건강",
    "조제": "조제",
    "기본": "기타",
    "기타": "기타",
    "세트": "기타",
}


def _normalize_category(cat_str: str) -> str:
    if not cat_str or pd.isna(cat_str):
        return "기타"
    cleaned = cat_str.replace("_비적립", "").replace("_적립", "")
    if cleaned in TOSS_TO_ERP_CATEGORY:
        return TOSS_TO_ERP_CATEGORY[cleaned]
    return cleaned


# ──────────────────────────────────────
# 주문 아이템 플래트닝
# ──────────────────────────────────────
def flatten_order_items(
    orders_df: pd.DataFrame,
    products_df: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    if orders_df.empty:
        return pd.DataFrame()

    product_category_map = {}
    if products_df is not None and not products_df.empty:
        for _, p in products_df.iterrows():
            if p.get("id") and p.get("erp_category"):
                product_category_map[p["id"]] = p["erp_category"]

    rows = []
    for _, order in orders_df.iterrows():
        items = order.get("order_items", [])
        if not items:
            continue
        order_dt = pd.to_datetime(order["order_date"])
        order_dt_kst = order_dt + timedelta(hours=9)
        order_date_str = order_dt_kst.strftime("%Y-%m-%d")

        for item in items:
            product_id = item.get("productId", "")
            raw_category = item.get("category", "")
            if product_id in product_category_map:
                erp_cat = product_category_map[product_id]
            else:
                erp_cat = _normalize_category(raw_category)

            rows.append({
                "order_id": order.get("toss_order_id", ""),
                "order_date": order_date_str,
                "order_datetime_kst": order_dt_kst,
                "product_id": product_id,
                "product_name": item.get("name", ""),
                "raw_category": raw_category,
                "erp_category": erp_cat,
                "quantity": item.get("quantity", 0),
                "unit_price": item.get("price", 0),
                "total_price": item.get("totalPrice", 0),
            })

    if not rows:
        return pd.DataFrame()
    result = pd.DataFrame(rows)
    result["order_date"] = pd.to_datetime(result["order_date"])
    return result
