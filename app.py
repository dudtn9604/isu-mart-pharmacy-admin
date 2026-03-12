"""
마트약국 SKU 분석 대시보드 v6
- 현황 분석 + 트렌드 분석(통합 키워드 분석 + 네이버 + 홈쇼핑모아) 통합
- Streamlit + Plotly 기반
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path

# ──────────────────────────────────────
# 페이지 설정
# ──────────────────────────────────────
st.set_page_config(
    page_title="마트약국 SKU 분석",
    page_icon="💊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ──────────────────────────────────────
# 데이터 로드 및 전처리
# ──────────────────────────────────────
BASE_DIR = Path(__file__).parent
SKU_FILE = BASE_DIR / "신규_sku분류_정제.xlsx"
SALES_FILE = BASE_DIR / "sku매출데이터.xlsx"
SALES_COL = "실 판매 금액 \n(할인, 옵션 포함)"


@st.cache_data
def load_data():
    """SKU 분류 + 매출 데이터를 통합 로드"""
    # 1. SKU 분류 데이터
    sku = pd.read_excel(SKU_FILE, engine="openpyxl")
    sku["유형"] = sku["신규 대분류"].apply(
        lambda x: "의약품" if "비적립" in str(x) else "비의약품"
    )
    sku["카테고리"] = (
        sku["신규 대분류"]
        .str.replace("_비적립", "", regex=False)
        .str.replace("_적립", "", regex=False)
    )

    # 2. 매출 데이터
    sales_raw = pd.read_excel(SALES_FILE, sheet_name="상품 주문 합계", engine="openpyxl")
    sales = sales_raw.groupby("상품명").agg(
        매출합계=(SALES_COL, "sum"),
        판매건수합계=("판매건수", "sum"),
    ).reset_index()

    # 3. 매칭 (상품명 기준 left join)
    merged = sku.merge(sales, on="상품명", how="left")
    merged["매출합계"] = merged["매출합계"].fillna(0)
    merged["판매건수합계"] = merged["판매건수합계"].fillna(0)

    return merged


@st.cache_data
def load_trend_data(use_api=True):
    """트렌드 데이터 로드 (API 또는 데모)"""
    from trend_api import (
        is_api_configured,
        fetch_all_trends,
        generate_demo_trend_data,
        calculate_trend_metrics,
    )

    if use_api and is_api_configured():
        trend_raw = fetch_all_trends(months_back=12, force_refresh=False)
        is_demo = False
    else:
        trend_raw = generate_demo_trend_data()
        is_demo = True

    metrics = calculate_trend_metrics(trend_raw)
    return trend_raw, metrics, is_demo


@st.cache_data(ttl=3600 * 24)
def load_hsmoa_data():
    """홈쇼핑모아 데이터 로드 (검색 기반 + 트렌드 사이트)"""
    from hsmoa_api import fetch_all_hsmoa_data
    data = fetch_all_hsmoa_data(force_refresh=False)
    return data


@st.cache_data(ttl=3600 * 24)
def load_baropharm_data():
    """바로팜 데이터 로드 (의약품 + 건강식품)"""
    from baropharm_api import fetch_all_baropharm_data, is_baropharm_configured
    if not is_baropharm_configured():
        return {"products": pd.DataFrame(), "analysis": {}}
    data = fetch_all_baropharm_data(force_refresh=False)
    return data


@st.cache_data(ttl=3600 * 12)
def load_cross_analysis(_products_df, _sku_df, use_api, _baro_df=None):
    """통합 교차 검증 분석 (캐시: 12시간)"""
    from cross_analysis import run_full_cross_analysis
    return run_full_cross_analysis(
        _products_df, _sku_df, use_api=use_api,
        baro_products_df=_baro_df,
    )


@st.cache_data(ttl=3600 * 12)
def load_reverse_analysis(subcategory, _sku_df, current_sku_count, current_avg_sales, use_api):
    """역방향 분석 (캐시: 12시간)"""
    from reverse_analysis import run_reverse_analysis
    return run_reverse_analysis(
        subcategory, _sku_df,
        current_sku_count=current_sku_count,
        current_avg_sales=current_avg_sales,
        use_api=use_api,
    )


df = load_data()


# ──────────────────────────────────────
# 사이드바 (공통)
# ──────────────────────────────────────
with st.sidebar:
    st.markdown("## 💊 마트약국")
    st.markdown("### SKU 분석 시스템")
    st.divider()

    # 페이지 선택
    page = st.radio(
        "📌 메뉴",
        options=["📊 현황 분석", "📈 트렌드 분석", "💰 매출 분석", "💄 약국 화장품", "🏷️ 쇼카드 제작"],
        index=0,
    )

    st.divider()

    # 공통 필터
    type_filter = st.radio(
        "📋 유형 필터",
        options=["전체", "의약품", "비의약품"],
        index=0,
        horizontal=True,
    )

    all_categories = sorted(df["카테고리"].unique())
    selected_categories = st.multiselect(
        "📂 카테고리 필터",
        options=all_categories,
        default=all_categories,
        placeholder="카테고리를 선택하세요",
    )

    st.divider()
    st.caption("이수 마트약국 매출 데이터")
    st.caption("기간: 2025.12.08 ~ 2026.02.28 (83일)")

# ──────────────────────────────────────
# 필터 적용
# ──────────────────────────────────────
filtered = df.copy()
if type_filter != "전체":
    filtered = filtered[filtered["유형"] == type_filter]
if selected_categories:
    filtered = filtered[filtered["카테고리"].isin(selected_categories)]


# ══════════════════════════════════════
#  페이지 1: 현황 분석
# ══════════════════════════════════════
def page_sku_analysis():
    st.markdown("# 📊 SKU 현황 분석 대시보드")
    st.caption("이수 마트약국 | 매출 기간: 2025.12.08 ~ 2026.02.28")
    st.divider()

    # ── 1. KPI ──
    c1, c2, c3, c4, c5 = st.columns(5)
    total_sku = len(filtered)
    med_count = len(filtered[filtered["유형"] == "의약품"])
    non_med_count = len(filtered[filtered["유형"] == "비의약품"])
    total_sales = filtered["매출합계"].sum()
    avg_sales_per_sku = total_sales / total_sku if total_sku > 0 else 0

    c1.metric("총 SKU", f"{total_sku:,}개")
    c2.metric("💊 의약품", f"{med_count:,}개")
    c3.metric("🛒 비의약품", f"{non_med_count:,}개")
    c4.metric("💰 총 매출", f"{total_sales / 10000:,.0f}만원")
    c5.metric("📦 SKU당 평균매출", f"{avg_sales_per_sku / 10000:,.1f}만원")

    st.divider()

    # ── 2. 카테고리별 SKU 수 & 매출 비교 ──
    st.markdown("## 📈 카테고리별 SKU 수 & 매출")

    cat_summary = filtered.groupby("카테고리").agg(
        SKU수=("상품명", "count"),
        총매출=("매출합계", "sum"),
        총판매건수=("판매건수합계", "sum"),
    ).reset_index()
    cat_summary["SKU당 평균매출"] = (cat_summary["총매출"] / cat_summary["SKU수"]).round(0)
    cat_summary = cat_summary.sort_values("총매출", ascending=True)

    col_left, col_right = st.columns(2)

    with col_left:
        st.markdown("#### SKU 수 분포")
        cat_type = filtered.groupby(["카테고리", "유형"]).size().reset_index(name="SKU 수")
        fig_sku = px.bar(
            cat_type,
            y="카테고리",
            x="SKU 수",
            color="유형",
            orientation="h",
            color_discrete_map={"의약품": "#4A90D9", "비의약품": "#7EC8A0"},
            category_orders={"카테고리": cat_summary["카테고리"].tolist()},
            height=max(500, len(cat_summary) * 30),
        )
        fig_sku.update_layout(
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            margin=dict(l=0, r=20, t=10, b=0),
            yaxis_title=None, xaxis_title="SKU 수", bargap=0.2,
        )
        st.plotly_chart(fig_sku, use_container_width=True)

    with col_right:
        st.markdown("#### 매출 분포")
        cat_sales_chart = cat_summary.copy()
        cat_sales_chart["총매출(만원)"] = (cat_sales_chart["총매출"] / 10000).round(0)
        fig_sales = px.bar(
            cat_sales_chart,
            y="카테고리",
            x="총매출(만원)",
            orientation="h",
            color="SKU당 평균매출",
            color_continuous_scale="RdYlGn",
            category_orders={"카테고리": cat_summary["카테고리"].tolist()},
            height=max(500, len(cat_summary) * 30),
        )
        fig_sales.update_layout(
            margin=dict(l=0, r=20, t=10, b=0),
            yaxis_title=None, xaxis_title="매출 (만원)",
            coloraxis_colorbar=dict(title="SKU당<br>평균매출"),
            bargap=0.2,
        )
        st.plotly_chart(fig_sales, use_container_width=True)

    st.divider()

    # ── 3. 세부분류별 과부족 분석 ──
    st.markdown("## ⚠️ 세부분류별 과부족 분석")
    st.caption("SKU당 평균매출이 높으면 → 해당 세부분류의 수요 대비 SKU가 부족할 수 있음 | SKU당 평균매출이 낮으면 → SKU가 과다할 수 있음")

    sub_analysis = filtered.groupby(["카테고리", "신규 세부분류"]).agg(
        SKU수=("상품명", "count"),
        총매출=("매출합계", "sum"),
        총판매건수=("판매건수합계", "sum"),
    ).reset_index()
    sub_analysis["SKU당 평균매출"] = (sub_analysis["총매출"] / sub_analysis["SKU수"]).round(0)

    overall_avg = filtered["매출합계"].sum() / len(filtered) if len(filtered) > 0 else 0

    def classify_status(row):
        avg = row["SKU당 평균매출"]
        if avg >= overall_avg * 1.5:
            return "🔴 SKU 부족 (수요↑)"
        elif avg <= overall_avg * 0.3:
            return "🔵 SKU 과다 (수요↓)"
        elif avg >= overall_avg * 1.2:
            return "🟡 SKU 보충 검토"
        else:
            return "✅ 적정"

    sub_analysis["상태"] = sub_analysis.apply(classify_status, axis=1)
    sub_analysis["평균매출 대비"] = (sub_analysis["SKU당 평균매출"] / overall_avg * 100).round(1)

    tab_lack, tab_excess, tab_all = st.tabs(["🔴 SKU 부족 (수요 높음)", "🔵 SKU 과다 (수요 낮음)", "📋 전체 세부분류"])

    with tab_lack:
        st.markdown(
            f"**SKU당 평균매출이 전체 평균({overall_avg:,.0f}원)의 1.5배 이상** → 수요 대비 SKU가 부족할 수 있음"
        )
        lack = sub_analysis[
            sub_analysis["상태"].isin(["🔴 SKU 부족 (수요↑)", "🟡 SKU 보충 검토"])
        ].sort_values("SKU당 평균매출", ascending=False)

        if len(lack) > 0:
            display_lack = lack[["카테고리", "신규 세부분류", "SKU수", "총매출", "SKU당 평균매출", "평균매출 대비", "상태"]].reset_index(drop=True)
            display_lack["총매출"] = display_lack["총매출"].apply(lambda x: f"{x:,.0f}원")
            display_lack["SKU당 평균매출"] = display_lack["SKU당 평균매출"].apply(lambda x: f"{x:,.0f}원")

            st.dataframe(
                display_lack,
                use_container_width=True,
                column_config={
                    "평균매출 대비": st.column_config.ProgressColumn(
                        "평균 대비 %", format="%.0f%%", min_value=0, max_value=1000,
                    ),
                },
                height=500,
            )
        else:
            st.info("부족한 세부분류가 없습니다.")

    with tab_excess:
        st.markdown(
            f"**SKU당 평균매출이 전체 평균({overall_avg:,.0f}원)의 30% 이하** → SKU가 과다하거나 수요가 낮을 수 있음"
        )
        excess = sub_analysis[
            sub_analysis["상태"] == "🔵 SKU 과다 (수요↓)"
        ].sort_values("SKU당 평균매출", ascending=True)

        if len(excess) > 0:
            display_excess = excess[["카테고리", "신규 세부분류", "SKU수", "총매출", "SKU당 평균매출", "평균매출 대비", "상태"]].reset_index(drop=True)
            display_excess["총매출"] = display_excess["총매출"].apply(lambda x: f"{x:,.0f}원")
            display_excess["SKU당 평균매출"] = display_excess["SKU당 평균매출"].apply(lambda x: f"{x:,.0f}원")

            st.dataframe(
                display_excess,
                use_container_width=True,
                column_config={
                    "평균매출 대비": st.column_config.ProgressColumn(
                        "평균 대비 %", format="%.0f%%", min_value=0, max_value=100,
                    ),
                },
            )

            # ── 제거 권고 상품 목록 ──
            st.markdown("---")
            st.markdown("### 🗑️ 제거 권고 상품 목록")
            st.caption(
                "SKU 과다 세부분류 내에서 매출이 낮은 상품들입니다. "
                "매출 0원 상품을 최우선으로, 이후 매출 하위 상품을 권고합니다."
            )

            excess_subcats = excess[["카테고리", "신규 세부분류"]].values.tolist()
            removal_candidates = []

            for cat, sub in excess_subcats:
                products = filtered[
                    (filtered["카테고리"] == cat) & (filtered["신규 세부분류"] == sub)
                ].copy()
                products = products.sort_values("매출합계", ascending=True)

                sub_avg = products["매출합계"].mean()
                sub_total = len(products)

                for _, p in products.iterrows():
                    if p["매출합계"] == 0:
                        reason = "❌ 매출 0원 (83일간 판매 없음)"
                        priority = "🔴 최우선"
                    elif p["매출합계"] < sub_avg * 0.3:
                        reason = f"⚠️ 세부분류 평균({sub_avg:,.0f}원)의 {p['매출합계']/sub_avg*100:.0f}%"
                        priority = "🟡 권고"
                    elif p["매출합계"] < sub_avg * 0.5:
                        reason = f"📉 세부분류 평균({sub_avg:,.0f}원)의 {p['매출합계']/sub_avg*100:.0f}%"
                        priority = "🔵 검토"
                    else:
                        continue

                    removal_candidates.append({
                        "우선순위": priority,
                        "카테고리": cat,
                        "세부분류": sub,
                        "상품명": p["상품명"],
                        "유형": p["유형"],
                        "83일 매출": p["매출합계"],
                        "판매건수": int(p["판매건수합계"]),
                        "사유": reason,
                    })

            if removal_candidates:
                removal_df = pd.DataFrame(removal_candidates)
                priority_order = {"🔴 최우선": 0, "🟡 권고": 1, "🔵 검토": 2}
                removal_df["_sort"] = removal_df["우선순위"].map(priority_order)
                removal_df = removal_df.sort_values(["_sort", "83일 매출"]).drop(columns=["_sort"]).reset_index(drop=True)

                rc1, rc2, rc3 = st.columns(3)
                n_critical = len(removal_df[removal_df["우선순위"] == "🔴 최우선"])
                n_recommend = len(removal_df[removal_df["우선순위"] == "🟡 권고"])
                n_review = len(removal_df[removal_df["우선순위"] == "🔵 검토"])
                rc1.metric("🔴 최우선 제거", f"{n_critical}개", help="83일간 매출 0원")
                rc2.metric("🟡 제거 권고", f"{n_recommend}개", help="세부분류 평균의 30% 미만")
                rc3.metric("🔵 제거 검토", f"{n_review}개", help="세부분류 평균의 50% 미만")

                st.dataframe(
                    removal_df,
                    use_container_width=True,
                    height=500,
                    column_config={
                        "83일 매출": st.column_config.NumberColumn("83일 매출", format="₩%d"),
                        "판매건수": st.column_config.NumberColumn("판매건수", format="%d건"),
                    },
                )

                csv_data = removal_df.to_csv(index=False).encode("utf-8-sig")
                st.download_button(
                    label="📥 제거 권고 목록 CSV 다운로드",
                    data=csv_data,
                    file_name="SKU_제거권고_목록.csv",
                    mime="text/csv",
                )
            else:
                st.success("제거 권고 대상 상품이 없습니다.")
        else:
            st.info("과다한 세부분류가 없습니다.")

    with tab_all:
        all_sub = sub_analysis.sort_values(
            ["카테고리", "SKU당 평균매출"], ascending=[True, False]
        ).reset_index(drop=True)

        display_all = all_sub[["카테고리", "신규 세부분류", "SKU수", "총매출", "총판매건수", "SKU당 평균매출", "평균매출 대비", "상태"]].copy()
        display_all["총매출"] = display_all["총매출"].apply(lambda x: f"{x:,.0f}원")
        display_all["SKU당 평균매출"] = display_all["SKU당 평균매출"].apply(lambda x: f"{x:,.0f}원")

        st.dataframe(
            display_all,
            use_container_width=True,
            height=600,
            column_config={
                "평균매출 대비": st.column_config.ProgressColumn(
                    "평균 대비 %", format="%.0f%%", min_value=0, max_value=500,
                ),
            },
        )

    st.divider()

    # ── 3.5. SKU 부족 세부분류 역분석 ──
    st.markdown("## 🔄 SKU 부족 세부분류 → 트렌드 역분석")
    st.caption(
        "SKU 부족/보충 검토로 판정된 세부분류를 선택하면, "
        "**홈쇼핑모아 + 바로팜(의약품 포함) + 네이버 트렌드**를 역분석하여 "
        "**어떤 성분/브랜드의 제품을 추가해야 하는지** 구체적으로 제시합니다."
    )

    # 부족 세부분류 목록 구성
    shortage_subcats = sub_analysis[
        sub_analysis["상태"].isin(["🔴 SKU 부족 (수요↑)", "🟡 SKU 보충 검토"])
    ].sort_values("SKU당 평균매출", ascending=False)

    if len(shortage_subcats) > 0:
        # 선택 옵션 구성
        options = []
        for _, row in shortage_subcats.iterrows():
            label = "{} — {} (SKU {}개, 평균매출 {:,.0f}원)".format(
                row["상태"][:2], row["신규 세부분류"],
                row["SKU수"], row["SKU당 평균매출"],
            )
            options.append(label)

        selected_option = st.selectbox(
            "역분석할 세부분류를 선택하세요",
            options=options,
            index=0,
            key="reverse_analysis_select",
        )

        # 선택된 세부분류 정보 추출
        selected_idx = options.index(selected_option)
        selected_row = shortage_subcats.iloc[selected_idx]
        selected_subcat = selected_row["신규 세부분류"]
        selected_sku_count = int(selected_row["SKU수"])
        selected_avg_sales = selected_row["SKU당 평균매출"]

        if st.button("🔍 역분석 실행: '{}'".format(selected_subcat), type="primary"):
            st.session_state["reverse_target"] = selected_subcat
            st.session_state["reverse_sku_count"] = selected_sku_count
            st.session_state["reverse_avg_sales"] = selected_avg_sales

        # 역분석 결과 렌더링
        if "reverse_target" in st.session_state:
            target = st.session_state["reverse_target"]
            target_count = st.session_state["reverse_sku_count"]
            target_avg = st.session_state["reverse_avg_sales"]

            from naver_keyword_api import is_api_configured as naver_api_ok

            with st.spinner("'{}' 역분석 중... (홈쇼핑모아 + 바로팜 검색 + 성분 분석 + 네이버 트렌드)".format(target)):
                reverse_result = load_reverse_analysis(
                    target, filtered, target_count, target_avg, naver_api_ok(),
                )

            # 현황 요약
            st.markdown("---")
            st.markdown("### 📋 '{}' 현황 요약".format(target))
            rc1, rc2, rc3, rc4 = st.columns(4)
            rc1.metric("세부분류", target)
            rc2.metric("현재 SKU", "{}개".format(target_count))
            rc3.metric("SKU당 평균매출", "{:,.0f}원".format(target_avg))
            ratio = target_avg / overall_avg * 100 if overall_avg > 0 else 0
            rc4.metric("전체 평균 대비", "{:.0f}%".format(ratio))

            search_kws = reverse_result.get("search_keywords", [])
            st.caption("검색 키워드: {}".format(", ".join(search_kws)))

            # 시장 현황 (홈쇼핑모아 + 바로팜)
            hsmoa_prods = reverse_result.get("hsmoa_products", pd.DataFrame())
            baro_prods = reverse_result.get("baro_products", pd.DataFrame())
            all_prods = reverse_result.get("all_products", pd.DataFrame())

            st.markdown("### 🛒 시장 현황 (홈쇼핑모아 + 💊 바로팜)")

            if not all_prods.empty:
                hc1, hc2, hc3, hc4 = st.columns(4)
                hc1.metric("🛒 홈쇼핑모아", "{}개".format(len(hsmoa_prods)))
                hc2.metric("💊 바로팜", "{}개".format(len(baro_prods)))
                hc3.metric("브랜드/제조사", "{}개".format(all_prods["brand"].nunique()))
                avg_p = all_prods["sale_price"].mean()
                hc4.metric("평균 가격", "{:,.0f}원".format(avg_p) if avg_p > 0 else "-")

                # 탭으로 소스별 분리 표시
                tab_all, tab_hsmoa, tab_baro = st.tabs([
                    "📋 전체 ({})".format(len(all_prods)),
                    "🛒 홈쇼핑모아 ({})".format(len(hsmoa_prods)),
                    "💊 바로팜 ({})".format(len(baro_prods)),
                ])

                with tab_all:
                    display_all = all_prods[[
                        "name", "brand", "sale_price", "review_count", "source", "section",
                    ]].copy()
                    display_all.columns = ["상품명", "브랜드/제조사", "가격", "인기도", "소스", "유형"]
                    display_all = display_all.sort_values("인기도", ascending=False).head(20).reset_index(drop=True)
                    st.dataframe(
                        display_all,
                        use_container_width=True,
                        height=min(500, 60 + len(display_all) * 38),
                        column_config={
                            "가격": st.column_config.NumberColumn("가격", format="₩%d"),
                        },
                    )

                with tab_hsmoa:
                    if not hsmoa_prods.empty:
                        display_hsmoa = hsmoa_prods[[
                            "name", "brand", "sale_price", "review_count", "review_rating", "site",
                        ]].copy()
                        display_hsmoa.columns = ["상품명", "브랜드", "판매가", "리뷰수", "리뷰평점", "판매처"]
                        display_hsmoa = display_hsmoa.sort_values("리뷰수", ascending=False).head(15).reset_index(drop=True)
                        st.dataframe(
                            display_hsmoa,
                            use_container_width=True,
                            height=min(400, 60 + len(display_hsmoa) * 38),
                            column_config={
                                "판매가": st.column_config.NumberColumn("판매가", format="₩%d"),
                                "리뷰평점": st.column_config.NumberColumn("리뷰평점", format="%.1f"),
                            },
                        )
                    else:
                        st.info("홈쇼핑모아에서 '{}' 관련 상품을 찾지 못했습니다.".format(target))

                with tab_baro:
                    if not baro_prods.empty:
                        display_baro = baro_prods[[
                            "name", "brand", "sale_price", "review_count", "section",
                        ]].copy()
                        display_baro.columns = ["상품명", "제조사", "최저가", "재고량", "유형"]
                        display_baro = display_baro.sort_values("재고량", ascending=False).head(15).reset_index(drop=True)
                        st.dataframe(
                            display_baro,
                            use_container_width=True,
                            height=min(400, 60 + len(display_baro) * 38),
                            column_config={
                                "최저가": st.column_config.NumberColumn("최저가", format="₩%d"),
                            },
                        )
                    else:
                        st.info("바로팜에서 '{}' 관련 상품을 찾지 못했습니다.".format(target))
            else:
                st.info("'{}' 관련 상품을 찾지 못했습니다.".format(target))

            # 성분/원료 분석
            ingredient_df = reverse_result.get("ingredient_analysis", pd.DataFrame())
            st.markdown("### 🧪 성분/원료 분석")
            st.caption(
                "홈쇼핑모아 + 바로팜 인기 상품에서 발견된 성분/원료 키워드와 우리 약국 보유 현황입니다."
            )

            if not ingredient_df.empty:
                n_missing = len(ingredient_df[ingredient_df["우리_보유수"] == 0])
                n_low = len(ingredient_df[(ingredient_df["우리_보유수"] > 0) & (ingredient_df["우리_보유수"] <= 2)])

                ic1, ic2, ic3 = st.columns(3)
                ic1.metric("발견 성분", "{}개".format(len(ingredient_df)))
                ic2.metric("미보유 성분", "{}개".format(n_missing))
                ic3.metric("보유 부족 성분", "{}개".format(n_low))

                display_ingr = ingredient_df.copy()
                display_ingr.columns = [
                    "성분/원료", "시장 상품수", "홈쇼핑", "바로팜",
                    "주요 브랜드", "평균 가격", "우리 보유수", "보유상태",
                ]
                st.dataframe(
                    display_ingr,
                    use_container_width=True,
                    height=min(350, 60 + len(display_ingr) * 38),
                    column_config={
                        "평균 가격": st.column_config.NumberColumn("평균 가격", format="₩%d"),
                        "시장 상품수": st.column_config.NumberColumn("시장 상품수", format="%d개"),
                        "홈쇼핑": st.column_config.NumberColumn("🛒 홈쇼핑", format="%d"),
                        "바로팜": st.column_config.NumberColumn("💊 바로팜", format="%d"),
                    },
                )
            else:
                st.info("성분/원료 분석 결과가 없습니다.")

            # 네이버 연관 검색어
            naver_result = reverse_result.get("naver_result", {})
            related_kws = naver_result.get("related_keywords", [])
            if related_kws:
                st.markdown("### 🔍 네이버 연관 검색어")
                st.caption("'{}'의 네이버 연관 검색어입니다. 추가 제품 발굴의 힌트가 됩니다.".format(target))
                st.markdown(", ".join(["**{}**".format(kw) for kw in related_kws]))

            # 최종 추천
            recommendations = reverse_result.get("recommendations", pd.DataFrame())
            st.markdown("### 💡 최종 추천: '{}' 세부분류에 추가할 제품".format(target))
            st.caption(
                "성분/원료별 트렌드 분석과 시장 현황을 종합한 최종 추천입니다. "
                "위에서부터 우선순위가 높습니다."
            )

            if not recommendations.empty:
                display_rec = recommendations.copy()
                display_rec.columns = [
                    "우선순위", "추천 성분", "트렌드", "추천 브랜드",
                    "추천 상품", "가격대", "현재 보유", "근거",
                ]
                st.dataframe(
                    display_rec,
                    use_container_width=True,
                    height=min(500, 60 + len(display_rec) * 38),
                )

                csv_rec = display_rec.to_csv(index=False).encode("utf-8-sig")
                st.download_button(
                    label="📥 역분석 추천 CSV 다운로드",
                    data=csv_rec,
                    file_name="역분석_추천_{}.csv".format(target),
                    mime="text/csv",
                )
            else:
                st.success("추가 추천 항목이 없습니다.")
    else:
        st.success("현재 SKU 부족/보충 검토 대상 세부분류가 없습니다.")

    st.divider()

    # ── 4. 카테고리별 상세 분석 ──
    st.markdown("## 🔍 카테고리별 상세 분석")

    drill_category = st.selectbox(
        "카테고리를 선택하세요",
        options=sorted(filtered["카테고리"].unique()),
        index=0,
    )

    if drill_category:
        drill_data = filtered[filtered["카테고리"] == drill_category].copy()
        cat_total_sales = drill_data["매출합계"].sum()
        cat_total_sku = len(drill_data)

        dc1, dc2, dc3, dc4 = st.columns(4)
        dc1.metric("SKU 수", f"{cat_total_sku}개")
        dc2.metric("총 매출", f"{cat_total_sales / 10000:,.0f}만원")
        dc3.metric("SKU당 평균매출", f"{cat_total_sales / cat_total_sku / 10000:,.1f}만원" if cat_total_sku > 0 else "0")
        dc4.metric("총 판매건수", f"{int(drill_data['판매건수합계'].sum()):,}건")

        sub_drill = drill_data.groupby("신규 세부분류").agg(
            SKU수=("상품명", "count"),
            총매출=("매출합계", "sum"),
            총판매건수=("판매건수합계", "sum"),
        ).reset_index()
        sub_drill["SKU당 평균매출"] = (sub_drill["총매출"] / sub_drill["SKU수"]).round(0)
        sub_drill = sub_drill.sort_values("총매출", ascending=False)

        col_donut, col_bar = st.columns(2)

        with col_donut:
            st.markdown("#### 세부분류별 매출 비중")
            fig_donut = px.pie(
                sub_drill, values="총매출", names="신규 세부분류",
                hole=0.4, color_discrete_sequence=px.colors.qualitative.Set3,
            )
            fig_donut.update_layout(margin=dict(l=0, r=0, t=30, b=0))
            fig_donut.update_traces(textposition="inside", textinfo="label+percent", textfont_size=12)
            st.plotly_chart(fig_donut, use_container_width=True)

        with col_bar:
            st.markdown("#### SKU 수 vs SKU당 평균매출")
            fig_scatter = px.bar(
                sub_drill.sort_values("SKU당 평균매출", ascending=True),
                y="신규 세부분류", x="SKU수",
                orientation="h",
                color="SKU당 평균매출",
                color_continuous_scale="RdYlGn",
                text="SKU수",
            )
            fig_scatter.update_layout(
                margin=dict(l=0, r=20, t=10, b=0),
                yaxis_title=None,
                coloraxis_colorbar=dict(title="SKU당<br>평균매출"),
            )
            fig_scatter.update_traces(textposition="outside")
            st.plotly_chart(fig_scatter, use_container_width=True)

        st.markdown("#### 개별 상품 매출 상세")
        product_detail = (
            drill_data[["상품명", "신규 세부분류", "유형", "매출합계", "판매건수합계"]]
            .sort_values("매출합계", ascending=False)
            .reset_index(drop=True)
        )

        zero_sales = len(product_detail[product_detail["매출합계"] == 0])
        if zero_sales > 0:
            st.warning(f"⚠️ 매출 0원 상품이 {zero_sales}개 있습니다 (매출 데이터에 매칭되지 않은 상품 포함)")

        st.dataframe(
            product_detail,
            use_container_width=True,
            height=400,
            column_config={
                "매출합계": st.column_config.NumberColumn("매출", format="₩%d"),
                "판매건수합계": st.column_config.NumberColumn("판매건수", format="%d건"),
            },
        )

    st.divider()

    # ── 5. SKU 효율성 버블 차트 ──
    st.markdown("## 💡 카테고리 효율성 맵")
    st.caption("X축: SKU 수 | Y축: SKU당 평균매출 | 버블 크기: 총 매출")

    bubble_data = filtered.groupby("카테고리").agg(
        SKU수=("상품명", "count"),
        총매출=("매출합계", "sum"),
    ).reset_index()
    bubble_data["SKU당 평균매출"] = (bubble_data["총매출"] / bubble_data["SKU수"]).round(0)

    fig_bubble = px.scatter(
        bubble_data,
        x="SKU수",
        y="SKU당 평균매출",
        size="총매출",
        color="카테고리",
        text="카테고리",
        size_max=60,
        height=500,
    )

    fig_bubble.add_hline(y=overall_avg, line_dash="dash", line_color="gray",
                         annotation_text=f"평균 SKU당 매출: {overall_avg:,.0f}원")
    fig_bubble.add_vline(x=len(filtered) / filtered["카테고리"].nunique(), line_dash="dash",
                         line_color="gray", annotation_text="평균 SKU 수")

    fig_bubble.update_traces(textposition="top center", textfont_size=10)
    fig_bubble.update_layout(
        showlegend=False,
        margin=dict(l=40, r=20, t=20, b=40),
        xaxis_title="SKU 수",
        yaxis_title="SKU당 평균매출 (원)",
    )
    st.plotly_chart(fig_bubble, use_container_width=True)

    st.caption("""
**해석 가이드**
- 🔴 **우상단** (SKU 많고 매출도 높음): 핵심 카테고리 → 유지/강화
- 🟡 **좌상단** (SKU 적은데 매출 높음): **SKU 보충 필요** → 상품 추가 검토
- 🟢 **우하단** (SKU 많은데 매출 낮음): **SKU 과다** → 정리/축소 검토
- ⚪ **좌하단** (SKU 적고 매출도 낮음): 전략적 판단 필요
""")


# ══════════════════════════════════════
#  페이지 2: 트렌드 분석 → SKU 추가 제안
# ══════════════════════════════════════
def page_trend_analysis():
    from trend_api import (
        is_api_configured,
        fetch_all_trends,
        generate_demo_trend_data,
        calculate_trend_metrics,
        generate_sku_recommendations,
        analyze_keyword_coverage,
    )
    from trend_config import TREND_KEYWORD_GROUPS

    st.markdown("# 📈 트렌드 기반 SKU 추가 제안")
    st.caption(
        "홈쇼핑모아 인기 상품에서 키워드를 발굴하고, "
        "네이버 검색 트렌드 + 연관 검색어로 검증·확장하여 SKU 추가를 제안합니다."
    )
    st.divider()

    # 데이터 갱신 버튼
    col_r, col_i = st.columns([1, 4])
    with col_r:
        if st.button("🔄 트렌드 갱신", type="primary"):
            st.cache_data.clear()
            st.rerun()
    with col_i:
        st.caption("24시간마다 자동 갱신됩니다.")

    # ══════════════════════════════════════
    # 탭 구성
    # ══════════════════════════════════════
    tab_main, tab_naver, tab_hsmoa, tab_baro = st.tabs([
        "🔍 트렌드 발굴 & SKU 제안",
        "📈 네이버 검색 트렌드",
        "🛒 홈쇼핑모아 상세",
        "💊 바로팜 상세",
    ])

    with tab_main:
        _render_cross_analysis(filtered)

    with tab_naver:
        _render_naver_trend(filtered, TREND_KEYWORD_GROUPS)

    with tab_hsmoa:
        _render_hsmoa_trend(filtered)

    with tab_baro:
        _render_baropharm_trend(filtered)


def _render_cross_analysis(filtered_df):
    """트렌드 발굴 & SKU 제안 — 홈쇼핑 시작 → 네이버 검증 → 연관검색어 확장"""
    from cross_analysis import (
        run_full_cross_analysis,
        get_keyword_hsmoa_products,
        get_keyword_trend_data,
    )
    from naver_keyword_api import is_api_configured as naver_api_ok

    # 분석 흐름 안내
    st.markdown(
        "> **분석 흐름**: "
        "홈쇼핑모아 + 바로팜 인기 상품에서 키워드 추출 → "
        "네이버 검색 트렌드로 검증 → "
        "연관 검색어로 추가 발굴 → "
        "**SKU 추가 제안**"
    )

    # 데이터 로드
    with st.spinner("홈쇼핑모아 + 바로팜 + 네이버 트렌드 교차 검증 중..."):
        hsmoa_data = load_hsmoa_data()
        products_df = hsmoa_data.get("products", pd.DataFrame())

        # 바로팜 데이터 로드
        baro_data = load_baropharm_data()
        baro_products_df = baro_data.get("products", pd.DataFrame())

        if products_df.empty and baro_products_df.empty:
            st.warning("홈쇼핑모아와 바로팜 데이터를 모두 불러오지 못했습니다.")
            return

        analysis = load_cross_analysis(
            products_df, filtered_df, naver_api_ok(),
            baro_products_df if not baro_products_df.empty else None,
        )

    keyword_report = analysis["keyword_report"]
    related_report = analysis["related_report"]
    action_summary = analysis["action_summary"]
    hsmoa_keywords = analysis["hsmoa_keywords"]
    is_demo = analysis["is_demo"]

    if keyword_report.empty:
        st.info("분석 가능한 데이터가 없습니다.")
        return

    if is_demo:
        st.info("⚡ 현재 네이버 트렌드는 데모 데이터입니다. API 연동 시 실시간 데이터가 반영됩니다.")

    st.divider()

    # ══════════════════════════════════════
    # STEP 1: 시작점 — 홈쇼핑모아 인기 키워드
    # ══════════════════════════════════════
    st.markdown("### STEP 1. 시작점 — 홈쇼핑모아 인기 키워드")
    st.caption(
        "홈쇼핑모아에서 수집된 건강식품 상품에서 추출한 트렌드 키워드입니다. "
        "이 키워드들을 다음 단계에서 네이버 검색 트렌드로 검증합니다."
    )

    if hsmoa_keywords:
        seed_data = []
        for kw_info in hsmoa_keywords:
            seed_data.append({
                "키워드": kw_info["keyword"],
                "홈쇼핑 상품수": kw_info["hsmoa_product_count"],
                "주요 브랜드": ", ".join(kw_info.get("brands", [])[:3]) if kw_info.get("brands") else "-",
            })
        seed_df = pd.DataFrame(seed_data).sort_values("홈쇼핑 상품수", ascending=False).reset_index(drop=True)

        sc1, sc2 = st.columns([1, 3])
        with sc1:
            st.metric("발견 키워드", "{}개".format(len(hsmoa_keywords)))
            st.metric("홈쇼핑 상품", "{}개".format(len(products_df)))
        with sc2:
            st.dataframe(
                seed_df,
                use_container_width=True,
                height=min(300, 60 + len(seed_df) * 35),
                column_config={
                    "홈쇼핑 상품수": st.column_config.NumberColumn("홈쇼핑 상품수", format="%d개"),
                },
            )

    st.divider()

    # ══════════════════════════════════════
    # STEP 2: 네이버 검색 트렌드 검증
    # ══════════════════════════════════════
    st.markdown("### STEP 2. 네이버 검색 트렌드 검증")
    st.caption(
        "STEP 1에서 발견된 키워드를 네이버 검색 트렌드로 검증합니다. "
        "실제 검색량이 상승 중인 **'진짜 트렌드'** 키워드를 식별합니다."
    )

    n_total = len(keyword_report)
    n_rising = len(keyword_report[keyword_report["네이버_트렌드"].str.contains("상승", na=False)])
    n_new = len(keyword_report[keyword_report["추천행동"].str.contains("신규", na=False)])
    n_strengthen = len(keyword_report[keyword_report["추천행동"].str.contains("강화", na=False)])

    kc1, kc2, kc3, kc4 = st.columns(4)
    kc1.metric("검증 키워드", "{}개".format(n_total))
    kc2.metric("🔺 검색량 상승", "{}개".format(n_rising), help="네이버 검색 트렌드 상승 중")
    kc3.metric("🔴 신규 도입 필요", "{}개".format(n_new), help="우리 약국에 없는 트렌드 키워드")
    kc4.metric("🟠 라인업 강화", "{}개".format(n_strengthen), help="보유 SKU가 1~2개로 부족")

    # 필터
    action_filter = st.radio(
        "필터",
        options=["전체", "🔴 신규 도입", "🟠 라인업 강화", "🟡 추가 검토", "✅ 충분"],
        index=0,
        horizontal=True,
        key="cross_kw_filter",
    )

    display_report = keyword_report.copy()
    if action_filter != "전체":
        display_report = display_report[display_report["추천행동"] == action_filter]

    if display_report.empty:
        st.info("해당 필터 조건의 키워드가 없습니다.")
    else:
        table_cols = [
            "키워드", "신뢰도", "종합점수", "추천행동",
            "홈쇼핑_상품수", "홈쇼핑_브랜드",
            "네이버_트렌드", "네이버_성장률_표시", "네이버_모멘텀",
            "연관검색어", "상승_연관검색어",
            "보유현황", "보유_상품",
            "추천_브랜드",
        ]
        available_cols = [c for c in table_cols if c in display_report.columns]

        st.dataframe(
            display_report[available_cols],
            use_container_width=True,
            height=min(650, 60 + len(display_report) * 38),
            column_config={
                "종합점수": st.column_config.ProgressColumn(
                    "종합점수", format="%.0f", min_value=0, max_value=100,
                ),
                "네이버_모멘텀": st.column_config.ProgressColumn(
                    "네이버 모멘텀", format="%.0f", min_value=0, max_value=100,
                ),
                "홈쇼핑_상품수": st.column_config.NumberColumn("홈쇼핑 상품수", format="%d개"),
            },
        )

        # CSV 다운로드 (내부용 컬럼 제외)
        export_cols = [c for c in keyword_report.columns if not c.startswith("_")]
        csv_data = keyword_report[export_cols].to_csv(index=False).encode("utf-8-sig")
        st.download_button(
            label="📥 키워드 분석 CSV 다운로드",
            data=csv_data,
            file_name="트렌드_키워드_분석.csv",
            mime="text/csv",
        )

    # 트렌드 맵 (접을 수 있음)
    with st.expander("📊 트렌드 맵 — 키워드 포지셔닝", expanded=False):
        st.caption(
            "X축: 홈쇼핑 인기도 (상품 수) | Y축: 네이버 검색 트렌드 (모멘텀) | "
            "크기: 종합점수 | 색상: 추천 행동"
        )

        action_colors = {
            "🔴 신규 도입": "#E74C3C",
            "🟠 라인업 강화": "#F39C12",
            "🟡 추가 검토": "#F1C40F",
            "✅ 충분": "#27AE60",
        }

        fig_map = px.scatter(
            keyword_report,
            x="홈쇼핑_상품수",
            y="네이버_모멘텀",
            size="종합점수",
            color="추천행동",
            color_discrete_map=action_colors,
            text="키워드",
            size_max=50,
            height=500,
            labels={
                "홈쇼핑_상품수": "홈쇼핑 인기도 (상품 수)",
                "네이버_모멘텀": "네이버 검색 트렌드 (모멘텀)",
                "추천행동": "추천 행동",
            },
        )
        fig_map.update_traces(textposition="top center", textfont_size=10)
        fig_map.update_layout(
            margin=dict(l=40, r=20, t=20, b=40),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        )

        if not keyword_report.empty:
            avg_x = keyword_report["홈쇼핑_상품수"].median()
            avg_y = keyword_report["네이버_모멘텀"].median()
            fig_map.add_hline(y=avg_y, line_dash="dash", line_color="gray", opacity=0.4)
            fig_map.add_vline(x=avg_x, line_dash="dash", line_color="gray", opacity=0.4)

        st.plotly_chart(fig_map, use_container_width=True)

        st.caption(
            "**우상단** = 확실한 트렌드 | **좌상단** = 검색 급상승, 선제 기회 | "
            "**우하단** = 홈쇼핑 인기, 검색은 약함 | **좌하단** = 모니터링"
        )

    st.divider()

    # ══════════════════════════════════════
    # STEP 3: 연관 검색어에서 추가 트렌드 발굴
    # ══════════════════════════════════════
    st.markdown("### STEP 3. 연관 검색어에서 추가 트렌드 발굴")
    st.caption(
        "STEP 2에서 검증한 키워드의 **네이버 연관 검색어**를 분석하여, "
        "홈쇼핑모아에서는 발견되지 않았지만 검색에서 뜨고 있는 "
        "**추가 키워드**를 발굴합니다."
    )

    n_related_rising = len(related_report) if not related_report.empty else 0
    st.metric("🟣 추가 발굴 키워드", "{}개".format(n_related_rising),
              help="연관 검색어에서 발견된 상승 트렌드")

    if not related_report.empty:
        st.dataframe(
            related_report,
            use_container_width=True,
            height=min(400, 60 + len(related_report) * 38),
            column_config={
                "모멘텀": st.column_config.ProgressColumn(
                    "모멘텀", format="%.0f", min_value=0, max_value=100,
                ),
            },
        )
    else:
        st.info("현재 상승 중인 연관 검색어가 없습니다.")

    st.divider()

    # ══════════════════════════════════════
    # STEP 4: 최종 SKU 추가 제안
    # ══════════════════════════════════════
    st.markdown("### STEP 4. 최종 SKU 추가 제안")
    st.caption(
        "STEP 2(홈쇼핑+네이버 교차 검증) + STEP 3(연관 검색어 발굴)을 종합한 "
        "**최종 SKU 추가 제안**입니다. 위에서부터 우선순위가 높은 순으로 정렬됩니다."
    )

    if not action_summary.empty:
        st.dataframe(
            action_summary,
            use_container_width=True,
            height=min(500, 60 + len(action_summary) * 38),
            column_config={
                "종합점수": st.column_config.ProgressColumn(
                    "종합점수", format="%.0f", min_value=0, max_value=100,
                ),
            },
        )

        csv_summary = action_summary.to_csv(index=False).encode("utf-8-sig")
        st.download_button(
            label="📥 SKU 추가 제안 CSV 다운로드",
            data=csv_summary,
            file_name="SKU_추가제안_최종.csv",
            mime="text/csv",
        )
    else:
        st.success("현재 추가 제안 항목이 없습니다.")

    st.divider()

    # ══════════════════════════════════════
    # 섹션 5: 키워드 드릴다운
    # ══════════════════════════════════════
    st.markdown("### 🔬 키워드 상세 드릴다운")
    st.caption("특정 키워드를 선택하면 홈쇼핑 인기 상품, 우리 약국 보유 현황, 검색 트렌드를 상세히 봅니다.")

    kw_options = keyword_report["키워드"].tolist()
    selected_kw = st.selectbox(
        "키워드 선택",
        options=kw_options,
        index=0,
        key="cross_kw_drilldown",
    )

    if selected_kw:
        sel_row = keyword_report[keyword_report["키워드"] == selected_kw].iloc[0]

        # 상단 요약
        dc1, dc2, dc3, dc4, dc5 = st.columns(5)
        dc1.metric("신뢰도", sel_row["신뢰도"])
        dc2.metric("추천행동", sel_row["추천행동"])
        dc3.metric("종합점수", "{:.0f}/100".format(sel_row["종합점수"]))
        dc4.metric("네이버 트렌드", sel_row["네이버_트렌드"])
        dc5.metric("보유 SKU", sel_row["보유현황"])

        # 네이버 검색량 추이 차트
        trend_data = get_keyword_trend_data(selected_kw, keyword_report)
        if trend_data:
            chart_df = pd.DataFrame(trend_data)
            chart_df["period"] = pd.to_datetime(chart_df["period"])

            fig_trend = px.line(
                chart_df, x="period", y="ratio",
                markers=True, height=250,
                labels={"period": "기간", "ratio": "검색량 지수"},
                title="📈 '{}' 네이버 검색량 추이 (6개월)".format(selected_kw),
            )
            fig_trend.update_layout(
                margin=dict(l=40, r=20, t=40, b=30),
                xaxis_title=None, yaxis_title="검색량 지수",
            )
            fig_trend.update_xaxes(dtick="M1", tickformat="%Y-%m")
            st.plotly_chart(fig_trend, use_container_width=True)

        col_left, col_right = st.columns(2)

        with col_left:
            st.markdown("#### 🛒 홈쇼핑 인기 상품")
            hsmoa_prods = get_keyword_hsmoa_products(selected_kw, hsmoa_keywords)
            if hsmoa_prods:
                prod_df = pd.DataFrame(hsmoa_prods)
                prod_df.columns = [
                    c.replace("name", "상품명").replace("brand", "브랜드")
                    .replace("price", "판매가").replace("review_count", "리뷰수")
                    .replace("review_rating", "리뷰평점")
                    for c in prod_df.columns
                ]
                st.dataframe(
                    prod_df.head(8),
                    use_container_width=True,
                    height=min(350, 50 + len(prod_df.head(8)) * 38),
                    column_config={
                        "판매가": st.column_config.NumberColumn("판매가", format="₩%d"),
                        "리뷰평점": st.column_config.NumberColumn("리뷰평점", format="%.1f"),
                    },
                )
            else:
                st.info("홈쇼핑 상품 데이터가 없습니다.")

        with col_right:
            st.markdown("#### 💊 우리 약국 보유 현황")
            from sku_enrichment import enriched_keyword_match
            our_skus = enriched_keyword_match(selected_kw, filtered_df)

            if not our_skus.empty:
                our_cols = ["상품명", "카테고리", "매출합계", "판매건수합계"]
                available_our = [c for c in our_cols if c in our_skus.columns]
                display_our = our_skus[available_our].sort_values(
                    "매출합계", ascending=False
                ).head(8).reset_index(drop=True)
                st.dataframe(
                    display_our,
                    use_container_width=True,
                    height=min(350, 50 + len(display_our) * 38),
                    column_config={
                        "매출합계": st.column_config.NumberColumn("매출", format="₩%d"),
                        "판매건수합계": st.column_config.NumberColumn("판매건수", format="%d건"),
                    },
                )
            else:
                st.warning("우리 약국에 이 키워드 관련 SKU가 없습니다.")

            # 연관 검색어 정보
            if sel_row["연관검색어"] != "-":
                st.markdown("#### 🔍 연관 검색어")
                st.markdown("**전체**: {}".format(sel_row["연관검색어"]))
                if sel_row["상승_연관검색어"] != "-":
                    st.markdown("**상승 중**: {}".format(sel_row["상승_연관검색어"]))

        # 추천 도입 정보
        if "신규" in sel_row["추천행동"] or "강화" in sel_row["추천행동"]:
            st.markdown("---")
            st.markdown("#### 💡 추천 도입 정보")
            rec_cols = st.columns(2)
            with rec_cols[0]:
                if sel_row["추천_브랜드"] != "-":
                    st.markdown("**추천 도입 브랜드**: {}".format(sel_row["추천_브랜드"]))
                    st.caption("홈쇼핑에서 인기 있지만 우리 약국에 없는 브랜드")
                else:
                    st.info("추천 도입 브랜드 없음 (주요 브랜드 이미 보유)")
            with rec_cols[1]:
                st.markdown(
                    "**홈쇼핑 인기도**: 상품 {}개 | 평균가 {:,}원".format(
                        sel_row["홈쇼핑_상품수"], sel_row["홈쇼핑_평균가격"],
                    )
                )


def _render_naver_trend(filtered_df, TREND_KEYWORD_GROUPS):
    """네이버 검색 트렌드 탭 렌더링"""
    from trend_api import (
        is_api_configured,
        generate_sku_recommendations,
        analyze_keyword_coverage,
    )

    api_ok = is_api_configured()
    if not api_ok:
        st.warning("네이버 API 키가 설정되지 않았습니다. 데모 데이터를 표시합니다.")

    trend_raw, trend_metrics, is_demo = load_trend_data(use_api=api_ok)

    if is_demo:
        st.info("현재 데모 데이터입니다. API 연동 시 실시간 트렌드가 반영됩니다.")

    if trend_metrics.empty:
        st.error("트렌드 데이터를 로드하지 못했습니다.")
        return

    # ── 1. SKU 추가 제안 ──
    st.markdown("### 🎯 네이버 트렌드 기반 SKU 추가 제안")
    st.caption("검색 트렌드 상승 중인 키워드 vs 현재 보유 SKU를 대조하여, 추가가 필요한 영역을 제안합니다.")

    recommendations = generate_sku_recommendations(trend_metrics, filtered_df)

    if not recommendations.empty:
        n_high = len(recommendations[recommendations["우선순위"].str.contains("높음")])
        n_mid = len(recommendations[recommendations["우선순위"].str.contains("중간")])
        n_low = len(recommendations[recommendations["우선순위"].str.contains("낮음")])

        kc1, kc2, kc3, kc4 = st.columns(4)
        kc1.metric("총 제안 영역", f"{len(recommendations)}개")
        kc2.metric("🔴 높은 우선순위", f"{n_high}개", help="트렌드 상승 + SKU 커버리지 30% 미만")
        kc3.metric("🟡 중간 우선순위", f"{n_mid}개", help="트렌드 상승 + 일부 보유 or 안정 수요")
        kc4.metric("🔵 낮은 우선순위", f"{n_low}개", help="이미 SKU 보유 중이나 확장 검토")

        st.markdown("---")

        display_cols = ["우선순위", "트렌드 영역", "모멘텀", "3개월 성장률", "트렌드 방향",
                        "보유 키워드", "커버리지", "미보유 키워드", "관련 카테고리"]
        st.dataframe(
            recommendations[display_cols],
            use_container_width=True,
            height=min(600, 60 + len(recommendations) * 38),
            column_config={
                "모멘텀": st.column_config.ProgressColumn(
                    "모멘텀", format="%.1f", min_value=0, max_value=100,
                ),
            },
        )

        csv_rec = recommendations.to_csv(index=False).encode("utf-8-sig")
        st.download_button(
            label="📥 네이버 트렌드 SKU 제안 CSV",
            data=csv_rec,
            file_name="SKU_추가제안_네이버.csv",
            mime="text/csv",
        )
    else:
        st.success("현재 모든 트렌드 영역의 SKU가 충분합니다!")

    st.divider()

    # ── 2. 트렌드 영역별 상세 분석 ──
    st.markdown("### 🔍 트렌드 영역 상세 분석")
    st.caption("특정 트렌드 영역을 선택하면, 키워드별로 현재 SKU 보유 현황을 상세히 분석합니다.")

    trend_options = trend_metrics["display_name"].tolist()
    selected_area = st.selectbox(
        "트렌드 영역 선택",
        options=trend_options,
        index=0,
    )

    if selected_area:
        area_row = trend_metrics[trend_metrics["display_name"] == selected_area].iloc[0]
        group_key = area_row["group_key"]

        dc1, dc2, dc3, dc4 = st.columns(4)
        dc1.metric("최근 검색량", f"{area_row['latest_ratio']:.1f}")
        dc2.metric("3개월 성장률", f"{area_row['growth_rate_3m']:+.1f}%")
        dc3.metric("모멘텀 점수", f"{area_row['momentum_score']:.1f}/100")

        direction_icons = {"급상승": "🔺🔺", "상승": "🔺", "유지": "➡️", "하락": "🔻", "급하락": "🔻🔻"}
        dc4.metric("트렌드 방향", f"{direction_icons.get(area_row['trend_direction'], '')} {area_row['trend_direction']}")

        if not trend_raw.empty:
            area_data = trend_raw[trend_raw["group_key"] == group_key].copy()
            current_month = pd.Timestamp.now().strftime("%Y-%m")
            area_data = area_data[~area_data["period"].str.startswith(current_month)]

            if not area_data.empty:
                area_data["period"] = pd.to_datetime(area_data["period"])
                fig_area = px.line(
                    area_data, x="period", y="ratio",
                    markers=True, height=300,
                    labels={"period": "기간", "ratio": "검색량 지수"},
                )
                fig_area.update_layout(
                    margin=dict(l=40, r=20, t=10, b=30),
                    xaxis_title=None, yaxis_title="검색량 지수",
                )
                fig_area.update_xaxes(dtick="M1", tickformat="%Y-%m")
                st.plotly_chart(fig_area, use_container_width=True)

        st.markdown("#### 키워드별 SKU 보유 현황")
        coverage_df = analyze_keyword_coverage(group_key, filtered_df)

        if not coverage_df.empty:
            n_has = len(coverage_df[coverage_df["매칭 SKU 수"] > 0])
            n_miss = len(coverage_df[coverage_df["매칭 SKU 수"] == 0])

            mc1, mc2 = st.columns(2)
            mc1.metric("✅ 보유 키워드", f"{n_has}개")
            mc2.metric("❌ 미보유 키워드", f"{n_miss}개")

            st.dataframe(
                coverage_df,
                use_container_width=True,
                height=min(500, 50 + len(coverage_df) * 38),
                column_config={
                    "총 매출": st.column_config.NumberColumn("총 매출", format="₩%.0f"),
                },
            )

            missing = coverage_df[coverage_df["매칭 SKU 수"] == 0]["키워드"].tolist()
            if missing:
                st.warning(f"**미보유 키워드**: {', '.join(missing)}")
                st.caption("위 키워드 관련 상품을 추가하면 트렌드 수요를 포착할 수 있습니다.")

    st.divider()

    # ── 3. 전체 트렌드 현황 ──
    with st.expander("📊 전체 트렌드 현황 보기", expanded=False):
        st.markdown("#### 트렌드 모멘텀 랭킹")
        st.caption("모멘텀 = 최근 검색량(50%) + 성장세(50%) 종합 점수")

        fig_momentum = px.bar(
            trend_metrics.sort_values("momentum_score", ascending=True),
            y="display_name",
            x="momentum_score",
            orientation="h",
            color="trend_direction",
            color_discrete_map={
                "급상승": "#E74C3C", "상승": "#F39C12",
                "유지": "#95A5A6", "하락": "#3498DB", "급하락": "#2C3E50",
            },
            labels={"display_name": "", "momentum_score": "모멘텀 점수", "trend_direction": "방향"},
            height=max(500, len(trend_metrics) * 30),
        )
        fig_momentum.update_layout(margin=dict(l=0, r=20, t=10, b=0), bargap=0.2)
        st.plotly_chart(fig_momentum, use_container_width=True)

        st.markdown("#### 3개월 성장률 비교")
        growth_data = trend_metrics.sort_values("growth_rate_3m", ascending=True).copy()
        growth_data["색상"] = growth_data["growth_rate_3m"].apply(lambda x: "상승" if x >= 0 else "하락")

        fig_growth = px.bar(
            growth_data,
            y="display_name", x="growth_rate_3m",
            orientation="h", color="색상",
            color_discrete_map={"상승": "#E74C3C", "하락": "#3498DB"},
            labels={"display_name": "", "growth_rate_3m": "3개월 성장률 (%)"},
            height=max(500, len(growth_data) * 30),
        )
        fig_growth.update_layout(
            margin=dict(l=0, r=20, t=10, b=0), showlegend=False, bargap=0.2,
        )
        fig_growth.add_vline(x=0, line_color="gray", line_width=1)
        st.plotly_chart(fig_growth, use_container_width=True)

        st.markdown("#### 시계열 비교 (최대 5개)")
        available_cats = sorted(trend_metrics["display_name"].tolist())
        default_cats = trend_metrics.head(5)["display_name"].tolist()

        sel_cats = st.multiselect(
            "비교 영역 선택", options=available_cats,
            default=default_cats[:5], max_selections=5,
            key="trend_compare",
        )

        if sel_cats and not trend_raw.empty:
            chart_data = trend_raw[trend_raw["display_name"].isin(sel_cats)].copy()
            current_month = pd.Timestamp.now().strftime("%Y-%m")
            chart_data = chart_data[~chart_data["period"].str.startswith(current_month)]

            if not chart_data.empty:
                chart_data["period"] = pd.to_datetime(chart_data["period"])
                fig_line = px.line(
                    chart_data, x="period", y="ratio",
                    color="display_name", markers=True,
                    labels={"period": "기간", "ratio": "검색량 지수", "display_name": "영역"},
                    height=400,
                )
                fig_line.update_layout(
                    margin=dict(l=40, r=20, t=20, b=40),
                    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                    hovermode="x unified",
                )
                fig_line.update_xaxes(dtick="M1", tickformat="%Y-%m")
                st.plotly_chart(fig_line, use_container_width=True)


def _render_hsmoa_trend(filtered_df):
    """홈쇼핑모아 트렌드 탭 렌더링"""
    from hsmoa_api import (
        analyze_trending_brands,
        analyze_trending_categories,
        generate_hsmoa_sku_suggestions,
    )

    st.markdown("### 🛒 홈쇼핑모아 건강식품 트렌드")
    st.caption(
        "홈쇼핑 채널에서 현재 인기 있는 건강기능식품 상품/브랜드를 분석하고, "
        "현재 SKU에 없는 아이템을 발견합니다."
    )

    # 데이터 로드
    with st.spinner("홈쇼핑모아 데이터 수집 중... (최초 1회만 소요)"):
        hsmoa_data = load_hsmoa_data()

    products_df = hsmoa_data.get("products", pd.DataFrame())
    popular_ranking = hsmoa_data.get("popular_ranking", [])

    if products_df.empty:
        st.warning("홈쇼핑모아 데이터를 불러오지 못했습니다. 나중에 다시 시도하세요.")
        return

    # ── KPI 요약 ──
    kc1, kc2, kc3, kc4 = st.columns(4)
    kc1.metric("🛍️ 수집 상품", f"{len(products_df)}개")
    kc2.metric("🏷️ 브랜드 수", f"{products_df['brand'].nunique()}개")
    kc3.metric("📂 세부카테고리", f"{products_df['category3'].nunique()}개")
    avg_price = products_df["sale_price"].mean()
    kc4.metric("💰 평균 판매가", f"{avg_price:,.0f}원")

    st.divider()

    # ══════════════════════════════════════
    # 1. 홈쇼핑 기반 SKU 추가 제안
    # ══════════════════════════════════════
    st.markdown("### 🎯 홈쇼핑 트렌드 기반 SKU 추가 제안")
    st.caption(
        "홈쇼핑에서 인기 있는 브랜드/카테고리 중 현재 마트약국 SKU에 없는 항목을 제안합니다."
    )

    hsmoa_suggestions = generate_hsmoa_sku_suggestions(products_df, filtered_df)

    if not hsmoa_suggestions.empty:
        n_brand = len(hsmoa_suggestions[hsmoa_suggestions["유형"] == "브랜드"])
        n_cat = len(hsmoa_suggestions[hsmoa_suggestions["유형"] == "카테고리"])

        sc1, sc2, sc3 = st.columns(3)
        sc1.metric("총 제안", f"{len(hsmoa_suggestions)}개")
        sc2.metric("🏷️ 브랜드 제안", f"{n_brand}개", help="홈쇼핑 인기 브랜드 중 미보유")
        sc3.metric("📂 카테고리 제안", f"{n_cat}개", help="홈쇼핑 인기 카테고리 중 SKU 부족")

        st.dataframe(
            hsmoa_suggestions,
            use_container_width=True,
            height=min(500, 60 + len(hsmoa_suggestions) * 38),
        )

        csv_hsmoa = hsmoa_suggestions.to_csv(index=False).encode("utf-8-sig")
        st.download_button(
            label="📥 홈쇼핑 SKU 제안 CSV",
            data=csv_hsmoa,
            file_name="SKU_추가제안_홈쇼핑.csv",
            mime="text/csv",
        )
    else:
        st.success("홈쇼핑 인기 항목이 모두 SKU에 포함되어 있습니다!")

    st.divider()

    # ══════════════════════════════════════
    # 2. 인기 브랜드 분석
    # ══════════════════════════════════════
    st.markdown("### 🏷️ 홈쇼핑 인기 브랜드 TOP 15")
    st.caption("홈쇼핑 건강기능식품 방송/판매에서 자주 등장하는 브랜드입니다.")

    brand_stats = analyze_trending_brands(products_df)

    if not brand_stats.empty:
        top_brands = brand_stats.head(15)

        # 브랜드 등장횟수 차트
        fig_brand = px.bar(
            top_brands.sort_values("등장횟수", ascending=True),
            y="brand",
            x="등장횟수",
            orientation="h",
            color="리뷰평균",
            color_continuous_scale="RdYlGn",
            text="등장횟수",
            height=max(400, len(top_brands) * 32),
            labels={"brand": "브랜드", "등장횟수": "등장 횟수", "리뷰평균": "리뷰<br>평점"},
        )
        fig_brand.update_layout(
            margin=dict(l=0, r=20, t=10, b=0),
            yaxis_title=None,
            bargap=0.25,
        )
        fig_brand.update_traces(textposition="outside")
        st.plotly_chart(fig_brand, use_container_width=True)

        # 브랜드 상세 테이블
        with st.expander("📋 브랜드 상세 데이터", expanded=False):
            display_brands = brand_stats.head(20).copy()
            display_brands["평균가격"] = display_brands["평균가격"].apply(lambda x: f"{x:,}원")
            st.dataframe(display_brands, use_container_width=True, height=500)

    st.divider()

    # ══════════════════════════════════════
    # 3. 인기 카테고리(세부분류) 분석
    # ══════════════════════════════════════
    st.markdown("### 📂 홈쇼핑 인기 카테고리")
    st.caption("홈쇼핑에서 가장 많이 다루는 건강기능식품 세부분류입니다.")

    cat_stats = analyze_trending_categories(products_df)

    if not cat_stats.empty:
        col_pie, col_bar = st.columns(2)

        with col_pie:
            st.markdown("#### 카테고리별 상품 비중")
            fig_cat_pie = px.pie(
                cat_stats,
                values="상품수",
                names="세부카테고리",
                hole=0.4,
                color_discrete_sequence=px.colors.qualitative.Set3,
            )
            fig_cat_pie.update_layout(margin=dict(l=0, r=0, t=30, b=0))
            fig_cat_pie.update_traces(
                textposition="inside", textinfo="label+percent", textfont_size=11,
            )
            st.plotly_chart(fig_cat_pie, use_container_width=True)

        with col_bar:
            st.markdown("#### 카테고리별 평균가격")
            fig_cat_bar = px.bar(
                cat_stats.sort_values("평균가격", ascending=True),
                y="세부카테고리",
                x="평균가격",
                orientation="h",
                color="상품수",
                color_continuous_scale="Blues",
                text=cat_stats.sort_values("평균가격", ascending=True)["평균가격"].apply(
                    lambda x: f"{x:,.0f}원"
                ),
                height=max(400, len(cat_stats) * 35),
                labels={"세부카테고리": "", "평균가격": "평균 판매가격 (원)", "상품수": "상품 수"},
            )
            fig_cat_bar.update_layout(
                margin=dict(l=0, r=20, t=10, b=0),
                yaxis_title=None, bargap=0.25,
            )
            fig_cat_bar.update_traces(textposition="outside")
            st.plotly_chart(fig_cat_bar, use_container_width=True)

        with st.expander("📋 카테고리 상세 데이터", expanded=False):
            display_cats = cat_stats.copy()
            display_cats["평균가격"] = display_cats["평균가격"].apply(lambda x: f"{x:,}원")
            st.dataframe(display_cats, use_container_width=True)

    st.divider()

    # ══════════════════════════════════════
    # 4. 개별 상품 탐색
    # ══════════════════════════════════════
    st.markdown("### 🔍 홈쇼핑 인기 상품 탐색")
    st.caption("수집된 건강식품 상품을 검색하고 상세 정보를 확인합니다.")

    # 필터
    fcol1, fcol2, fcol3 = st.columns(3)
    with fcol1:
        search_text = st.text_input("🔎 상품/브랜드 검색", placeholder="예: 비타민, 종근당")
    with fcol2:
        cat3_options = ["전체"] + sorted(products_df["category3"].dropna().unique().tolist())
        sel_cat3 = st.selectbox("카테고리 필터", options=cat3_options, key="hsmoa_cat3")
    with fcol3:
        section_options = ["전체"] + sorted(products_df["section"].unique().tolist())
        sel_section = st.selectbox(
            "섹션 필터",
            options=section_options,
            key="hsmoa_section",
            help="best=인기, past=방송완료, future=방송예정, ep=가격비교",
        )

    view_df = products_df.copy()
    if search_text:
        view_df = view_df[
            view_df["name"].str.contains(search_text, case=False, na=False)
            | view_df["brand"].str.contains(search_text, case=False, na=False)
        ]
    if sel_cat3 != "전체":
        view_df = view_df[view_df["category3"] == sel_cat3]
    if sel_section != "전체":
        view_df = view_df[view_df["section"] == sel_section]

    st.caption(f"검색 결과: {len(view_df)}개 상품")

    display_products = view_df[
        ["name", "brand", "category3", "sale_price", "review_count",
         "review_rating", "site", "section", "keyword"]
    ].copy()
    display_products.columns = [
        "상품명", "브랜드", "카테고리", "판매가", "리뷰수",
        "리뷰평점", "판매처", "섹션", "검색키워드",
    ]
    display_products = display_products.sort_values("리뷰수", ascending=False).reset_index(drop=True)

    st.dataframe(
        display_products,
        use_container_width=True,
        height=500,
        column_config={
            "판매가": st.column_config.NumberColumn("판매가", format="₩%d"),
            "리뷰평점": st.column_config.NumberColumn("리뷰평점", format="%.1f"),
        },
    )

    # ══════════════════════════════════════
    # 5. 전체 인기 랭킹 (trend API)
    # ══════════════════════════════════════
    if popular_ranking:
        with st.expander("📊 홈쇼핑 전체 인기 상품 랭킹 (전 카테고리)", expanded=False):
            st.caption("홈쇼핑모아 트렌드 사이트 기준 전체 카테고리 인기 상품 TOP 30")
            ranking_data = []
            for item in popular_ranking[:30]:
                ranking_data.append({
                    "순위": item.get("rank", ""),
                    "상품명": item.get("result", ""),
                    "노출수": item.get("count", 0),
                })
            if ranking_data:
                st.dataframe(
                    pd.DataFrame(ranking_data),
                    use_container_width=True,
                    height=min(600, 50 + len(ranking_data) * 38),
                )


def _render_baropharm_trend(filtered_df):
    """바로팜 트렌드 탭 렌더링 — 의약품 + 건강식품 도매 데이터"""
    from baropharm_api import (
        is_baropharm_configured,
        analyze_baropharm_products,
        generate_baropharm_sku_suggestions,
    )

    st.markdown("### 💊 바로팜 의약품·건강식품 트렌드")
    st.caption(
        "바로팜 도매 플랫폼(43,000+ 상품)에서 의약품과 건강식품의 "
        "인기 상품/제조사를 분석하고, 현재 SKU에 없는 아이템을 발견합니다."
    )

    if not is_baropharm_configured():
        st.warning("바로팜 계정이 설정되지 않았습니다.")
        return

    # 데이터 로드
    with st.spinner("바로팜 데이터 수집 중... (최초 1회만 소요)"):
        baro_data = load_baropharm_data()

    products_df = baro_data.get("products", pd.DataFrame())
    analysis = baro_data.get("analysis", {})

    if products_df.empty:
        st.warning("바로팜 데이터를 불러오지 못했습니다. 나중에 다시 시도하세요.")
        return

    # ── KPI 요약 ──
    kc1, kc2, kc3, kc4 = st.columns(4)
    kc1.metric("📦 수집 상품", "{}개".format(analysis.get("total_products", 0)))
    kc2.metric("💊 의약품", "{}개".format(analysis.get("drug_count", 0)))
    kc3.metric("🛒 건강식품", "{}개".format(analysis.get("product_count", 0)))
    if "type" in products_df.columns:
        mfr_count = products_df["manufacturer"].nunique()
    else:
        mfr_count = 0
    kc4.metric("🏭 제조사", "{}개".format(mfr_count))

    st.divider()

    # ══════════════════════════════════════
    # 1. 바로팜 기반 SKU 추가 제안
    # ══════════════════════════════════════
    st.markdown("### 🎯 바로팜 기반 SKU 추가 제안")
    st.caption(
        "바로팜에서 판매량 높은 상품 중 현재 마트약국 SKU에 없는 항목을 제안합니다. "
        "의약품도 포함됩니다."
    )

    baro_suggestions = generate_baropharm_sku_suggestions(products_df, filtered_df)

    if not baro_suggestions.empty:
        n_drug = len(baro_suggestions[baro_suggestions["유형"] == "의약품"])
        n_health = len(baro_suggestions[baro_suggestions["유형"] == "건강식품"])

        sc1, sc2, sc3 = st.columns(3)
        sc1.metric("총 제안", "{}개".format(len(baro_suggestions)))
        sc2.metric("💊 의약품 제안", "{}개".format(n_drug))
        sc3.metric("🛒 건강식품 제안", "{}개".format(n_health))

        # 유형 필터
        type_option = st.radio(
            "유형 필터",
            options=["전체", "💊 의약품", "🛒 건강식품"],
            index=0,
            horizontal=True,
            key="baro_type_filter",
        )

        display_suggestions = baro_suggestions.copy()
        if type_option == "💊 의약품":
            display_suggestions = display_suggestions[display_suggestions["유형"] == "의약품"]
        elif type_option == "🛒 건강식품":
            display_suggestions = display_suggestions[display_suggestions["유형"] == "건강식품"]

        st.dataframe(
            display_suggestions,
            use_container_width=True,
            height=min(500, 60 + len(display_suggestions) * 38),
            column_config={
                "최저가": st.column_config.NumberColumn("최저가", format="₩%d"),
                "판매량": st.column_config.NumberColumn("판매량", format="%d"),
            },
        )

        csv_baro = baro_suggestions.to_csv(index=False).encode("utf-8-sig")
        st.download_button(
            label="📥 바로팜 SKU 제안 CSV",
            data=csv_baro,
            file_name="SKU_추가제안_바로팜.csv",
            mime="text/csv",
        )
    else:
        st.success("바로팜 인기 항목이 모두 SKU에 포함되어 있습니다!")

    st.divider()

    # ══════════════════════════════════════
    # 2. 인기 제조사 분석
    # ══════════════════════════════════════
    st.markdown("### 🏭 바로팜 인기 제조사 TOP 15")
    st.caption("바로팜에서 가장 많은 상품을 보유한 제조사입니다.")

    top_mfr = analysis.get("top_manufacturers", pd.DataFrame())
    if not top_mfr.empty:
        fig_mfr = px.bar(
            top_mfr.sort_values("상품수", ascending=True),
            y="제조사",
            x="상품수",
            orientation="h",
            color="평균가격",
            color_continuous_scale="RdYlGn",
            text="상품수",
            height=max(400, len(top_mfr) * 32),
            labels={"제조사": "", "상품수": "상품 수", "평균가격": "평균가격"},
        )
        fig_mfr.update_layout(
            margin=dict(l=0, r=20, t=10, b=0),
            yaxis_title=None,
            bargap=0.25,
        )
        fig_mfr.update_traces(textposition="outside")
        st.plotly_chart(fig_mfr, use_container_width=True)

        with st.expander("📋 제조사 상세 데이터", expanded=False):
            display_mfr = top_mfr.copy()
            display_mfr["평균가격"] = display_mfr["평균가격"].apply(lambda x: "{:,}원".format(x))
            st.dataframe(display_mfr, use_container_width=True)

    st.divider()

    # ══════════════════════════════════════
    # 3. 카테고리별 분석
    # ══════════════════════════════════════
    cat_stats = analysis.get("category_stats", pd.DataFrame())
    if not cat_stats.empty:
        st.markdown("### 📂 바로팜 카테고리별 분석")
        st.caption("바로팜 상품의 대분류/세부분류별 상품 수와 평균 가격입니다.")

        st.dataframe(
            cat_stats.head(20),
            use_container_width=True,
            height=min(500, 60 + min(len(cat_stats), 20) * 38),
            column_config={
                "평균가격": st.column_config.NumberColumn("평균가격", format="₩%d"),
                "상품수": st.column_config.NumberColumn("상품수", format="%d개"),
            },
        )

    st.divider()

    # ══════════════════════════════════════
    # 4. 개별 상품 탐색
    # ══════════════════════════════════════
    st.markdown("### 🔍 바로팜 상품 탐색")
    st.caption("수집된 의약품·건강식품 상품을 검색하고 상세 정보를 확인합니다.")

    fcol1, fcol2 = st.columns(2)
    with fcol1:
        baro_search = st.text_input(
            "🔎 상품/제조사 검색",
            placeholder="예: 비타민, 동아제약, 탈모",
            key="baro_search",
        )
    with fcol2:
        if "type" in products_df.columns:
            baro_type_options = ["전체", "DRUG (의약품)", "PRODUCT (건강식품)"]
        else:
            baro_type_options = ["전체"]
        baro_type_sel = st.selectbox(
            "유형 필터",
            options=baro_type_options,
            key="baro_type_sel",
        )

    view_baro = products_df.copy()
    if baro_search:
        view_baro = view_baro[
            view_baro["name"].str.contains(baro_search, case=False, na=False)
            | view_baro["manufacturer"].str.contains(baro_search, case=False, na=False)
        ]
    if baro_type_sel == "DRUG (의약품)":
        view_baro = view_baro[view_baro["type"] == "DRUG"]
    elif baro_type_sel == "PRODUCT (건강식품)":
        view_baro = view_baro[view_baro["type"] == "PRODUCT"]

    st.caption("검색 결과: {}개 상품".format(len(view_baro)))

    if not view_baro.empty:
        display_cols = ["name", "type", "manufacturer", "lowest_price", "total_qty", "keyword"]
        available_cols = [c for c in display_cols if c in view_baro.columns]
        display_baro_view = view_baro[available_cols].copy()

        col_rename = {
            "name": "상품명", "type": "유형", "manufacturer": "제조사",
            "lowest_price": "최저가", "total_qty": "재고량", "keyword": "검색키워드",
        }
        display_baro_view = display_baro_view.rename(columns=col_rename)
        display_baro_view = display_baro_view.sort_values("재고량", ascending=False).head(50).reset_index(drop=True)

        st.dataframe(
            display_baro_view,
            use_container_width=True,
            height=500,
            column_config={
                "최저가": st.column_config.NumberColumn("최저가", format="₩%d"),
            },
        )


# ══════════════════════════════════════
#  페이지 3: 매출 분석 (Supabase ERP 기반)
# ══════════════════════════════════════
def page_sales_analysis():
    """매출 분석 페이지 — Supabase ERP DB 실시간 데이터 기반"""
    from sales_analysis import (
        run_sales_analysis,
        load_sales_data,
        calculate_kpis,
        analyze_daily_trend,
        analyze_category_sales,
        analyze_category_gp,
        analyze_top_products,
        analyze_bottom_products,
        analyze_top_gp_products,
        analyze_hourly_pattern,
        analyze_weekday_pattern,
        analyze_monthly_trend,
        analyze_category_monthly_trend,
        compare_periods,
    )
    from supabase_client import is_supabase_configured

    st.markdown("# 💰 매출 분석 대시보드")
    st.caption("이수 마트약국 | Supabase ERP 실시간 데이터")

    if not is_supabase_configured():
        st.error("Supabase ERP 설정이 필요합니다. trend_config.py를 확인해주세요.")
        return

    st.divider()

    # ── 기간 선택 ──
    from datetime import datetime, timedelta, date

    col_date1, col_date2, col_preset = st.columns([2, 2, 3])

    with col_preset:
        period_preset = st.selectbox(
            "📅 기간 프리셋",
            options=[
                "최근 7일", "최근 30일", "최근 90일",
                "이번 달", "지난 달", "전체 기간", "직접 선택",
            ],
            index=1,
        )

    today = date.today()

    if period_preset == "최근 7일":
        d_from = today - timedelta(days=7)
        d_to = today
    elif period_preset == "최근 30일":
        d_from = today - timedelta(days=30)
        d_to = today
    elif period_preset == "최근 90일":
        d_from = today - timedelta(days=90)
        d_to = today
    elif period_preset == "이번 달":
        d_from = today.replace(day=1)
        d_to = today
    elif period_preset == "지난 달":
        first_this_month = today.replace(day=1)
        d_to = first_this_month - timedelta(days=1)
        d_from = d_to.replace(day=1)
    elif period_preset == "전체 기간":
        d_from = date(2025, 12, 8)
        d_to = today
    else:
        d_from = today - timedelta(days=30)
        d_to = today

    with col_date1:
        date_from = st.date_input("시작일", value=d_from, key="sales_date_from")
    with col_date2:
        date_to = st.date_input("종료일", value=d_to, key="sales_date_to")

    date_from_str = date_from.strftime("%Y-%m-%d")
    date_to_str = date_to.strftime("%Y-%m-%d")
    days_selected = (date_to - date_from).days + 1

    # ── 데이터 로드 (캐시) ──
    @st.cache_data(ttl=3600 * 1, show_spinner="📊 매출 데이터 로드 중...")
    def _load_sales(df_str, dt_str):
        return run_sales_analysis(date_from=df_str, date_to=dt_str)

    result = _load_sales(date_from_str, date_to_str)

    if "error" in result:
        st.error(result["error"])
        return

    kpis = result.get("kpis", {})
    if not kpis:
        st.warning("선택한 기간에 매출 데이터가 없습니다.")
        return

    # ══════════════════════════════════════
    # 1. KPI 요약
    # ══════════════════════════════════════
    st.divider()
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric(
        "💰 총 매출",
        "{:,.0f}만원".format(kpis["total_revenue"] / 10000),
        help="기간 내 실결제 금액 합계",
    )
    c2.metric(
        "🛒 총 주문",
        "{:,}건".format(kpis["total_orders"]),
    )
    c3.metric(
        "📦 평균 주문가",
        "{:,.0f}원".format(kpis["avg_order_value"]),
    )
    c4.metric(
        "📊 일평균 매출",
        "{:,.0f}만원".format(kpis["daily_avg_revenue"] / 10000),
    )
    c5.metric(
        "📈 매출총이익(GP)",
        "{:,.0f}만원 ({:.1f}%)".format(kpis["total_gp"] / 10000, kpis["gp_rate"]),
    )

    st.caption(
        "기간: {} ~ {} ({}일) | 판매 상품: {:,}종".format(
            date_from_str, date_to_str, days_selected,
            kpis.get("unique_products", 0),
        )
    )

    st.divider()

    # ══════════════════════════════════════
    # 2. 일별 매출 트렌드
    # ══════════════════════════════════════
    st.markdown("## 📈 일별 매출 트렌드")

    daily_trend = result.get("daily_trend", pd.DataFrame())
    if not daily_trend.empty:
        fig_daily = go.Figure()
        fig_daily.add_trace(go.Bar(
            x=daily_trend["date"],
            y=daily_trend["총매출"],
            name="일매출",
            marker_color="#4A90D9",
            opacity=0.6,
        ))
        fig_daily.add_trace(go.Scatter(
            x=daily_trend["date"],
            y=daily_trend["7일이평"],
            name="7일 이동평균",
            line=dict(color="#E74C3C", width=2.5),
            mode="lines",
        ))
        fig_daily.update_layout(
            height=350,
            margin=dict(l=40, r=20, t=10, b=40),
            yaxis_title="매출 (원)",
            xaxis_title="",
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            hovermode="x unified",
        )
        fig_daily.update_yaxes(tickformat=",")
        st.plotly_chart(fig_daily, use_container_width=True)

        # 일별 주문수 추가 표시
        col_daily_1, col_daily_2 = st.columns(2)
        with col_daily_1:
            fig_orders = px.line(
                daily_trend, x="date", y="주문수",
                title="일별 주문 건수",
                height=250,
            )
            fig_orders.update_layout(margin=dict(l=40, r=20, t=40, b=30))
            st.plotly_chart(fig_orders, use_container_width=True)
        with col_daily_2:
            daily_trend_copy = daily_trend.copy()
            daily_trend_copy["건당 매출"] = (daily_trend_copy["총매출"] / daily_trend_copy["주문수"]).round(0)
            fig_aov = px.line(
                daily_trend_copy, x="date", y="건당 매출",
                title="일별 평균 주문가",
                height=250,
            )
            fig_aov.update_layout(margin=dict(l=40, r=20, t=40, b=30))
            fig_aov.update_yaxes(tickformat=",")
            st.plotly_chart(fig_aov, use_container_width=True)
    else:
        st.info("일별 트렌드 데이터가 없습니다.")

    st.divider()

    # ══════════════════════════════════════
    # 3. 카테고리별 매출 분석
    # ══════════════════════════════════════
    st.markdown("## 📂 카테고리별 매출 분석")

    tab_cat_sales, tab_cat_gp, tab_cat_trend = st.tabs([
        "💰 매출 분포", "📊 수익성(GP) 분석", "📈 카테고리별 월별 트렌드",
    ])

    with tab_cat_sales:
        cat_sales = result.get("category_sales", pd.DataFrame())
        if not cat_sales.empty:
            col_pie, col_bar = st.columns(2)

            with col_pie:
                fig_pie = px.pie(
                    cat_sales.head(15),
                    values="총매출",
                    names="erp_category",
                    hole=0.4,
                    title="카테고리별 매출 비중",
                    color_discrete_sequence=px.colors.qualitative.Set3,
                )
                fig_pie.update_traces(textposition="inside", textinfo="label+percent")
                fig_pie.update_layout(
                    height=450,
                    margin=dict(l=0, r=0, t=40, b=0),
                    showlegend=False,
                )
                st.plotly_chart(fig_pie, use_container_width=True)

            with col_bar:
                fig_bar = px.bar(
                    cat_sales.sort_values("총매출", ascending=True),
                    y="erp_category",
                    x="총매출",
                    orientation="h",
                    title="카테고리별 매출",
                    color="SKU당 평균매출",
                    color_continuous_scale="RdYlGn",
                )
                fig_bar.update_layout(
                    height=450,
                    margin=dict(l=0, r=20, t=40, b=0),
                    yaxis_title=None,
                    xaxis_title="매출 (원)",
                    coloraxis_colorbar=dict(title="SKU당<br>평균매출"),
                )
                fig_bar.update_xaxes(tickformat=",")
                st.plotly_chart(fig_bar, use_container_width=True)

            # 카테고리 상세 테이블
            display_cat = cat_sales.copy()
            display_cat["총매출"] = display_cat["총매출"].apply(lambda x: "{:,.0f}원".format(x))
            display_cat["SKU당 평균매출"] = display_cat["SKU당 평균매출"].apply(lambda x: "{:,.0f}원".format(x))
            display_cat["건당 평균매출"] = display_cat["건당 평균매출"].apply(lambda x: "{:,.0f}원".format(x))
            display_cat = display_cat.rename(columns={"erp_category": "카테고리"})
            st.dataframe(
                display_cat[["카테고리", "판매건수", "총매출", "상품종류", "SKU당 평균매출", "건당 평균매출", "매출비중"]],
                use_container_width=True,
                height=500,
                column_config={
                    "매출비중": st.column_config.ProgressColumn(
                        "매출비중 %", format="%.1f%%", min_value=0, max_value=100,
                    ),
                },
            )
        else:
            st.info("카테고리 매출 데이터가 없습니다.")

    with tab_cat_gp:
        cat_gp = result.get("category_gp", pd.DataFrame())
        if not cat_gp.empty:
            col_gp1, col_gp2 = st.columns(2)

            with col_gp1:
                fig_gp_bar = px.bar(
                    cat_gp.sort_values("총이익", ascending=True).tail(15),
                    y="erp_category",
                    x="총이익",
                    orientation="h",
                    title="카테고리별 매출총이익",
                    color="GP율",
                    color_continuous_scale="RdYlGn",
                )
                fig_gp_bar.update_layout(
                    height=450,
                    margin=dict(l=0, r=20, t=40, b=0),
                    yaxis_title=None,
                    xaxis_title="매출총이익 (원)",
                    coloraxis_colorbar=dict(title="GP율(%)"),
                )
                fig_gp_bar.update_xaxes(tickformat=",")
                st.plotly_chart(fig_gp_bar, use_container_width=True)

            with col_gp2:
                fig_gp_scatter = px.scatter(
                    cat_gp,
                    x="총매출",
                    y="GP율",
                    size="총이익",
                    color="erp_category",
                    title="매출 vs GP율 (버블 크기: 총이익)",
                    size_max=50,
                )
                fig_gp_scatter.update_layout(
                    height=450,
                    margin=dict(l=40, r=20, t=40, b=30),
                    showlegend=False,
                    xaxis_title="총매출 (원)",
                    yaxis_title="GP율 (%)",
                )
                fig_gp_scatter.update_xaxes(tickformat=",")
                st.plotly_chart(fig_gp_scatter, use_container_width=True)

            # GP 상세 테이블
            display_gp = cat_gp.copy()
            for col in ["총매출", "총원가", "총이익"]:
                display_gp[col] = display_gp[col].apply(lambda x: "{:,.0f}원".format(x))
            display_gp = display_gp.rename(columns={"erp_category": "카테고리"})
            st.dataframe(
                display_gp[["카테고리", "총매출", "총원가", "총이익", "GP율", "거래건수", "이익비중"]],
                use_container_width=True,
                height=450,
                column_config={
                    "GP율": st.column_config.ProgressColumn(
                        "GP율 %", format="%.1f%%", min_value=0, max_value=100,
                    ),
                    "이익비중": st.column_config.ProgressColumn(
                        "이익비중 %", format="%.1f%%", min_value=0, max_value=100,
                    ),
                },
            )
        else:
            st.info("GP 분석 데이터가 없습니다.")

    with tab_cat_trend:
        cat_monthly = result.get("category_monthly", pd.DataFrame())
        if not cat_monthly.empty:
            # 월별 TOP 카테고리 라인 차트
            top_cats = cat_monthly.sum().nlargest(10).index.tolist()
            fig_cat_trend = go.Figure()
            colors = px.colors.qualitative.Set2
            for i, cat in enumerate(top_cats):
                fig_cat_trend.add_trace(go.Scatter(
                    x=cat_monthly.index,
                    y=cat_monthly[cat],
                    name=cat,
                    mode="lines+markers",
                    line=dict(color=colors[i % len(colors)], width=2),
                ))
            fig_cat_trend.update_layout(
                title="카테고리별 월별 매출 추이 (TOP 10)",
                height=450,
                margin=dict(l=40, r=20, t=40, b=30),
                yaxis_title="매출 (원)",
                hovermode="x unified",
            )
            fig_cat_trend.update_yaxes(tickformat=",")
            st.plotly_chart(fig_cat_trend, use_container_width=True)
        else:
            st.info("월별 트렌드 데이터가 없습니다.")

    st.divider()

    # ══════════════════════════════════════
    # 4. 상품별 매출 분석
    # ══════════════════════════════════════
    st.markdown("## 🏆 상품별 매출 분석")

    tab_top, tab_bottom, tab_gp_top = st.tabs([
        "🔺 매출 TOP 30", "🔻 매출 하위 30", "💎 이익 TOP 30",
    ])

    with tab_top:
        top_products = result.get("top_products", pd.DataFrame())
        if not top_products.empty:
            display_top = top_products.copy()
            display_top = display_top.rename(columns={
                "product_name": "상품명",
                "erp_category": "카테고리",
            })
            st.dataframe(
                display_top[["상품명", "카테고리", "판매수량", "총매출", "주문건수", "건당 평균"]],
                use_container_width=True,
                height=600,
                column_config={
                    "총매출": st.column_config.NumberColumn("총매출", format="₩%d"),
                    "건당 평균": st.column_config.NumberColumn("건당 평균", format="₩%d"),
                },
            )
        else:
            st.info("상품 매출 데이터가 없습니다.")

    with tab_bottom:
        bottom_products = result.get("bottom_products", pd.DataFrame())
        if not bottom_products.empty:
            display_bottom = bottom_products.copy()
            display_bottom = display_bottom.rename(columns={
                "product_name": "상품명",
                "erp_category": "카테고리",
            })
            st.dataframe(
                display_bottom[["상품명", "카테고리", "판매수량", "총매출", "주문건수"]],
                use_container_width=True,
                height=600,
                column_config={
                    "총매출": st.column_config.NumberColumn("총매출", format="₩%d"),
                },
            )
        else:
            st.info("상품 매출 데이터가 없습니다.")

    with tab_gp_top:
        top_gp = result.get("top_gp_products", pd.DataFrame())
        if not top_gp.empty:
            display_gp_top = top_gp.copy()
            display_gp_top = display_gp_top.rename(columns={
                "name": "상품명",
                "erp_category": "카테고리",
            })
            st.dataframe(
                display_gp_top[["상품명", "카테고리", "판매수량", "총매출", "총원가", "총이익", "GP율"]],
                use_container_width=True,
                height=600,
                column_config={
                    "총매출": st.column_config.NumberColumn("총매출", format="₩%d"),
                    "총원가": st.column_config.NumberColumn("총원가", format="₩%d"),
                    "총이익": st.column_config.NumberColumn("총이익", format="₩%d"),
                    "GP율": st.column_config.ProgressColumn(
                        "GP율 %", format="%.1f%%", min_value=0, max_value=100,
                    ),
                },
            )
        else:
            st.info("GP 분석 데이터가 없습니다.")

    st.divider()

    # ══════════════════════════════════════
    # 5. 판매 패턴 분석
    # ══════════════════════════════════════
    st.markdown("## 🕐 판매 패턴 분석")

    col_hourly, col_weekday = st.columns(2)

    with col_hourly:
        hourly = result.get("hourly_pattern", pd.DataFrame())
        if not hourly.empty:
            fig_hourly = go.Figure()
            fig_hourly.add_trace(go.Bar(
                x=hourly["hour"],
                y=hourly["주문수"],
                name="주문수",
                marker_color="#4A90D9",
            ))
            fig_hourly.add_trace(go.Scatter(
                x=hourly["hour"],
                y=hourly["총매출"],
                name="총매출",
                yaxis="y2",
                line=dict(color="#E74C3C", width=2),
                mode="lines+markers",
            ))
            fig_hourly.update_layout(
                title="시간대별 판매 패턴 (KST)",
                height=400,
                margin=dict(l=40, r=40, t=40, b=30),
                xaxis=dict(title="시간 (KST)", dtick=1),
                yaxis=dict(title="주문수"),
                yaxis2=dict(title="매출 (원)", overlaying="y", side="right", tickformat=","),
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            )
            st.plotly_chart(fig_hourly, use_container_width=True)
        else:
            st.info("시간대 분석 데이터가 없습니다.")

    with col_weekday:
        weekday = result.get("weekday_pattern", pd.DataFrame())
        if not weekday.empty:
            fig_weekday = go.Figure()
            fig_weekday.add_trace(go.Bar(
                x=weekday["weekday_name"],
                y=weekday["주평균 매출"],
                name="주평균 매출",
                marker_color="#7EC8A0",
            ))
            fig_weekday.add_trace(go.Scatter(
                x=weekday["weekday_name"],
                y=weekday["주평균 주문수"],
                name="주평균 주문수",
                yaxis="y2",
                line=dict(color="#E74C3C", width=2),
                mode="lines+markers",
            ))
            fig_weekday.update_layout(
                title="요일별 판매 패턴 (KST)",
                height=400,
                margin=dict(l=40, r=40, t=40, b=30),
                xaxis=dict(title="요일"),
                yaxis=dict(title="주평균 매출 (원)", tickformat=","),
                yaxis2=dict(title="주평균 주문수", overlaying="y", side="right"),
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            )
            st.plotly_chart(fig_weekday, use_container_width=True)
        else:
            st.info("요일별 분석 데이터가 없습니다.")

    st.divider()

    # ══════════════════════════════════════
    # 6. 월별 요약
    # ══════════════════════════════════════
    monthly = result.get("monthly_trend", pd.DataFrame())
    if not monthly.empty:
        st.markdown("## 📅 월별 매출 요약")

        col_m1, col_m2 = st.columns(2)

        with col_m1:
            fig_monthly = go.Figure()
            fig_monthly.add_trace(go.Bar(
                x=monthly["month"],
                y=monthly["총매출"],
                name="총매출",
                marker_color="#4A90D9",
                text=monthly["총매출"].apply(lambda x: "{:,.0f}만".format(x / 10000)),
                textposition="outside",
            ))
            fig_monthly.update_layout(
                title="월별 매출",
                height=350,
                margin=dict(l=40, r=20, t=40, b=30),
                yaxis_title="매출 (원)",
                xaxis_title="",
            )
            fig_monthly.update_yaxes(tickformat=",")
            st.plotly_chart(fig_monthly, use_container_width=True)

        with col_m2:
            fig_monthly_orders = go.Figure()
            fig_monthly_orders.add_trace(go.Bar(
                x=monthly["month"],
                y=monthly["주문수"],
                name="주문수",
                marker_color="#7EC8A0",
                text=monthly["주문수"].apply(lambda x: "{:,}".format(x)),
                textposition="outside",
            ))
            fig_monthly_orders.add_trace(go.Scatter(
                x=monthly["month"],
                y=monthly["평균주문가"],
                name="평균주문가",
                yaxis="y2",
                line=dict(color="#E74C3C", width=2),
                mode="lines+markers",
            ))
            fig_monthly_orders.update_layout(
                title="월별 주문수 & 평균 주문가",
                height=350,
                margin=dict(l=40, r=40, t=40, b=30),
                yaxis=dict(title="주문수"),
                yaxis2=dict(title="평균 주문가 (원)", overlaying="y", side="right", tickformat=","),
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            )
            st.plotly_chart(fig_monthly_orders, use_container_width=True)

        # 월별 상세 테이블
        display_monthly = monthly.copy()
        display_monthly["총매출"] = display_monthly["총매출"].apply(lambda x: "{:,.0f}원".format(x))
        display_monthly["평균주문가"] = display_monthly["평균주문가"].apply(lambda x: "{:,.0f}원".format(x))
        display_monthly = display_monthly.rename(columns={"month": "월"})
        st.dataframe(display_monthly, use_container_width=True)

    st.divider()

    # ══════════════════════════════════════
    # 7. 특이일 심층 분석 (고객↑ / 객단가↑)
    # ══════════════════════════════════════
    st.markdown("## 🔍 2월 특이일 분석 — 확 띈 날의 인사이트")
    st.caption(
        "2월 데이터에서 고객수·객단가가 평균 대비 확연히 높았던 날(z-score ≥ 1.0)을 "
        "자동 감지하고, 보통날과의 차이를 분석합니다. (조제·키인결제 제외)"
    )

    outlier = result.get("outlier_analysis", {})
    if not outlier:
        st.info("특이일 분석 데이터가 부족합니다.")
    else:
        stats = outlier.get("stats", {})
        busy_a = outlier.get("busy_analysis", {})
        aov_a = outlier.get("aov_analysis", {})

        # 기준 안내
        col_info1, col_info2 = st.columns(2)
        col_info1.info(
            "**고객 많은 날**: 평균 {:.0f}명 기준, {:.0f}명 이상 (z≥1.0) → **{}일** 감지".format(
                stats.get("mean_cust", 0),
                stats.get("mean_cust", 0) + stats.get("std_cust", 0),
                busy_a.get("summary", {}).get("n_target", 0) if busy_a else 0,
            )
        )
        col_info2.info(
            "**객단가 높은 날**: 평균 {:,.0f}원 기준, {:,.0f}원 이상 (z≥1.0) → **{}일** 감지".format(
                stats.get("mean_aov", 0),
                stats.get("mean_aov", 0) + stats.get("std_aov", 0),
                aov_a.get("summary", {}).get("n_target", 0) if aov_a else 0,
            )
        )
        if stats.get("overlap_dates"):
            st.warning("겹치는 날: **{}** (고객도 많고 객단가도 높았음)".format(
                ", ".join(stats["overlap_dates"])
            ))

        # 일별 오버뷰 차트
        daily_ov = outlier.get("daily_overview", pd.DataFrame())
        if not daily_ov.empty:
            st.markdown("##### 📈 2월 일별 고객수 & 객단가 추이")
            from plotly.subplots import make_subplots
            fig_ov = make_subplots(specs=[[{"secondary_y": True}]])
            # 고객수 바
            colors_cust = [
                "#E74C3C" if z >= stats.get("z_threshold", 1.0) else "#D5DBDB"
                for z in daily_ov["고객수_z"]
            ]
            fig_ov.add_trace(go.Bar(
                x=daily_ov["date_str"], y=daily_ov["주문수"],
                name="고객수", marker_color=colors_cust, opacity=0.7,
            ), secondary_y=False)
            # 객단가 라인
            fig_ov.add_trace(go.Scatter(
                x=daily_ov["date_str"], y=daily_ov["객단가"],
                name="객단가", mode="lines+markers",
                line=dict(color="#F39C12", width=2),
                marker=dict(
                    size=8,
                    color=["#F39C12" if z >= stats.get("z_threshold", 1.0) else "#F5CBA7"
                           for z in daily_ov["객단가_z"]],
                ),
            ), secondary_y=True)
            fig_ov.update_layout(
                height=350, margin=dict(l=0, r=0, t=30, b=40),
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                xaxis_tickangle=-45,
            )
            fig_ov.update_yaxes(title_text="고객수(명)", secondary_y=False)
            fig_ov.update_yaxes(title_text="객단가(원)", tickformat=",", secondary_y=True)
            st.plotly_chart(fig_ov, use_container_width=True)
            st.caption("빨간 바 = 고객수 특이일 / 주황 큰 점 = 객단가 특이일")

        tab_busy, tab_aov = st.tabs([
            "🏃 고객 많은 날 인사이트",
            "💎 객단가 높은 날 인사이트",
        ])

        # ────────────────────────
        # TAB 1: 고객 많은 날
        # ────────────────────────
        with tab_busy:
            if not busy_a:
                st.info("고객수 특이일이 감지되지 않았습니다.")
            else:
                bs = busy_a.get("summary", {})
                basket = busy_a.get("basket_compare", {})

                # 일별 상세 테이블
                detail = busy_a.get("daily_detail", pd.DataFrame())
                if not detail.empty:
                    st.markdown("##### 📅 고객 많은 날 일별 현황")
                    dd = detail[["날짜", "고객수", "객단가(원)", "매출(원)", "평균구매수량"]].copy()
                    dd["객단가(원)"] = dd["객단가(원)"].apply(lambda x: "{:,.0f}원".format(x))
                    dd["매출(원)"] = dd["매출(원)"].apply(lambda x: "{:,.0f}원".format(x))
                    dd = dd.rename(columns={"고객수": "고객수(명)"})
                    st.dataframe(dd, use_container_width=True, hide_index=True)

                # KPI 비교
                st.markdown("##### 📊 고객 많은 날 vs 보통날")
                kc1, kc2, kc3, kc4, kc5 = st.columns(5)
                kc1.metric("해당 일수", "{}일".format(bs.get("n_target", 0)))
                kc2.metric(
                    "평균 고객수",
                    "{:.0f}명".format(bs.get("target_avg_orders", 0)),
                    delta="{:+.0f}명".format(
                        bs.get("target_avg_orders", 0) - bs.get("normal_avg_orders", 0)
                    ),
                )
                kc3.metric(
                    "평균 객단가",
                    "{:,.0f}원".format(bs.get("target_avg_aov", 0)),
                    delta="{:+,.0f}원".format(
                        bs.get("target_avg_aov", 0) - bs.get("normal_avg_aov", 0)
                    ),
                )
                kc4.metric(
                    "일평균 매출",
                    "{:,.0f}만원".format(bs.get("target_avg_revenue", 0) / 10000),
                    delta="{:+,.0f}만원".format(
                        (bs.get("target_avg_revenue", 0) - bs.get("normal_avg_revenue", 0)) / 10000
                    ),
                )
                kc5.metric(
                    "인당 구매수량",
                    "{:.1f}개".format(basket.get("특이일_평균상품수", 0)),
                    delta="{:+.1f}개".format(
                        basket.get("특이일_평균상품수", 0) - basket.get("보통날_평균상품수", 0)
                    ),
                )

                # 시간대 비교
                hourly = busy_a.get("hourly_comparison", pd.DataFrame())
                if not hourly.empty:
                    st.markdown("##### ⏰ 시간대별 고객수 비교")
                    fig_hr = go.Figure()
                    fig_hr.add_trace(go.Bar(
                        x=hourly["hour"], y=hourly["일평균 건수_특이일"],
                        name="고객 많은 날", marker_color="#E74C3C", opacity=0.8,
                    ))
                    fig_hr.add_trace(go.Bar(
                        x=hourly["hour"], y=hourly["일평균 건수_보통날"],
                        name="보통날", marker_color="#BDC3C7", opacity=0.8,
                    ))
                    fig_hr.update_layout(
                        barmode="group", height=300,
                        margin=dict(l=0, r=0, t=10, b=30),
                        xaxis_title="시간", yaxis_title="일평균 고객수",
                        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                    )
                    st.plotly_chart(fig_hr, use_container_width=True)

                # 카테고리 매출비중 파이차트 비교
                st.markdown("##### 🥧 카테고리 매출비중 비교")
                cat_comp = busy_a.get("category_comparison", pd.DataFrame())
                if not cat_comp.empty:
                    pie_col1, pie_col2 = st.columns(2)
                    # 특이일 파이
                    pie_data_t = cat_comp[["erp_category", "매출비중_특이일"]].copy()
                    pie_data_t = pie_data_t.sort_values("매출비중_특이일", ascending=False)
                    top_cats = pie_data_t.head(8)
                    others_val = pie_data_t.iloc[8:]["매출비중_특이일"].sum() if len(pie_data_t) > 8 else 0
                    if others_val > 0:
                        top_cats = pd.concat([top_cats, pd.DataFrame({"erp_category": ["기타"], "매출비중_특이일": [others_val]})], ignore_index=True)
                    fig_pie_t = go.Figure(go.Pie(
                        labels=top_cats["erp_category"], values=top_cats["매출비중_특이일"],
                        hole=0.35, textinfo="label+percent", textposition="outside",
                        marker=dict(colors=["#E74C3C","#F1948A","#F5B7B1","#FADBD8","#E8DAEF","#D5F5E3","#D6EAF8","#FDEBD0","#D5DBDB"]),
                    ))
                    fig_pie_t.update_layout(
                        title=dict(text="고객 많은 날", x=0.5, font=dict(size=14)),
                        height=350, margin=dict(l=10, r=10, t=40, b=10),
                        showlegend=False,
                    )
                    with pie_col1:
                        st.plotly_chart(fig_pie_t, use_container_width=True)
                    # 보통날 파이
                    pie_data_n = cat_comp[["erp_category", "매출비중_보통날"]].copy()
                    pie_data_n = pie_data_n.sort_values("매출비중_보통날", ascending=False)
                    top_cats_n = pie_data_n.head(8)
                    others_val_n = pie_data_n.iloc[8:]["매출비중_보통날"].sum() if len(pie_data_n) > 8 else 0
                    if others_val_n > 0:
                        top_cats_n = pd.concat([top_cats_n, pd.DataFrame({"erp_category": ["기타"], "매출비중_보통날": [others_val_n]})], ignore_index=True)
                    fig_pie_n = go.Figure(go.Pie(
                        labels=top_cats_n["erp_category"], values=top_cats_n["매출비중_보통날"],
                        hole=0.35, textinfo="label+percent", textposition="outside",
                        marker=dict(colors=["#BDC3C7","#D5DBDB","#E8E8E8","#F0F0F0","#E8DAEF","#D5F5E3","#D6EAF8","#FDEBD0","#F5F5F5"]),
                    ))
                    fig_pie_n.update_layout(
                        title=dict(text="보통날", x=0.5, font=dict(size=14)),
                        height=350, margin=dict(l=10, r=10, t=40, b=10),
                        showlegend=False,
                    )
                    with pie_col2:
                        st.plotly_chart(fig_pie_n, use_container_width=True)

                # 카테고리 바차트 (매출 기반)
                st.markdown("##### 📂 카테고리별 일평균 매출 비교")
                if not cat_comp.empty:
                    fig_bc = go.Figure()
                    cs = cat_comp.sort_values("일평균 매출_특이일", ascending=True).tail(12)
                    fig_bc.add_trace(go.Bar(
                        y=cs["erp_category"], x=cs["일평균 매출_특이일"],
                        name="고객 많은 날", orientation="h", marker_color="#E74C3C",
                    ))
                    fig_bc.add_trace(go.Bar(
                        y=cs["erp_category"], x=cs["일평균 매출_보통날"],
                        name="보통날", orientation="h", marker_color="#BDC3C7",
                    ))
                    fig_bc.update_layout(
                        barmode="group", height=400,
                        margin=dict(l=0, r=20, t=10, b=30),
                        xaxis_title="일평균 매출(원)", xaxis_tickformat=",",
                        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                    )
                    st.plotly_chart(fig_bc, use_container_width=True)

                    # 비중변화 테이블
                    dc = cat_comp.rename(columns={"erp_category": "카테고리"})
                    st.dataframe(
                        dc[["카테고리", "일평균 매출_특이일", "매출비중_특이일",
                            "일평균 매출_보통날", "매출비중_보통날", "매출증가율", "비중변화"]],
                        use_container_width=True, height=400,
                        column_config={
                            "일평균 매출_특이일": st.column_config.NumberColumn("일평균매출(특이일)", format="%,.0f원"),
                            "일평균 매출_보통날": st.column_config.NumberColumn("일평균매출(보통날)", format="%,.0f원"),
                            "매출비중_특이일": st.column_config.NumberColumn("비중(특이일)", format="%.1f%%"),
                            "매출비중_보통날": st.column_config.NumberColumn("비중(보통날)", format="%.1f%%"),
                            "매출증가율": st.column_config.NumberColumn("매출 증가율", format="+%.1f%%"),
                            "비중변화": st.column_config.NumberColumn("비중변화(%p)", format="%+.1f"),
                        },
                    )

                # 상품 비교
                st.markdown("##### 🏆 고객 많은 날에 매출 비중이 눈에 띄게 올라간 상품")
                st.caption("보통날에도 판매 실적이 있는 상품 중, 특이일에 매출 비중(%p)이 가장 크게 상승한 순")
                prod_comp = busy_a.get("product_comparison", pd.DataFrame())
                if not prod_comp.empty:
                    dp = prod_comp.copy()
                    for c in ["일평균 매출_특이일", "일평균 매출_보통날"]:
                        if c in dp.columns:
                            dp[c] = dp[c].apply(lambda x: "{:,.0f}원".format(x))
                    display_cols = ["product_name", "erp_category",
                        "매출비중_특이일", "매출비중_보통날", "비중변화",
                        "일평균 매출_특이일", "일평균 매출_보통날"]
                    display_cols = [c for c in display_cols if c in dp.columns]
                    st.dataframe(
                        dp[display_cols].rename(columns={
                            "product_name": "상품명", "erp_category": "카테고리",
                        }),
                        use_container_width=True, height=500,
                        column_config={
                            "매출비중_특이일": st.column_config.NumberColumn("비중(특이일)", format="%.2f%%"),
                            "매출비중_보통날": st.column_config.NumberColumn("비중(보통날)", format="%.2f%%"),
                            "비중변화": st.column_config.NumberColumn("비중변화(%p)", format="%+.2f"),
                        },
                    )

                # 인사이트 자동 생성
                st.markdown("##### 💡 핵심 인사이트")
                busy_insights = []
                if detail is not None and not detail.empty:
                    days_of_week = detail["날짜"].tolist()
                    if all("Sat" in d for d in days_of_week):
                        busy_insights.append("모든 고객 많은 날이 **토요일** → 주말 고객 집중 패턴")
                aov_diff = bs.get("target_avg_aov", 0) - bs.get("normal_avg_aov", 0)
                if aov_diff > 0:
                    busy_insights.append(
                        "고객 많은 날 객단가도 {:+,.0f}원 **상승** → 고객수와 객단가 동반 상승".format(aov_diff)
                    )
                elif aov_diff < -500:
                    busy_insights.append(
                        "고객 많은 날 객단가가 {:+,.0f}원 **하락** → 소액 구매 고객 유입 증가".format(aov_diff)
                    )
                qty_diff = basket.get("특이일_평균상품수", 0) - basket.get("보통날_평균상품수", 0)
                if qty_diff > 0.1:
                    busy_insights.append(
                        "인당 평균 {:.1f}개 구매 (보통날 대비 +{:.1f}개) → 교차판매 기회".format(
                            basket.get("특이일_평균상품수", 0), qty_diff
                        )
                    )
                if not cat_comp.empty:
                    top_increase = cat_comp[cat_comp["비중변화"] >= 1.0].head(3)
                    if not top_increase.empty:
                        cats = ", ".join(top_increase["erp_category"].tolist())
                        busy_insights.append("비중 증가 카테고리: **{}**".format(cats))

                for ins in busy_insights:
                    st.markdown("- " + ins)
                if not busy_insights:
                    st.info("분석 데이터가 충분히 쌓이면 인사이트가 표시됩니다.")

        # ────────────────────────
        # TAB 2: 객단가 높은 날
        # ────────────────────────
        with tab_aov:
            if not aov_a:
                st.info("객단가 특이일이 감지되지 않았습니다.")
            else:
                avs = aov_a.get("summary", {})
                basket_aov = aov_a.get("basket_compare", {})

                # 일별 상세 테이블
                detail_aov = aov_a.get("daily_detail", pd.DataFrame())
                if not detail_aov.empty:
                    st.markdown("##### 📅 객단가 높은 날 일별 현황")
                    da = detail_aov[["날짜", "고객수", "객단가(원)", "매출(원)", "평균구매수량"]].copy()
                    da["객단가(원)"] = da["객단가(원)"].apply(lambda x: "{:,.0f}원".format(x))
                    da["매출(원)"] = da["매출(원)"].apply(lambda x: "{:,.0f}원".format(x))
                    da = da.rename(columns={"고객수": "고객수(명)"})
                    st.dataframe(da, use_container_width=True, hide_index=True)

                # KPI 비교
                st.markdown("##### 📊 객단가 높은 날 vs 보통날")
                ac1, ac2, ac3, ac4, ac5 = st.columns(5)
                ac1.metric("해당 일수", "{}일".format(avs.get("n_target", 0)))
                ac2.metric(
                    "평균 객단가",
                    "{:,.0f}원".format(avs.get("target_avg_aov", 0)),
                    delta="{:+,.0f}원".format(
                        avs.get("target_avg_aov", 0) - avs.get("normal_avg_aov", 0)
                    ),
                )
                ac3.metric(
                    "평균 고객수",
                    "{:.0f}명".format(avs.get("target_avg_orders", 0)),
                    delta="{:+.0f}명".format(
                        avs.get("target_avg_orders", 0) - avs.get("normal_avg_orders", 0)
                    ),
                )
                ac4.metric(
                    "일평균 매출",
                    "{:,.0f}만원".format(avs.get("target_avg_revenue", 0) / 10000),
                    delta="{:+,.0f}만원".format(
                        (avs.get("target_avg_revenue", 0) - avs.get("normal_avg_revenue", 0)) / 10000
                    ),
                )
                ac5.metric(
                    "인당 구매수량",
                    "{:.1f}개".format(basket_aov.get("특이일_평균상품수", 0)),
                    delta="{:+.1f}개".format(
                        basket_aov.get("특이일_평균상품수", 0) - basket_aov.get("보통날_평균상품수", 0)
                    ),
                )

                # 시간대 비교
                hourly_aov = aov_a.get("hourly_comparison", pd.DataFrame())
                if not hourly_aov.empty:
                    st.markdown("##### ⏰ 시간대별 매출 비교")
                    fig_hr2 = go.Figure()
                    fig_hr2.add_trace(go.Bar(
                        x=hourly_aov["hour"], y=hourly_aov["일평균 매출_특이일"],
                        name="객단가 높은 날", marker_color="#F39C12", opacity=0.8,
                    ))
                    fig_hr2.add_trace(go.Bar(
                        x=hourly_aov["hour"], y=hourly_aov["일평균 매출_보통날"],
                        name="보통날", marker_color="#BDC3C7", opacity=0.8,
                    ))
                    fig_hr2.update_layout(
                        barmode="group", height=300,
                        margin=dict(l=0, r=0, t=10, b=30),
                        xaxis_title="시간", yaxis_title="일평균 매출(원)", yaxis_tickformat=",",
                        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                    )
                    st.plotly_chart(fig_hr2, use_container_width=True)

                # 카테고리 매출비중 파이차트 비교
                st.markdown("##### 🥧 카테고리 매출비중 비교")
                cat_comp_aov = aov_a.get("category_comparison", pd.DataFrame())
                if not cat_comp_aov.empty:
                    apie_col1, apie_col2 = st.columns(2)
                    # 특이일 파이
                    apie_data_t = cat_comp_aov[["erp_category", "매출비중_특이일"]].copy()
                    apie_data_t = apie_data_t.sort_values("매출비중_특이일", ascending=False)
                    atop_cats = apie_data_t.head(8)
                    aothers_val = apie_data_t.iloc[8:]["매출비중_특이일"].sum() if len(apie_data_t) > 8 else 0
                    if aothers_val > 0:
                        atop_cats = pd.concat([atop_cats, pd.DataFrame({"erp_category": ["기타"], "매출비중_특이일": [aothers_val]})], ignore_index=True)
                    fig_apie_t = go.Figure(go.Pie(
                        labels=atop_cats["erp_category"], values=atop_cats["매출비중_특이일"],
                        hole=0.35, textinfo="label+percent", textposition="outside",
                        marker=dict(colors=["#F39C12","#F8C471","#FAD7A0","#FDEBD0","#E8DAEF","#D5F5E3","#D6EAF8","#F5B7B1","#D5DBDB"]),
                    ))
                    fig_apie_t.update_layout(
                        title=dict(text="객단가 높은 날", x=0.5, font=dict(size=14)),
                        height=350, margin=dict(l=10, r=10, t=40, b=10),
                        showlegend=False,
                    )
                    with apie_col1:
                        st.plotly_chart(fig_apie_t, use_container_width=True)
                    # 보통날 파이
                    apie_data_n = cat_comp_aov[["erp_category", "매출비중_보통날"]].copy()
                    apie_data_n = apie_data_n.sort_values("매출비중_보통날", ascending=False)
                    atop_cats_n = apie_data_n.head(8)
                    aothers_val_n = apie_data_n.iloc[8:]["매출비중_보통날"].sum() if len(apie_data_n) > 8 else 0
                    if aothers_val_n > 0:
                        atop_cats_n = pd.concat([atop_cats_n, pd.DataFrame({"erp_category": ["기타"], "매출비중_보통날": [aothers_val_n]})], ignore_index=True)
                    fig_apie_n = go.Figure(go.Pie(
                        labels=atop_cats_n["erp_category"], values=atop_cats_n["매출비중_보통날"],
                        hole=0.35, textinfo="label+percent", textposition="outside",
                        marker=dict(colors=["#BDC3C7","#D5DBDB","#E8E8E8","#F0F0F0","#E8DAEF","#D5F5E3","#D6EAF8","#FDEBD0","#F5F5F5"]),
                    ))
                    fig_apie_n.update_layout(
                        title=dict(text="보통날", x=0.5, font=dict(size=14)),
                        height=350, margin=dict(l=10, r=10, t=40, b=10),
                        showlegend=False,
                    )
                    with apie_col2:
                        st.plotly_chart(fig_apie_n, use_container_width=True)

                # 카테고리 비교 (매출 기반)
                st.markdown("##### 📂 카테고리별 차이 (일평균 매출 기준)")
                if not cat_comp_aov.empty:
                    fig_ac = go.Figure()
                    cs2 = cat_comp_aov.sort_values("일평균 매출_특이일", ascending=True).tail(12)
                    fig_ac.add_trace(go.Bar(
                        y=cs2["erp_category"], x=cs2["일평균 매출_특이일"],
                        name="객단가 높은 날", orientation="h", marker_color="#F39C12",
                    ))
                    fig_ac.add_trace(go.Bar(
                        y=cs2["erp_category"], x=cs2["일평균 매출_보통날"],
                        name="보통날", orientation="h", marker_color="#BDC3C7",
                    ))
                    fig_ac.update_layout(
                        barmode="group", height=400,
                        margin=dict(l=0, r=20, t=10, b=30),
                        xaxis_title="일평균 매출(원)", xaxis_tickformat=",",
                        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                    )
                    st.plotly_chart(fig_ac, use_container_width=True)

                    dc2 = cat_comp_aov.rename(columns={"erp_category": "카테고리"})
                    st.dataframe(
                        dc2[["카테고리", "일평균 매출_특이일", "매출비중_특이일",
                             "일평균 매출_보통날", "매출비중_보통날", "매출증가율", "비중변화"]],
                        use_container_width=True, height=400,
                        column_config={
                            "일평균 매출_특이일": st.column_config.NumberColumn("일평균매출(특이일)", format="%,.0f원"),
                            "일평균 매출_보통날": st.column_config.NumberColumn("일평균매출(보통날)", format="%,.0f원"),
                            "매출비중_특이일": st.column_config.NumberColumn("비중(특이일)", format="%.1f%%"),
                            "매출비중_보통날": st.column_config.NumberColumn("비중(보통날)", format="%.1f%%"),
                            "매출증가율": st.column_config.NumberColumn("매출 증가율", format="+%.1f%%"),
                            "비중변화": st.column_config.NumberColumn("비중변화(%p)", format="%+.1f"),
                        },
                    )

                # 상품 비교
                st.markdown("##### 🏆 객단가 높은 날에 매출 비중이 눈에 띄게 올라간 상품")
                st.caption("보통날에도 판매 실적이 있는 상품 중, 특이일에 매출 비중(%p)이 가장 크게 상승한 순")
                prod_comp_aov = aov_a.get("product_comparison", pd.DataFrame())
                if not prod_comp_aov.empty:
                    dp2 = prod_comp_aov.copy()
                    for c in ["일평균 매출_특이일", "일평균 매출_보통날"]:
                        if c in dp2.columns:
                            dp2[c] = dp2[c].apply(lambda x: "{:,.0f}원".format(x))
                    display_cols2 = ["product_name", "erp_category",
                        "매출비중_특이일", "매출비중_보통날", "비중변화",
                        "일평균 매출_특이일", "일평균 매출_보통날"]
                    display_cols2 = [c for c in display_cols2 if c in dp2.columns]
                    st.dataframe(
                        dp2[display_cols2].rename(columns={
                            "product_name": "상품명", "erp_category": "카테고리",
                        }),
                        use_container_width=True, height=500,
                        column_config={
                            "매출비중_특이일": st.column_config.NumberColumn("비중(특이일)", format="%.2f%%"),
                            "매출비중_보통날": st.column_config.NumberColumn("비중(보통날)", format="%.2f%%"),
                            "비중변화": st.column_config.NumberColumn("비중변화(%p)", format="%+.2f"),
                        },
                    )

                # 인사이트 자동 생성
                st.markdown("##### 💡 핵심 인사이트")
                aov_insights = []
                cust_diff = avs.get("target_avg_orders", 0) - avs.get("normal_avg_orders", 0)
                if abs(cust_diff) < 20:
                    aov_insights.append(
                        "고객수는 비슷(차이 {:+.0f}명)한데 객단가만 높음 → **고단가 상품 판매**가 핵심".format(cust_diff)
                    )
                elif cust_diff > 20:
                    aov_insights.append(
                        "고객수도 {:+.0f}명 많음 → 고객수와 객단가 동반 상승".format(cust_diff)
                    )
                qty_diff_aov = basket_aov.get("특이일_평균상품수", 0) - basket_aov.get("보통날_평균상품수", 0)
                price_diff = basket_aov.get("특이일_평균주문금액", 0) - basket_aov.get("보통날_평균주문금액", 0)
                if qty_diff_aov < -0.05 and price_diff > 2000:
                    aov_insights.append(
                        "구매 수량은 적지만({:+.1f}개) 주문금액은 {:+,.0f}원 높음 → **단가 높은 상품** 구매".format(
                            qty_diff_aov, price_diff
                        )
                    )
                elif qty_diff_aov > 0.1:
                    aov_insights.append(
                        "인당 구매수량도 +{:.1f}개 → 더 많이, 더 비싸게 구매".format(qty_diff_aov)
                    )
                if not cat_comp_aov.empty:
                    top_increase_aov = cat_comp_aov[cat_comp_aov["비중변화"] >= 1.0].head(3)
                    if not top_increase_aov.empty:
                        cats = ", ".join(top_increase_aov["erp_category"].tolist())
                        aov_insights.append("비중 증가 카테고리: **{}**".format(cats))

                for ins in aov_insights:
                    st.markdown("- " + ins)
                if not aov_insights:
                    st.info("분석 데이터가 충분히 쌓이면 인사이트가 표시됩니다.")


# ══════════════════════════════════════
#  페이지 4: 약국 화장품 매니저 대시보드
# ══════════════════════════════════════

def page_cosmetics_dashboard():
    """약국 화장품 Sales Manager 전용 대시보드"""
    from cosmetics_analysis import run_cosmetics_analysis, get_available_months
    from supabase_client import fetch_orders, flatten_order_items, fetch_products, is_supabase_configured

    if not is_supabase_configured():
        st.error("Supabase 연결이 필요합니다.")
        return

    st.markdown("# 💄 약국 화장품 대시보드")
    st.caption("피부 건강 > 약국 화장품 카테고리 전용 판매 성과 모니터링")

    # 데이터 로드
    @st.cache_data(ttl=3600)
    def _load_cosmetics():
        products = fetch_products()
        orders = fetch_orders("2025-12-08", "2026-03-31")
        items = flatten_order_items(orders, products)
        return {"order_items": items}, products

    with st.spinner("데이터 로딩 중..."):
        data, products = _load_cosmetics()

    # 사용 가능한 월 목록
    available_months = get_available_months(data, products)
    if not available_months:
        st.error("약국 화장품 판매 데이터가 없습니다.")
        return

    # 기본 월: 완결된 직전 월 (현재월 데이터가 15일 미만이면 전월을 기본값)
    import datetime as _dt
    _today = _dt.date.today()
    _current_ym = _today.strftime("%Y-%m")
    if len(available_months) >= 2 and available_months[0] == _current_ym and _today.day < 15:
        default_month_idx = 1  # 직전 완결 월
    else:
        default_month_idx = 0  # 현재 월 (데이터 충분)

    # 월 선택 드롭다운
    col_month_sel, col_month_info = st.columns([1, 3])
    with col_month_sel:
        selected_month = st.selectbox(
            "📅 분석 기준월",
            options=available_months,
            index=default_month_idx,
            key="cos_month_select",
        )
    with col_month_info:
        if selected_month == _current_ym:
            st.info(f"⚠️ {selected_month}은 진행 중인 월입니다 (현재 {_today.day}일차). 완결 데이터는 직전 월을 선택하세요.")

    # 분석 실행
    result = run_cosmetics_analysis(data, products, year_month_override=selected_month)

    if "error" in result:
        st.error(result["error"])
        return

    ref_date = result["reference_date"]
    dr = result["daily_report"]
    wr = result["weekly_report"]
    mr = result["monthly_report"]
    daily_trend = result["daily_trend"]

    # ── 상단 KPI 요약 (선택 월 기준) ──
    tm = mr["this_month"]
    pm = mr["prev_month"]

    k1, k2, k3, k4, k5 = st.columns(5)
    k1.metric(
        f"{selected_month} 매출",
        "{:,.0f}만원".format(tm["매출"] / 10000),
        delta="{:+,.0f}만원".format((tm["매출"] - pm["매출"]) / 10000) if pm["매출"] else None,
    )
    k2.metric(
        "객단가",
        "{:,.0f}원".format(tm["객단가"]),
        delta="{:+,.0f}원".format(tm["객단가"] - pm["객단가"]) if pm["객단가"] else None,
    )
    k3.metric(
        "일평균 매출",
        "{:,.0f}원".format(tm["일평균매출"]),
        delta="{:+,.0f}원".format(tm["일평균매출"] - pm["일평균매출"]) if pm["일평균매출"] else None,
    )
    k4.metric(
        "판매 SKU",
        "{}/{}개".format(mr["sold_sku_count"], mr["total_sku_count"]),
    )
    k5.metric(
        "판매0 SKU",
        "{}개".format(mr["zero_sales_count"]),
        delta="퇴출 검토" if mr["zero_sales_count"] > 0 else None,
        delta_color="inverse",
    )

    st.divider()

    # ── 탭 구성 ──
    tab_daily, tab_weekly, tab_monthly = st.tabs([
        "📅 일일 리포트", "📆 주간 리포트", "📊 월간 리포트",
    ])

    # ════════════════════════════════════
    # TAB 1: 일일 리포트
    # ════════════════════════════════════
    with tab_daily:
        st.markdown("### 📅 일일 판매 현황 — {}".format(dr["date_str"]))

        # 전일/전주동요일 비교
        t = dr["today"]
        y = dr["yesterday"]
        lw = dr["lastweek"]

        dc1, dc2, dc3, dc4 = st.columns(4)
        dc1.metric(
            "매출", "{:,.0f}원".format(t["매출"]),
            delta="{:+,.0f}원 (전일)".format(t["매출"] - y["매출"]) if y["매출"] else None,
        )
        dc2.metric(
            "건수 (고객)", "{}건".format(t["건수"]),
            delta="{:+d}건 (전일)".format(t["건수"] - y["건수"]) if y["건수"] else None,
        )
        dc3.metric(
            "객단가", "{:,.0f}원".format(t["객단가"]),
            delta="{:+,.0f}원 (전일)".format(t["객단가"] - y["객단가"]) if y["객단가"] else None,
        )
        dc4.metric(
            "vs 전주 동요일",
            "{:,.0f}원".format(lw["매출"]),
            delta="{:+,.0f}원".format(t["매출"] - lw["매출"]) if lw["매출"] else None,
        )

        # SKU TOP 5
        col_sku, col_hourly = st.columns([1, 1])
        with col_sku:
            st.markdown("##### 🏆 오늘의 TOP 상품")
            sku_top = dr.get("sku_top", pd.DataFrame())
            if not sku_top.empty:
                st.dataframe(
                    sku_top.head(10).rename(columns={"product_name": "상품명"}).assign(
                        매출=lambda x: x["매출"].apply(lambda v: "{:,.0f}원".format(v))
                    )[["상품명", "매출", "수량"]],
                    use_container_width=True, hide_index=True,
                )
            else:
                st.info("판매 데이터 없음")

        with col_hourly:
            st.markdown("##### ⏰ 시간대별 판매")
            hourly = dr.get("hourly", pd.DataFrame())
            if not hourly.empty:
                fig_h = go.Figure()
                fig_h.add_trace(go.Bar(
                    x=hourly["hour"], y=hourly["매출"],
                    name="매출", marker_color="#E91E63", opacity=0.8,
                ))
                fig_h.update_layout(
                    height=280, margin=dict(l=0, r=0, t=10, b=30),
                    xaxis_title="시간", yaxis_title="매출(원)", yaxis_tickformat=",",
                )
                st.plotly_chart(fig_h, use_container_width=True)
            else:
                st.info("시간대 데이터 없음")

        # 최근 7일 추이
        st.markdown("##### 📈 최근 7일 매출 추이")
        recent7 = daily_trend.tail(7)
        if not recent7.empty:
            fig_r7 = go.Figure()
            fig_r7.add_trace(go.Bar(
                x=recent7["date_str"], y=recent7["매출"],
                marker_color="#E91E63", opacity=0.8, name="매출",
            ))
            fig_r7.add_trace(go.Scatter(
                x=recent7["date_str"], y=recent7["객단가"],
                mode="lines+markers", name="객단가",
                yaxis="y2", line=dict(color="#FF9800", width=2),
            ))
            fig_r7.update_layout(
                height=300, margin=dict(l=0, r=40, t=10, b=30),
                yaxis=dict(title="매출(원)", tickformat=","),
                yaxis2=dict(title="객단가(원)", overlaying="y", side="right", tickformat=","),
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            )
            st.plotly_chart(fig_r7, use_container_width=True)

    # ════════════════════════════════════
    # TAB 2: 주간 리포트
    # ════════════════════════════════════
    with tab_weekly:
        st.markdown("### 📆 주간 판매 현황")

        tw = wr["this_week"]
        pw = wr["prev_week"]
        rev_diff = tw["매출"] - pw["매출"]
        rev_pct = (rev_diff / max(pw["매출"], 1) * 100)

        st.markdown("**이번 주** ({})  vs  **전주** ({})".format(wr["week_range"], wr["prev_range"]))

        wc1, wc2, wc3, wc4 = st.columns(4)
        wc1.metric("이번주 매출", "{:,.0f}원".format(tw["매출"]),
                    delta="{:+,.0f}원 ({:+.1f}%)".format(rev_diff, rev_pct))
        wc2.metric("건수", "{}건".format(tw["건수"]),
                    delta="{:+d}건".format(tw["건수"] - pw["건수"]))
        wc3.metric("객단가", "{:,.0f}원".format(tw["객단가"]),
                    delta="{:+,.0f}원".format(tw["객단가"] - pw["객단가"]))
        wc4.metric("판매수량", "{}개".format(tw["수량"]),
                    delta="{:+d}개".format(tw["수량"] - pw["수량"]))

        # 이번주 일별 상세
        col_wd, col_ws = st.columns([1, 1])

        with col_wd:
            st.markdown("##### 📅 이번 주 일별 현황")
            tw_daily = wr.get("this_week_daily", pd.DataFrame())
            if not tw_daily.empty:
                td = tw_daily[["date_str", "매출", "건수", "객단가"]].copy()
                td["매출"] = td["매출"].apply(lambda x: "{:,.0f}원".format(x))
                td["객단가"] = td["객단가"].apply(lambda x: "{:,.0f}원".format(x))
                st.dataframe(
                    td.rename(columns={"date_str": "날짜"}),
                    use_container_width=True, hide_index=True,
                )

        with col_ws:
            st.markdown("##### 🏆 이번 주 TOP 상품")
            sku_w = wr.get("sku_ranking", pd.DataFrame())
            if not sku_w.empty:
                sw = sku_w.head(10).copy()
                sw["매출"] = sw["매출"].apply(lambda x: "{:,.0f}원".format(x))
                st.dataframe(
                    sw.rename(columns={"product_name": "상품명"})[["상품명", "매출", "수량"]],
                    use_container_width=True, hide_index=True,
                )

        # 이번주 vs 전주 일별 차트
        st.markdown("##### 📊 이번 주 vs 전주 일별 매출")
        tw_d = wr.get("this_week_daily", pd.DataFrame())
        pw_d = wr.get("prev_week_daily", pd.DataFrame())
        if not tw_d.empty:
            fig_wk = go.Figure()
            dow_kr = {"Monday": "월", "Tuesday": "화", "Wednesday": "수",
                      "Thursday": "목", "Friday": "금", "Saturday": "토", "Sunday": "일"}
            tw_d = tw_d.copy()
            tw_d["요일_kr"] = tw_d["요일"].map(dow_kr)
            fig_wk.add_trace(go.Bar(
                x=tw_d["요일_kr"], y=tw_d["매출"],
                name="이번주", marker_color="#E91E63", opacity=0.8,
            ))
            if not pw_d.empty:
                pw_d = pw_d.copy()
                pw_d["요일_kr"] = pw_d["요일"].map(dow_kr)
                fig_wk.add_trace(go.Bar(
                    x=pw_d["요일_kr"], y=pw_d["매출"],
                    name="전주", marker_color="#BDBDBD", opacity=0.8,
                ))
            fig_wk.update_layout(
                barmode="group", height=300,
                margin=dict(l=0, r=0, t=10, b=30),
                yaxis_title="매출(원)", yaxis_tickformat=",",
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            )
            st.plotly_chart(fig_wk, use_container_width=True)

    # ════════════════════════════════════
    # TAB 3: 월간 리포트
    # ════════════════════════════════════
    with tab_monthly:
        st.markdown("### 📊 월간 종합 리포트 — {}".format(selected_month))

        mc1, mc2, mc3, mc4, mc5 = st.columns(5)
        mc1.metric("월 매출", "{:,.0f}만원".format(tm["매출"] / 10000),
                    delta="{:+,.0f}만원 (전월)".format((tm["매출"] - pm["매출"]) / 10000) if pm["매출"] else None)
        mc2.metric("객단가", "{:,.0f}원".format(tm["객단가"]),
                    delta="{:+,.0f}원".format(tm["객단가"] - pm["객단가"]) if pm["객단가"] else None)
        mc3.metric("총 건수", "{}건".format(tm["건수"]),
                    delta="{:+d}건".format(tm["건수"] - pm["건수"]) if pm["건수"] else None)
        mc4.metric("일평균 매출", "{:,.0f}원".format(tm["일평균매출"]),
                    delta="{:+,.0f}원".format(tm["일평균매출"] - pm["일평균매출"]) if pm["일평균매출"] else None)
        mc5.metric("영업일수", "{}일".format(tm["일수"]))

        # 일별 매출 추이 (월간)
        st.markdown("##### 📈 일별 매출 추이")
        m_daily = mr.get("this_month_daily", pd.DataFrame())
        if not m_daily.empty:
            fig_md = go.Figure()
            fig_md.add_trace(go.Bar(
                x=m_daily["date_str"], y=m_daily["매출"],
                marker_color="#E91E63", opacity=0.8, name="매출",
            ))
            avg_line = tm["일평균매출"]
            fig_md.add_hline(y=avg_line, line_dash="dash", line_color="gray",
                             annotation_text="일평균 {:,.0f}원".format(avg_line))
            fig_md.update_layout(
                height=300, margin=dict(l=0, r=0, t=10, b=30),
                yaxis_title="매출(원)", yaxis_tickformat=",",
            )
            st.plotly_chart(fig_md, use_container_width=True)

        # 요일별 패턴 + 시간대별 패턴
        col_dow, col_hour = st.columns([1, 1])

        with col_dow:
            st.markdown("##### 📅 요일별 일평균 매출")
            dow = mr.get("day_of_week", pd.DataFrame())
            if not dow.empty:
                fig_dow = go.Figure(go.Bar(
                    x=dow["요일_kr"], y=dow["일평균매출"],
                    marker_color="#E91E63", opacity=0.8,
                ))
                fig_dow.update_layout(
                    height=280, margin=dict(l=0, r=0, t=10, b=30),
                    yaxis_title="일평균 매출(원)", yaxis_tickformat=",",
                )
                st.plotly_chart(fig_dow, use_container_width=True)

        with col_hour:
            st.markdown("##### ⏰ 시간대별 일평균 매출")
            hourly_m = mr.get("hourly", pd.DataFrame())
            if not hourly_m.empty:
                fig_hm = go.Figure(go.Bar(
                    x=hourly_m["hour"], y=hourly_m["일평균매출"],
                    marker_color="#E91E63", opacity=0.8,
                ))
                fig_hm.update_layout(
                    height=280, margin=dict(l=0, r=0, t=10, b=30),
                    xaxis_title="시간", yaxis_title="일평균 매출(원)", yaxis_tickformat=",",
                )
                st.plotly_chart(fig_hm, use_container_width=True)

        # 객단가 분포
        st.markdown("##### 💰 객단가 분포 (화장품 주문 건별)")
        price_dist = mr.get("price_distribution", pd.DataFrame())
        if not price_dist.empty:
            fig_pd = go.Figure(go.Bar(
                x=price_dist["가격대"].astype(str), y=price_dist["건수"],
                marker_color="#E91E63", opacity=0.8,
            ))
            fig_pd.update_layout(
                height=250, margin=dict(l=0, r=0, t=10, b=30),
                xaxis_title="주문 가격대", yaxis_title="건수",
            )
            st.plotly_chart(fig_pd, use_container_width=True)

        # SKU 랭킹
        col_top, col_bottom = st.columns(2)

        with col_top:
            st.markdown("##### 🥇 매출 TOP 10 상품")
            top10 = mr.get("sku_top10", pd.DataFrame())
            if not top10.empty:
                t10 = top10.copy()
                t10["매출"] = t10["매출"].apply(lambda x: "{:,.0f}원".format(x))
                st.dataframe(
                    t10[["순위", "product_name", "매출", "수량", "건수"]].rename(
                        columns={"product_name": "상품명"}
                    ),
                    use_container_width=True, hide_index=True,
                )

        with col_bottom:
            st.markdown("##### 🔻 매출 하위 10 상품")
            bot10 = mr.get("sku_bottom10", pd.DataFrame())
            if not bot10.empty:
                b10 = bot10.copy()
                b10["매출"] = b10["매출"].apply(lambda x: "{:,.0f}원".format(x))
                st.dataframe(
                    b10[["product_name", "매출", "수량", "건수"]].rename(
                        columns={"product_name": "상품명"}
                    ),
                    use_container_width=True, hide_index=True,
                )

        # 판매 0 SKU (퇴출 후보)
        st.markdown("##### ⚠️ 이번 달 판매 0건 SKU ({}/{}개)".format(
            mr["zero_sales_count"], mr["total_sku_count"]))
        if mr["zero_sales_count"] > 0:
            st.caption("30일간 판매 실적이 없는 상품 — 퇴출 또는 기획 매대 편입 검토")
            zero_list = mr.get("zero_sales_skus", [])
            # 3열로 표시
            cols = st.columns(3)
            for i, name in enumerate(zero_list):
                cols[i % 3].markdown(f"- {name}")
        else:
            st.success("모든 SKU가 이번 달 판매 실적이 있습니다.")


# ══════════════════════════════════════
#  쇼카드 제작 페이지
# ══════════════════════════════════════

def page_showcard():
    st.title("🏷️ 쇼카드 제작")
    st.caption("매대 쇼카드를 직접 제작하고 인쇄용 PDF/SVG로 다운로드합니다.")

    from supabase_client import is_supabase_configured, fetch_products
    from trend_config import SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY

    # ── 상수 ──
    SC_COLORS = {
        "진통/해열": "#d6211a", "소화/위장": "#5e9e33", "잇몸/치과": "#e61a40",
        "치질": "#94c96e", "비염/코": "#14a1ad", "눈건강": "#2e2b85",
        "피부/연고": "#e61778", "여성건강": "#a80d82", "간/영양": "#146133",
        "소화효소": "#1c2e6e", "탈모": "#1c9ecc", "관절": "#0a82c2",
        "감기약": "#d6211a", "알레르기・비염약": "#14a1ad", "위장 건강": "#5e9e33",
        "피로회복・종합영양": "#146133", "근육・파스": "#0a82c2", "피부 건강": "#e61778",
        "구강": "#e61a40", "이너뷰티": "#a80d82", "생활건강": "#5e9e33",
    }
    SC_SIZES = {
        "S": {"w": 54, "label": "S (54mm)", "desc": "진열폭 3-5cm"},
        "M": {"w": 70, "label": "M (70mm)", "desc": "진열폭 5-7cm"},
        "L": {"w": 90, "label": "L (90mm)", "desc": "진열폭 7-11cm"},
        "XL": {"w": 110, "label": "XL (110mm)", "desc": "진열폭 11-15cm"},
        "XXL": {"w": 150, "label": "XXL (150mm)", "desc": "진열폭 15cm+"},
    }

    def _sc_color(cat):
        if not cat: return "#5e9e33"
        for k, c in SC_COLORS.items():
            if k in cat or cat in k: return c
        return "#5e9e33"

    def _esc(s):
        return (s or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")

    def _fit(text, max_w, max_fs, min_fs=8):
        for fs in range(int(max_fs), int(min_fs) - 1, -1):
            if len(text) * fs * 0.55 <= max_w: return fs
        return min_fs

    def _badge(bt, x, y, wp):
        if bt == "none": return ""
        info = {"동일성분": ("동일성분", "저렴해요"), "유사성분": ("유사성분", "저렴해요"), "업그레이드": ("업그레이드", None)}.get(bt)
        if not info: return ""
        bw = min(wp * 0.4, 60)
        s = (f'<rect x="{x}" y="{y}" width="{bw}" height="16" rx="8" fill="rgba(0,0,0,0.25)"/>'
             f'<text x="{x+bw/2}" y="{y+11.5}" text-anchor="middle" fill="white" font-size="8" font-weight="700">{_esc(info[0])}</text>')
        if info[1]:
            x2 = x + bw + 4; bw2 = min(wp * 0.35, 52)
            s += (f'<rect x="{x2}" y="{y}" width="{bw2}" height="16" rx="8" fill="rgba(255,255,255,0.3)"/>'
                  f'<text x="{x2+bw2/2}" y="{y+11.5}" text-anchor="middle" fill="white" font-size="8" font-weight="700">{_esc(info[1])}</text>')
        return s

    def _design_a(wp, hp, bg, bt, l1, l2, l3):
        r, p = 8, 8; fs3 = _fit(l3, wp-p*2, 28, 12); fs1 = _fit(l1 or "", wp-p*2, 12, 8)
        t = ""
        if l1: t += f'<text x="{wp/2}" y="{hp*0.42}" text-anchor="middle" fill="white" font-size="{fs1}" font-family="sans-serif" opacity="0.9">{_esc(l1)}</text>'
        if l2: t += f'<text x="{wp/2}" y="{hp*0.56}" text-anchor="middle" fill="white" font-size="{max(fs1-1,8)}" font-family="sans-serif" opacity="0.8">{_esc(l2)}</text>'
        return (f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {wp} {hp}" width="{wp}" height="{hp}">'
                f'<rect width="{wp}" height="{hp}" rx="{r}" fill="{bg}"/>{_badge(bt,p,p,wp)}{t}'
                f'<text x="{wp/2}" y="{hp*0.82}" text-anchor="middle" fill="white" font-size="{fs3}" font-weight="800" font-family="sans-serif">{_esc(l3)}</text></svg>')

    def _design_b(wp, hp, bg, bt, l1, l2, l3):
        r, p = 8, 8; oh = hp*0.38; fs3 = _fit(l3, wp-p*2, 26, 12); fs1 = _fit(l1 or "", wp-p*2, 11, 7)
        t = ""
        if l1: t += f'<text x="{wp/2}" y="{hp*0.35}" text-anchor="middle" fill="white" font-size="{fs1}" font-family="sans-serif" opacity="0.9">{_esc(l1)}</text>'
        if l2: t += f'<text x="{wp/2}" y="{hp*0.50}" text-anchor="middle" fill="white" font-size="{max(fs1-1,7)}" font-family="sans-serif" opacity="0.8">{_esc(l2)}</text>'
        return (f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {wp} {hp}" width="{wp}" height="{hp}">'
                f'<rect width="{wp}" height="{hp}" rx="{r}" fill="{bg}"/>'
                f'<rect y="{hp-oh}" width="{wp}" height="{oh}" fill="rgba(0,0,0,0.18)"/>'
                f'{_badge(bt,p,p,wp)}{t}'
                f'<text x="{wp/2}" y="{hp*0.84}" text-anchor="middle" fill="white" font-size="{fs3}" font-weight="800" font-family="sans-serif">{_esc(l3)}</text></svg>')

    def _design_c(wp, hp, bg, bt, l1, l2, l3):
        r = 8; bw = wp*0.12; pad = bw+8; fs3 = _fit(l3, wp-pad-8, 24, 11); fs1 = _fit(l1 or "", wp-pad-8, 11, 7)
        t = ""
        if bt != "none": t += f'<text x="{pad}" y="18" fill="{bg}" font-size="8" font-weight="700" font-family="sans-serif">{_esc(bt)}</text>'
        if l1: t += f'<text x="{pad}" y="{hp*0.40}" fill="{bg}" font-size="{fs1}" font-family="sans-serif">{_esc(l1)}</text>'
        if l2: t += f'<text x="{pad}" y="{hp*0.55}" fill="{bg}" font-size="{max(fs1-1,7)}" font-family="sans-serif" opacity="0.7">{_esc(l2)}</text>'
        return (f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {wp} {hp}" width="{wp}" height="{hp}">'
                f'<rect width="{wp}" height="{hp}" rx="{r}" fill="{bg}33"/>'
                f'<rect width="{bw}" height="{hp}" rx="{r} 0 0 {r}" fill="{bg}"/>{t}'
                f'<text x="{pad}" y="{hp*0.82}" fill="{bg}" font-size="{fs3}" font-weight="800" font-family="sans-serif">{_esc(l3)}</text></svg>')

    # Supabase 쇼카드 저장/조회
    def _sb_showcard_client():
        if not is_supabase_configured(): return None
        try:
            from supabase import create_client
            return create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)
        except Exception:
            return None

    def _save_showcard(data):
        sb = _sb_showcard_client()
        if sb:
            sb.table("showcards").insert(data).execute()

    def _load_showcard_history():
        sb = _sb_showcard_client()
        if not sb: return []
        try:
            res = sb.table("showcards").select("*").order("created_at", desc=True).limit(30).execute()
            return res.data or []
        except Exception:
            return []

    # ── 상품 로드 ──
    @st.cache_data(ttl=600)
    def _load_products():
        try:
            return fetch_products()
        except Exception:
            return pd.DataFrame()

    products_df = _load_products()
    if products_df.empty:
        st.warning("상품 데이터를 불러올 수 없습니다. Supabase 연결을 확인해주세요.")
        return

    product_names = sorted(products_df["name"].dropna().unique().tolist())

    # ── Step 1: 상품 선택 ──
    st.subheader("1️⃣ 상품 선택")
    sc_product = st.selectbox("상품 검색", [""] + product_names, index=0, key="sc_product")

    if not sc_product:
        # 이력만 표시
        st.markdown("---")
        st.subheader("📋 최근 제작 이력")
        hist = _load_showcard_history()
        if hist:
            design_names = {1: "클래식", 2: "모던", 3: "미니멀"}
            hist_rows = [{"제품명": h.get("product_name",""), "사이즈": h.get("size_class",""),
                          "디자인": design_names.get(h.get("selected_design"),""), "워딩": h.get("wording_line1",""),
                          "제작일": pd.to_datetime(h.get("created_at","")).strftime("%Y-%m-%d %H:%M") if h.get("created_at") else ""} for h in hist]
            st.dataframe(pd.DataFrame(hist_rows), use_container_width=True, hide_index=True)
        else:
            st.info("아직 제작 이력이 없습니다.")
        return

    # 상품 정보
    p_row = products_df[products_df["name"] == sc_product].iloc[0]
    category = p_row.get("erp_category", "") or ""
    st.info(f"📦 **{sc_product}** — 카테고리: {category or '미분류'}")

    # ── Step 2: 사이즈 & 색상 ──
    st.subheader("2️⃣ 사이즈 & 색상")
    col_sz, col_clr = st.columns(2)
    with col_sz:
        size_opts = list(SC_SIZES.keys())
        sc_size = st.selectbox("사이즈", size_opts, index=1,
                               format_func=lambda x: f"{SC_SIZES[x]['label']} — {SC_SIZES[x]['desc']}", key="sc_size")
    with col_clr:
        sc_color = st.color_picker("배경 색상", _sc_color(category), key="sc_color")

    # ── Step 3: 뱃지 ──
    st.subheader("3️⃣ 뱃지 타입")
    sc_badge = st.radio("뱃지", ["none", "동일성분", "유사성분", "업그레이드"],
                        format_func=lambda x: {"none":"없음","동일성분":"동일성분 + 저렴해요","유사성분":"유사성분 + 저렴해요","업그레이드":"업그레이드"}.get(x,x),
                        horizontal=True, key="sc_badge")

    # ── Step 4: 워딩 ──
    st.subheader("4️⃣ 워딩 입력")
    col_w1, col_w2 = st.columns(2)
    with col_w1:
        sc_l1 = st.text_input("1줄: 소구 포인트", placeholder="예: 같은 성분, 더 저렴하게", key="sc_l1")
        sc_l2 = st.text_input("2줄: 부가 설명 (선택)", key="sc_l2")
        sc_l3 = st.text_input("3줄: 제품명", value=sc_product, key="sc_l3")

    with col_w2:
        st.markdown("**✨ AI 워딩 제안**")
        if st.button("AI 카피 생성", key="sc_ai_btn", use_container_width=True):
            with st.spinner("AI가 카피를 생성 중..."):
                try:
                    import anthropic, json
                    client = anthropic.Anthropic()
                    prompt = (f"마트약국 쇼카드 카피라이터. 원본 기반 2가지 대안 제안. 한 줄 15자 이내.\n"
                              f"제품명: {sc_product}\n카테고리: {category}\n뱃지: {sc_badge}\n"
                              f"원본: 1줄:{sc_l1} / 2줄:{sc_l2} / 3줄:{sc_l3}\n"
                              f'JSON만: {{"variantA":{{"line1":"...","line2":"...","line3":"{sc_product}"}},'
                              f'"variantB":{{"line1":"...","line2":"...","line3":"{sc_product}"}}}}')
                    resp = client.messages.create(model="claude-haiku-4-5-20251001", max_tokens=300,
                                                 messages=[{"role":"user","content":prompt}])
                    t = resp.content[0].text.strip()
                    if "{" in t: t = t[t.index("{"):t.rindex("}")+1]
                    st.session_state["sc_ai"] = json.loads(t)
                except ImportError:
                    st.session_state["sc_ai"] = {
                        "variantA": {"line1": (sc_l1[:8]+"!" if sc_l1 else "추천 제품"), "line2": "가성비 최고", "line3": sc_product},
                        "variantB": {"line1": {"동일성분":"같은 성분, 더 저렴하게","유사성분":"비슷한 효과, 합리적 가격","업그레이드":"한 단계 업그레이드"}.get(sc_badge,"약사 추천"), "line2": category, "line3": sc_product},
                    }
                    st.warning("anthropic 미설치 — 규칙 기반 폴백 적용")
                except Exception as e:
                    st.error(f"AI 실패: {e}")

        if "sc_ai" in st.session_state:
            ai = st.session_state["sc_ai"]
            wc = st.radio("워딩 선택", ["내가 쓴 워딩", "AI A (임팩트)", "AI B (설득력)"], key="sc_wc")
            if wc == "AI A (임팩트)":
                v = ai["variantA"]; st.caption(f'{v["line1"]} / {v["line2"]} / {v["line3"]}')
            elif wc == "AI B (설득력)":
                v = ai["variantB"]; st.caption(f'{v["line1"]} / {v["line2"]} / {v["line3"]}')

    # 최종 워딩 결정
    fl1, fl2, fl3, ws = sc_l1, sc_l2, sc_l3, "original"
    if "sc_ai" in st.session_state and "sc_wc" in st.session_state:
        ai = st.session_state["sc_ai"]
        if st.session_state["sc_wc"] == "AI A (임팩트)":
            v = ai["variantA"]; fl1, fl2, fl3, ws = v["line1"], v["line2"], v["line3"], "ai_a"
        elif st.session_state["sc_wc"] == "AI B (설득력)":
            v = ai["variantB"]; fl1, fl2, fl3, ws = v["line1"], v["line2"], v["line3"], "ai_b"

    # ── Step 5: 디자인 프리뷰 ──
    st.subheader("5️⃣ 디자인 프리뷰")
    sz = SC_SIZES[sc_size]; wp, hp = sz["w"]*3, 65*3
    svg_a = _design_a(wp, hp, sc_color, sc_badge, fl1, fl2, fl3)
    svg_b = _design_b(wp, hp, sc_color, sc_badge, fl1, fl2, fl3)
    svg_c = _design_c(wp, hp, sc_color, sc_badge, fl1, fl2, fl3)

    c1, c2, c3 = st.columns(3)
    with c1: st.markdown("**A. 클래식**"); st.markdown(svg_a, unsafe_allow_html=True)
    with c2: st.markdown("**B. 모던**"); st.markdown(svg_b, unsafe_allow_html=True)
    with c3: st.markdown("**C. 미니멀**"); st.markdown(svg_c, unsafe_allow_html=True)

    sc_design = st.radio("디자인 선택", ["A. 클래식", "B. 모던", "C. 미니멀"], horizontal=True, key="sc_dc")
    di = {"A. 클래식": 0, "B. 모던": 1, "C. 미니멀": 2}[sc_design]
    sel_svg = [svg_a, svg_b, svg_c][di]

    # ── Step 6: 다운로드 ──
    st.subheader("6️⃣ 다운로드")
    dl = ["classic", "modern", "minimal"][di]
    fname = f"showcard_{sc_product}_{sc_size}_{dl}"

    dc1, dc2 = st.columns(2)
    with dc1:
        st.download_button("📥 SVG 다운로드", data=sel_svg, file_name=f"{fname}.svg", mime="image/svg+xml",
                           use_container_width=True, key="sc_dl_svg")
    with dc2:
        # PDF 생성 시도
        pdf_ok = False
        try:
            import cairosvg
            from io import BytesIO as _BIO
            pdf_data = cairosvg.svg2pdf(bytestring=sel_svg.encode("utf-8"))
            st.download_button("📥 PDF 다운로드 (인쇄용)", data=pdf_data, file_name=f"{fname}.pdf",
                               mime="application/pdf", use_container_width=True, key="sc_dl_pdf")
            pdf_ok = True
        except ImportError:
            st.caption("PDF 변환: `pip install cairosvg` 필요")

    # 이력 저장
    if st.button("💾 이력 저장", key="sc_save", use_container_width=True):
        try:
            _save_showcard({
                "product_name": sc_product, "category": category, "badge_type": sc_badge,
                "appeal_text": fl1, "wording_line1": fl1, "wording_line2": fl2, "wording_line3": fl3,
                "wording_source": ws, "size_class": sc_size, "card_width_mm": sz["w"],
                "card_height_mm": 65, "bg_color": sc_color, "selected_design": di + 1,
            })
            st.success("이력 저장 완료!")
        except Exception as e:
            st.error(f"저장 실패: {e}")

    # ── 제작 이력 ──
    st.markdown("---")
    st.subheader("📋 최근 제작 이력")
    hist = _load_showcard_history()
    if hist:
        design_names = {1: "클래식", 2: "모던", 3: "미니멀"}
        hist_rows = [{"제품명": h.get("product_name",""), "사이즈": h.get("size_class",""),
                      "디자인": design_names.get(h.get("selected_design"),""), "워딩": h.get("wording_line1",""),
                      "제작일": pd.to_datetime(h.get("created_at","")).strftime("%Y-%m-%d %H:%M") if h.get("created_at") else ""} for h in hist]
        st.dataframe(pd.DataFrame(hist_rows), use_container_width=True, hide_index=True)
    else:
        st.info("아직 제작 이력이 없습니다.")


# ══════════════════════════════════════
#  페이지 라우팅
# ══════════════════════════════════════
if page == "📊 현황 분석":
    page_sku_analysis()
elif page == "📈 트렌드 분석":
    page_trend_analysis()
elif page == "💰 매출 분석":
    page_sales_analysis()
elif page == "💄 약국 화장품":
    page_cosmetics_dashboard()
elif page == "🏷️ 쇼카드 제작":
    page_showcard()
