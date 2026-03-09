"""
이수 마트약국 — 성과 리포트 대시보드
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import datetime as _dt

from supabase_client import (
    fetch_products, fetch_orders, flatten_order_items,
    fetch_sale_cost_records, is_supabase_configured,
)
from analysis import (
    build_daily_summary, get_available_months,
    daily_report, daily_category_breakdown, daily_hourly_pattern,
    daily_top_products, daily_product_anomalies,
    weekly_report, weekly_category_comparison, weekly_product_movers,
    monthly_report, monthly_category_movement,
    analyze_category_sales, analyze_category_gp,
    analyze_top_products, analyze_top_gp_products,
    analyze_hourly_pattern, analyze_weekday_pattern,
    analyze_outlier_days,
)
from insights import (
    generate_daily_insights, generate_weekly_insights, generate_monthly_insights,
)

# ──────────────────────────────────────
# 페이지 설정
# ──────────────────────────────────────
st.set_page_config(
    page_title="마트약국 성과 리포트",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ──────────────────────────────────────
# 디자인 시스템
# ──────────────────────────────────────

# 색상 팔레트 — 마트약국 BI(하늘색+베이지) 기반 프리미엄 웰니스 톤
C_TEAL = "#1A8A8D"          # 딥 틸 — 전문성 + 신뢰
C_TEAL_LIGHT = "#2CB5B8"    # 라이트 틸
C_TEAL_SOFT = "#E8F6F6"     # 틸 배경
C_AMBER = "#D4952A"         # 앰버 악센트 — 따뜻함 + 발견
C_AMBER_LIGHT = "#F5DEB3"   # 라이트 앰버
C_CORAL = "#D4654A"         # 코랄 — 경고/하락
C_SAGE = "#7BAE7F"          # 세이지 — 긍정/상승
C_SAND = "#FAF8F4"          # 샌드 배경
C_WARM_GRAY = "#B8AFA5"     # 따뜻한 그레이
C_CHARCOAL = "#2D2A26"      # 차콜 텍스트
C_CARD_BG = "#FFFFFF"       # 카드 배경

# Plotly 차트 색상
CHART_COLORS = [C_TEAL, C_AMBER, C_CORAL, C_SAGE, "#7B9EC9", "#C47ED0", "#E8A838", "#5BA08B"]
CHART_BG = "rgba(0,0,0,0)"
CHART_GRID = "rgba(45,42,38,0.06)"

# ──────────────────────────────────────
# 커스텀 CSS
# ──────────────────────────────────────

CUSTOM_CSS = """
<style>
@import url('https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;500;600;700&display=swap');
@import url('https://cdn.jsdelivr.net/gh/orioncactus/pretendard@v1.3.9/dist/web/variable/pretendardvariable-dynamic-subset.min.css');

:root {
    --teal: #1A8A8D;
    --teal-light: #2CB5B8;
    --teal-soft: #E8F6F6;
    --amber: #D4952A;
    --coral: #D4654A;
    --sage: #7BAE7F;
    --sand: #FAF8F4;
    --charcoal: #2D2A26;
    --warm-gray: #B8AFA5;
}

/* 전체 폰트 */
html, body, [class*="css"] {
    font-family: 'Pretendard Variable', 'Pretendard', -apple-system, sans-serif !important;
}

/* 사이드바 숨기기 */
[data-testid="stSidebar"] { display: none; }
[data-testid="collapsedControl"] { display: none; }

/* 메인 컨테이너 */
.block-container {
    padding-top: 1rem;
    padding-bottom: 2rem;
    max-width: 1400px;
}

/* 배경 그라디언트 오버레이 */
.stApp {
    background: linear-gradient(170deg, #FAF8F4 0%, #F5F0E8 40%, #EEF5F5 100%) !important;
}

/* 헤더 타이틀 */
.dashboard-header {
    background: linear-gradient(135deg, #1A8A8D 0%, #157578 50%, #1A8A8D 100%);
    border-radius: 16px;
    padding: 28px 36px;
    margin-bottom: 24px;
    position: relative;
    overflow: hidden;
}
.dashboard-header::before {
    content: '';
    position: absolute;
    top: -50%;
    right: -20%;
    width: 300px;
    height: 300px;
    background: radial-gradient(circle, rgba(255,255,255,0.08) 0%, transparent 70%);
    border-radius: 50%;
}
.dashboard-header::after {
    content: '';
    position: absolute;
    bottom: -30%;
    left: 10%;
    width: 200px;
    height: 200px;
    background: radial-gradient(circle, rgba(212,149,42,0.15) 0%, transparent 70%);
    border-radius: 50%;
}
.dashboard-header h1 {
    color: #FFFFFF;
    font-family: 'Outfit', sans-serif !important;
    font-size: 1.6rem;
    font-weight: 600;
    letter-spacing: -0.02em;
    margin: 0 0 4px 0;
    position: relative;
    z-index: 1;
}
.dashboard-header .subtitle {
    color: rgba(255,255,255,0.7);
    font-size: 0.85rem;
    font-weight: 300;
    position: relative;
    z-index: 1;
}

/* KPI 카드 */
.kpi-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
    gap: 14px;
    margin: 20px 0;
}
.kpi-card {
    background: #FFFFFF;
    border-radius: 14px;
    padding: 20px 22px;
    border: 1px solid rgba(45,42,38,0.06);
    box-shadow: 0 1px 3px rgba(45,42,38,0.04);
    transition: transform 0.2s ease, box-shadow 0.2s ease;
    position: relative;
    overflow: hidden;
}
.kpi-card:hover {
    transform: translateY(-2px);
    box-shadow: 0 6px 20px rgba(26,138,141,0.1);
}
.kpi-card::before {
    content: '';
    position: absolute;
    top: 0;
    left: 0;
    right: 0;
    height: 3px;
    background: linear-gradient(90deg, var(--teal), var(--teal-light));
    border-radius: 14px 14px 0 0;
}
.kpi-card.accent::before {
    background: linear-gradient(90deg, var(--amber), #E8A838);
}
.kpi-label {
    font-size: 0.75rem;
    color: var(--warm-gray);
    font-weight: 500;
    text-transform: uppercase;
    letter-spacing: 0.08em;
    margin-bottom: 8px;
}
.kpi-value {
    font-family: 'Outfit', sans-serif !important;
    font-size: 1.65rem;
    font-weight: 700;
    color: var(--charcoal);
    line-height: 1.1;
    margin-bottom: 6px;
}
.kpi-delta {
    font-size: 0.78rem;
    font-weight: 500;
    display: inline-flex;
    align-items: center;
    gap: 3px;
    padding: 2px 8px;
    border-radius: 6px;
}
.kpi-delta.up {
    color: var(--sage);
    background: rgba(123,174,127,0.1);
}
.kpi-delta.down {
    color: var(--coral);
    background: rgba(212,101,74,0.1);
}
.kpi-delta.neutral {
    color: var(--warm-gray);
    background: rgba(184,175,165,0.1);
}

/* 인사이트 카드 */
.insight-container {
    background: #FFFFFF;
    border-radius: 14px;
    padding: 20px 24px;
    margin: 16px 0;
    border: 1px solid rgba(45,42,38,0.06);
    box-shadow: 0 1px 3px rgba(45,42,38,0.04);
}
.insight-title {
    font-family: 'Outfit', sans-serif !important;
    font-size: 0.95rem;
    font-weight: 600;
    color: var(--charcoal);
    margin-bottom: 14px;
    display: flex;
    align-items: center;
    gap: 8px;
}
.insight-title .count {
    background: var(--teal-soft);
    color: var(--teal);
    font-size: 0.72rem;
    padding: 2px 8px;
    border-radius: 10px;
    font-weight: 600;
}
.insight-item {
    display: flex;
    align-items: flex-start;
    gap: 10px;
    padding: 10px 0;
    border-bottom: 1px solid rgba(45,42,38,0.04);
}
.insight-item:last-child { border-bottom: none; }
.insight-badge {
    width: 8px;
    height: 8px;
    border-radius: 50%;
    margin-top: 6px;
    flex-shrink: 0;
}
.insight-badge.alert { background: var(--coral); }
.insight-badge.positive { background: var(--sage); }
.insight-badge.action { background: var(--teal); }
.insight-badge.neutral { background: var(--warm-gray); }
.insight-text {
    font-size: 0.85rem;
    color: var(--charcoal);
    line-height: 1.5;
}
.insight-text strong {
    font-weight: 600;
}

/* 섹션 헤더 */
.section-header {
    font-family: 'Outfit', sans-serif !important;
    font-size: 1.1rem;
    font-weight: 600;
    color: var(--charcoal);
    margin: 28px 0 16px 0;
    padding-bottom: 8px;
    border-bottom: 2px solid var(--teal-soft);
    display: flex;
    align-items: center;
    gap: 8px;
}
.section-header .icon {
    width: 28px;
    height: 28px;
    background: var(--teal-soft);
    border-radius: 8px;
    display: flex;
    align-items: center;
    justify-content: center;
    font-size: 0.85rem;
}

/* Streamlit 기본 컴포넌트 오버라이드 */
.stTabs [data-baseweb="tab-list"] {
    gap: 0;
    background: #FFFFFF;
    border-radius: 12px;
    padding: 4px;
    border: 1px solid rgba(45,42,38,0.08);
    box-shadow: 0 1px 3px rgba(45,42,38,0.04);
}
.stTabs [data-baseweb="tab"] {
    border-radius: 8px;
    padding: 10px 24px;
    font-family: 'Outfit', sans-serif !important;
    font-weight: 500;
    font-size: 0.88rem;
    color: var(--warm-gray);
    border: none;
    background: transparent;
}
.stTabs [aria-selected="true"] {
    background: var(--teal) !important;
    color: #FFFFFF !important;
    border: none !important;
}
.stTabs [data-baseweb="tab-highlight"] { display: none; }
.stTabs [data-baseweb="tab-border"] { display: none; }

/* 데이터프레임 스타일 */
.stDataFrame {
    border-radius: 12px;
    overflow: hidden;
}
[data-testid="stDataFrame"] > div {
    border-radius: 12px;
    border: 1px solid rgba(45,42,38,0.06) !important;
}

/* 셀렉트박스 */
.stSelectbox > div > div {
    border-radius: 10px !important;
    border-color: rgba(45,42,38,0.12) !important;
    font-family: 'Pretendard Variable', sans-serif !important;
}

/* st.metric 숨기기 (커스텀 KPI 사용) */
[data-testid="stMetric"] {
    background: #FFFFFF;
    border-radius: 12px;
    padding: 14px 18px;
    border: 1px solid rgba(45,42,38,0.06);
    box-shadow: 0 1px 3px rgba(45,42,38,0.04);
}
[data-testid="stMetricLabel"] {
    font-size: 0.75rem !important;
    color: var(--warm-gray) !important;
    font-weight: 500 !important;
    text-transform: uppercase;
    letter-spacing: 0.06em;
}
[data-testid="stMetricValue"] {
    font-family: 'Outfit', sans-serif !important;
    font-size: 1.5rem !important;
    font-weight: 700 !important;
    color: var(--charcoal) !important;
}
[data-testid="stMetricDelta"] > div {
    font-size: 0.78rem !important;
    font-weight: 500 !important;
}

/* divider */
hr {
    border: none;
    height: 1px;
    background: linear-gradient(90deg, transparent, rgba(26,138,141,0.15), transparent);
    margin: 24px 0;
}

/* expander */
.streamlit-expanderHeader {
    font-family: 'Outfit', sans-serif !important;
    font-weight: 600;
    font-size: 0.92rem;
    background: #FFFFFF;
    border-radius: 12px;
}

/* caption */
.stCaption {
    color: var(--warm-gray) !important;
    font-size: 0.78rem !important;
}

/* 페이드인 애니메이션 */
@keyframes fadeInUp {
    from { opacity: 0; transform: translateY(12px); }
    to { opacity: 1; transform: translateY(0); }
}
.dashboard-header { animation: fadeInUp 0.5s ease-out; }
.kpi-card { animation: fadeInUp 0.5s ease-out; }
.kpi-card:nth-child(2) { animation-delay: 0.05s; }
.kpi-card:nth-child(3) { animation-delay: 0.1s; }
.kpi-card:nth-child(4) { animation-delay: 0.15s; }
.kpi-card:nth-child(5) { animation-delay: 0.2s; }
.insight-container { animation: fadeInUp 0.5s ease-out 0.25s both; }

</style>
"""

st.markdown(CUSTOM_CSS, unsafe_allow_html=True)


# ──────────────────────────────────────
# Plotly 테마 레이아웃
# ──────────────────────────────────────

def _chart_layout(**overrides):
    """통일된 Plotly 레이아웃"""
    base = dict(
        plot_bgcolor=CHART_BG,
        paper_bgcolor=CHART_BG,
        font=dict(family="Pretendard Variable, Outfit, sans-serif", color=C_CHARCOAL, size=12),
        margin=dict(l=20, r=20, t=40, b=20),
        height=350,
        xaxis=dict(gridcolor=CHART_GRID, zeroline=False),
        yaxis=dict(gridcolor=CHART_GRID, zeroline=False),
        legend=dict(
            orientation="h", y=1.12, x=0,
            font=dict(size=11),
            bgcolor="rgba(0,0,0,0)",
        ),
        hoverlabel=dict(
            bgcolor="#FFFFFF",
            bordercolor=C_TEAL,
            font=dict(family="Pretendard Variable, sans-serif", size=12, color=C_CHARCOAL),
        ),
    )
    base.update(overrides)
    return base


# ──────────────────────────────────────
# 헬퍼
# ──────────────────────────────────────

def _fmt(v, unit="원"):
    if pd.isna(v) or v == 0:
        return f"0{unit}"
    if abs(v) >= 1e8:
        return f"{v/1e8:.1f}억{unit}"
    if abs(v) >= 1e4:
        return f"{v/1e4:.0f}만{unit}"
    return f"{v:,.0f}{unit}"


def _delta_str(current, previous):
    if previous == 0:
        return None
    diff = current - previous
    pct = diff / previous * 100
    return f"{pct:+.1f}% ({_fmt(diff)})"


def _delta_class(current, previous):
    if previous == 0:
        return "neutral"
    return "up" if current >= previous else "down"


def _delta_arrow(current, previous):
    if previous == 0:
        return ""
    return "▲" if current >= previous else "▼"


def _render_kpi_card(label, value, delta_str=None, unit="원"):
    st.metric(label=label, value=_fmt(value, unit), delta=delta_str)


def _render_kpi_cards_html(cards):
    """커스텀 KPI 카드 그리드 (HTML)"""
    html = '<div class="kpi-grid">'
    for i, card in enumerate(cards):
        accent_cls = " accent" if i == len(cards) - 1 else ""
        delta_html = ""
        if card.get("delta_str"):
            cls = card.get("delta_class", "neutral")
            arrow = card.get("delta_arrow", "")
            delta_html = f'<div class="kpi-delta {cls}">{arrow} {card["delta_str"]}</div>'
        html += f'''
        <div class="kpi-card{accent_cls}">
            <div class="kpi-label">{card["label"]}</div>
            <div class="kpi-value">{card["value"]}</div>
            {delta_html}
        </div>'''
    html += '</div>'
    st.markdown(html, unsafe_allow_html=True)


def _render_insights(insights, title="핵심 사항"):
    if not insights:
        return
    html = f'''<div class="insight-container">
        <div class="insight-title">{title} <span class="count">{len(insights)}건</span></div>'''
    for ins in insights:
        badge = {"alert": "alert", "positive": "positive", "action": "action"}.get(ins["type"], "neutral")
        html += f'''
        <div class="insight-item">
            <div class="insight-badge {badge}"></div>
            <div class="insight-text">{ins["icon"]} <strong>{ins["title"]}</strong> — {ins["detail"]}</div>
        </div>'''
    html += '</div>'
    st.markdown(html, unsafe_allow_html=True)


def _section_header(icon, title):
    st.markdown(
        f'<div class="section-header"><span class="icon">{icon}</span>{title}</div>',
        unsafe_allow_html=True,
    )


def _plotly_bar(df, x, y, title, color=None, horizontal=False):
    colors = color or C_TEAL
    fig = go.Figure()
    if horizontal:
        fig.add_trace(go.Bar(
            y=df[x], x=df[y], orientation="h",
            marker=dict(color=colors, cornerradius=4),
        ))
    else:
        fig.add_trace(go.Bar(
            x=df[x], y=df[y],
            marker=dict(color=colors, cornerradius=4),
        ))
    fig.update_layout(**_chart_layout(title=dict(text=title, font=dict(size=14, family="Outfit, sans-serif"))))
    return fig


# ──────────────────────────────────────
# 데이터 로드
# ──────────────────────────────────────

@st.cache_data(ttl=3600)
def load_all_data():
    products = fetch_products()
    orders = fetch_orders("2025-12-08", "2026-12-31")
    order_items = flatten_order_items(orders, products)
    cost_records = fetch_sale_cost_records("2025-12-08", "2026-12-31")

    if not cost_records.empty and not products.empty:
        cost_records = cost_records.merge(
            products[["id", "name", "erp_category", "erp_subcategory", "selling_price"]],
            left_on="product_id", right_on="id",
            how="left", suffixes=("", "_product"),
        )

    return {
        "products": products,
        "orders": orders,
        "order_items": order_items,
        "cost_records": cost_records,
    }


# ──────────────────────────────────────
# 메인
# ──────────────────────────────────────

def main():
    if not is_supabase_configured():
        st.error("Supabase 연결 설정이 필요합니다.")
        st.code("""
# .streamlit/secrets.toml
[supabase]
url = "https://your-project.supabase.co"
service_role_key = "your-service-role-key"
store_id = "your-store-id"
        """)
        return

    # 헤더
    today = _dt.date.today()
    st.markdown(f"""
    <div class="dashboard-header">
        <h1>이수 마트약국 — 성과 리포트</h1>
        <div class="subtitle">{today.strftime("%Y년 %m월 %d일")} 기준 · 실시간 데이터 분석</div>
    </div>
    """, unsafe_allow_html=True)

    with st.spinner("데이터 로딩 중..."):
        data = load_all_data()

    summary = build_daily_summary(data)
    if summary.empty:
        st.warning("매출 데이터가 없습니다.")
        return

    available_months = get_available_months(data)
    if not available_months:
        st.warning("사용 가능한 월이 없습니다.")
        return

    # 기준월 선택
    current_ym = today.strftime("%Y-%m")
    if len(available_months) >= 2 and available_months[0] == current_ym and today.day < 15:
        default_idx = 1
    else:
        default_idx = 0

    col_sel, _ = st.columns([1, 3])
    with col_sel:
        selected_ym = st.selectbox("기준월", available_months, index=default_idx)

    # 월간 KPI 카드 (상단)
    mr = monthly_report(data, summary, selected_ym)
    tm = mr["this_month"]
    pm = mr["prev_month"]

    _render_kpi_cards_html([
        {
            "label": "총매출",
            "value": _fmt(tm["매출"]),
            "delta_str": _delta_str(tm["매출"], pm["매출"]),
            "delta_class": _delta_class(tm["매출"], pm["매출"]),
            "delta_arrow": _delta_arrow(tm["매출"], pm["매출"]),
        },
        {
            "label": "주문수",
            "value": _fmt(tm["주문수"], "건"),
            "delta_str": _delta_str(tm["주문수"], pm["주문수"]),
            "delta_class": _delta_class(tm["주문수"], pm["주문수"]),
            "delta_arrow": _delta_arrow(tm["주문수"], pm["주문수"]),
        },
        {
            "label": "객단가",
            "value": _fmt(tm["객단가"]),
            "delta_str": _delta_str(tm["객단가"], pm["객단가"]),
            "delta_class": _delta_class(tm["객단가"], pm["객단가"]),
            "delta_arrow": _delta_arrow(tm["객단가"], pm["객단가"]),
        },
        {
            "label": "매출총이익",
            "value": _fmt(tm["GP"]),
            "delta_str": _delta_str(tm["GP"], pm["GP"]),
            "delta_class": _delta_class(tm["GP"], pm["GP"]),
            "delta_arrow": _delta_arrow(tm["GP"], pm["GP"]),
        },
        {
            "label": "GP율",
            "value": f"{tm['GP율']:.1f}%",
            "delta_str": f"{tm['GP율'] - pm['GP율']:+.1f}%p" if pm["GP율"] > 0 else None,
            "delta_class": "up" if tm["GP율"] >= pm["GP율"] else "down",
            "delta_arrow": "▲" if tm["GP율"] >= pm["GP율"] else "▼",
        },
    ])

    # 핵심 인사이트
    monthly_insights = generate_monthly_insights(data, summary, selected_ym)
    _render_insights(monthly_insights, "이번 달 핵심 사항")

    st.divider()

    # 탭
    tab_daily, tab_weekly, tab_monthly = st.tabs(["일일 리포트", "주간 리포트", "월간 리포트"])

    with tab_daily:
        _render_daily_tab(data, summary, selected_ym)
    with tab_weekly:
        _render_weekly_tab(data, summary, selected_ym)
    with tab_monthly:
        _render_monthly_tab(data, summary, selected_ym)


# ──────────────────────────────────────
# 일일 리포트 탭
# ──────────────────────────────────────

def _render_daily_tab(data, summary, ym):
    month_dates = summary[summary["date"].dt.strftime("%Y-%m") == ym]["date"].sort_values(ascending=False)
    if month_dates.empty:
        st.info("해당 월의 데이터가 없습니다.")
        return

    date_options = month_dates.dt.strftime("%Y-%m-%d").tolist()
    selected_date_str = st.selectbox("날짜 선택", date_options, key="daily_date")
    selected_date = pd.Timestamp(selected_date_str)

    dr = daily_report(data, summary, selected_date)
    t, y, lw = dr["today"], dr["yesterday"], dr["lastweek"]

    _section_header("📅", "일일 성과")

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        _render_kpi_card("매출", t["매출"], _delta_str(t["매출"], y["매출"]))
        st.caption(f"전주동요일 대비: {_delta_str(t['매출'], lw['매출']) or '-'}")
    with c2:
        _render_kpi_card("주문수", t["주문수"], _delta_str(t["주문수"], y["주문수"]), unit="건")
    with c3:
        _render_kpi_card("객단가", t["객단가"], _delta_str(t["객단가"], y["객단가"]))
    with c4:
        _render_kpi_card("매출총이익", t["GP"], _delta_str(t["GP"], y["GP"]))

    daily_ins = generate_daily_insights(data, summary, selected_date)
    _render_insights(daily_ins, "오늘의 주목 사항")

    # 카테고리별 매출
    cat = daily_category_breakdown(data, selected_date)
    if not cat.empty:
        _section_header("🏷️", "카테고리별 매출")
        col_chart, col_table = st.columns(2)
        with col_chart:
            fig = _plotly_bar(cat, "erp_category", "매출", "카테고리 매출", horizontal=True)
            st.plotly_chart(fig, use_container_width=True)
        with col_table:
            display_cols = ["erp_category", "매출", "매출비중"]
            if "GP율" in cat.columns:
                display_cols.append("GP율")
            st.dataframe(
                cat[display_cols].rename(columns={"erp_category": "카테고리"}),
                use_container_width=True, hide_index=True,
            )

    # 시간대별 판매
    hourly = daily_hourly_pattern(data, selected_date)
    if not hourly.empty:
        _section_header("⏰", "시간대별 판매")
        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=hourly["hour"], y=hourly["매출"], name="매출",
            marker=dict(color=C_TEAL, cornerradius=4),
        ))
        fig.add_trace(go.Scatter(
            x=hourly["hour"], y=hourly["건수"], name="건수",
            yaxis="y2", mode="lines+markers",
            line=dict(color=C_AMBER, width=2.5),
            marker=dict(color=C_AMBER, size=6),
        ))
        fig.update_layout(**_chart_layout(
            height=300,
            yaxis=dict(title="매출(원)", gridcolor=CHART_GRID),
            yaxis2=dict(title="건수", overlaying="y", side="right", gridcolor=CHART_GRID),
        ))
        st.plotly_chart(fig, use_container_width=True)

    # TOP/주목 상품
    top_prods = daily_top_products(data, selected_date)
    anomalies = daily_product_anomalies(data, selected_date)

    if not top_prods.empty or not anomalies.empty:
        _section_header("🔍", "상품 분석")
        col_top, col_anom = st.columns(2)
        with col_top:
            st.markdown(f"**매출 TOP 10**")
            if not top_prods.empty:
                st.dataframe(
                    top_prods.rename(columns={"product_name": "상품명", "erp_category": "카테고리"}),
                    use_container_width=True, hide_index=True,
                )
        with col_anom:
            st.markdown(f"**평소 대비 급변 상품**")
            if not anomalies.empty:
                display_df = anomalies[["product_name", "당일매출", "평균매출", "변화율"]].copy()
                display_df.columns = ["상품명", "당일매출", "14일평균", "변화율(%)"]
                st.dataframe(display_df, use_container_width=True, hide_index=True)
            else:
                st.info("급변 상품 없음")

    # 최근 7일 추이
    recent_7 = summary[
        (summary["date"] <= selected_date) &
        (summary["date"] > selected_date - pd.Timedelta(days=7))
    ].sort_values("date")

    if not recent_7.empty:
        _section_header("📈", "최근 7일 추이")
        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=recent_7["date"].dt.strftime("%m/%d"),
            y=recent_7["매출"], name="매출",
            marker=dict(color=C_TEAL, cornerradius=4),
        ))
        fig.add_trace(go.Scatter(
            x=recent_7["date"].dt.strftime("%m/%d"),
            y=recent_7["객단가"], name="객단가",
            yaxis="y2", mode="lines+markers",
            line=dict(color=C_AMBER, width=2.5),
            marker=dict(color=C_AMBER, size=6),
        ))
        fig.update_layout(**_chart_layout(
            height=300,
            yaxis=dict(title="매출(원)", gridcolor=CHART_GRID),
            yaxis2=dict(title="객단가(원)", overlaying="y", side="right", gridcolor=CHART_GRID),
        ))
        st.plotly_chart(fig, use_container_width=True)


# ──────────────────────────────────────
# 주간 리포트 탭
# ──────────────────────────────────────

def _render_weekly_tab(data, summary, ym):
    month_dates = summary[summary["date"].dt.strftime("%Y-%m") == ym]["date"].sort_values(ascending=False)
    if month_dates.empty:
        st.info("해당 월의 데이터가 없습니다.")
        return

    latest = month_dates.iloc[0]
    weeks = []
    d = latest
    while d.strftime("%Y-%m") == ym or len(weeks) == 0:
        weekday = d.dayofweek
        mon = d - pd.Timedelta(days=weekday)
        sun = mon + pd.Timedelta(days=6)
        label = f"{mon.strftime('%m/%d')}~{sun.strftime('%m/%d')}"
        if label not in [w[0] for w in weeks]:
            weeks.append((label, d))
        d = d - pd.Timedelta(days=7)
        if len(weeks) >= 6:
            break

    week_labels = [w[0] for w in weeks]
    selected_week_label = st.selectbox("주간 선택", week_labels, key="weekly_sel")
    selected_week_date = dict(weeks)[selected_week_label]

    wr = weekly_report(data, summary, selected_week_date)
    tw, lw = wr["this_week"], wr["last_week"]

    _section_header("📆", "주간 성과")
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        _render_kpi_card("매출", tw["매출"], _delta_str(tw["매출"], lw["매출"]))
    with c2:
        _render_kpi_card("주문수", tw["주문수"], _delta_str(tw["주문수"], lw["주문수"]), unit="건")
    with c3:
        _render_kpi_card("객단가", tw["객단가"], _delta_str(tw["객단가"], lw["객단가"]))
    with c4:
        _render_kpi_card("매출총이익", tw["GP"], _delta_str(tw["GP"], lw["GP"]))

    weekly_ins = generate_weekly_insights(data, summary, selected_week_date)
    _render_insights(weekly_ins, "주간 주목 사항")

    # 요일별 비교
    tw_daily = wr["this_week_daily"]
    lw_daily = wr["last_week_daily"]

    if not tw_daily.empty:
        _section_header("📊", "요일별 비교")
        col_chart, col_table = st.columns(2)
        with col_chart:
            fig = go.Figure()
            fig.add_trace(go.Bar(
                x=tw_daily["요일"], y=tw_daily["매출"],
                name="이번주", marker=dict(color=C_TEAL, cornerradius=4),
            ))
            if not lw_daily.empty:
                fig.add_trace(go.Bar(
                    x=lw_daily["요일"], y=lw_daily["매출"],
                    name="전주", marker=dict(color=C_WARM_GRAY, cornerradius=4),
                ))
            fig.update_layout(**_chart_layout(barmode="group", height=350))
            st.plotly_chart(fig, use_container_width=True)
        with col_table:
            display = tw_daily[["date", "요일", "매출", "주문수", "객단가"]].copy()
            display["날짜"] = display["date"].dt.strftime("%m/%d")
            st.dataframe(
                display[["날짜", "요일", "매출", "주문수", "객단가"]],
                use_container_width=True, hide_index=True,
            )

    # 카테고리 주간 변동
    cat_comp = weekly_category_comparison(data, selected_week_date)
    if not cat_comp.empty:
        _section_header("🏷️", "카테고리 주간 변동")
        display = cat_comp.rename(columns={"erp_category": "카테고리"})
        display["매출_이번주"] = display["매출_이번주"].apply(lambda x: f"{x:,.0f}")
        display["매출_전주"] = display["매출_전주"].apply(lambda x: f"{x:,.0f}")
        st.dataframe(display, use_container_width=True, hide_index=True)

    # 상품 변동
    movers = weekly_product_movers(data, selected_week_date)
    top = movers.get("top", pd.DataFrame())
    rising = movers.get("rising", pd.DataFrame())

    if not top.empty or not rising.empty:
        _section_header("🚀", "상품 변동")
        col_top, col_rise = st.columns(2)
        with col_top:
            st.markdown("**이번주 TOP 10**")
            if not top.empty:
                st.dataframe(
                    top.rename(columns={"product_name": "상품명", "erp_category": "카테고리"}),
                    use_container_width=True, hide_index=True,
                )
        with col_rise:
            st.markdown("**전주 대비 급상승**")
            if not rising.empty:
                display = rising[["product_name", "erp_category", "매출_이번주", "증감률"]].copy()
                display.columns = ["상품명", "카테고리", "이번주매출", "증감률(%)"]
                st.dataframe(display, use_container_width=True, hide_index=True)
            else:
                st.info("전주 데이터 부족")


# ──────────────────────────────────────
# 월간 리포트 탭
# ──────────────────────────────────────

def _render_monthly_tab(data, summary, ym):
    mr = monthly_report(data, summary, ym)
    tm = mr["this_month"]
    pm = mr["prev_month"]

    _section_header("📊", f"{ym} 월간 성과")
    c1, c2, c3, c4, c5 = st.columns(5)
    with c1:
        _render_kpi_card("총매출", tm["매출"], _delta_str(tm["매출"], pm["매출"]))
    with c2:
        _render_kpi_card("주문수", tm["주문수"], _delta_str(tm["주문수"], pm["주문수"]), unit="건")
    with c3:
        _render_kpi_card("객단가", tm["객단가"], _delta_str(tm["객단가"], pm["객단가"]))
    with c4:
        _render_kpi_card("매출총이익", tm["GP"], _delta_str(tm["GP"], pm["GP"]))
    with c5:
        gp_delta = f"{tm['GP율'] - pm['GP율']:+.1f}%p" if pm["GP율"] > 0 else None
        st.metric("GP율", f"{tm['GP율']:.1f}%", delta=gp_delta)

    monthly_ins = generate_monthly_insights(data, summary, ym)
    _render_insights(monthly_ins, "월간 핵심 사항")

    # 일별 매출 추이
    month_daily = mr.get("this_month_daily", pd.DataFrame())
    if not month_daily.empty:
        _section_header("📈", "일별 매출 추이")
        avg_rev = month_daily["매출"].mean()
        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=month_daily["date"].dt.strftime("%m/%d"),
            y=month_daily["매출"], name="매출",
            marker=dict(color=C_TEAL, cornerradius=4),
        ))
        fig.add_hline(
            y=avg_rev, line_dash="dash", line_color=C_AMBER, line_width=2,
            annotation_text=f"일평균 {_fmt(avg_rev)}",
            annotation_font=dict(color=C_AMBER, size=11, family="Outfit, sans-serif"),
        )
        fig.update_layout(**_chart_layout())
        st.plotly_chart(fig, use_container_width=True)

    # 카테고리 분석
    cat_sales = analyze_category_sales(data, ym)
    cat_gp = analyze_category_gp(data, ym)

    if not cat_sales.empty:
        _section_header("🏷️", "카테고리 분석")
        col_chart, col_table = st.columns(2)
        with col_chart:
            fig = _plotly_bar(cat_sales, "erp_category", "총매출", "카테고리 매출", horizontal=True)
            st.plotly_chart(fig, use_container_width=True)
        with col_table:
            if not cat_gp.empty:
                st.markdown("**카테고리 GP 분석**")
                display = cat_gp[["erp_category", "총매출", "총이익", "GP율", "이익비중"]].copy()
                display.columns = ["카테고리", "매출", "이익", "GP율(%)", "이익비중(%)"]
                st.dataframe(display, use_container_width=True, hide_index=True)

    # 카테고리 월간 변동
    cat_mv = monthly_category_movement(data, ym)
    if not cat_mv.empty:
        _section_header("↕️", "카테고리 전월 대비 변동")
        display_cols = ["erp_category", "매출_이번달", "매출_전월", "매출증감률", "비중_이번달", "비중변화"]
        if "GP율" in cat_mv.columns:
            display_cols.append("GP율")
        display = cat_mv[display_cols].copy()
        display.columns = ["카테고리", "이번달매출", "전월매출", "증감률(%)", "비중(%)", "비중변화(%p)"] + (["GP율(%)"] if "GP율" in cat_mv.columns else [])
        st.dataframe(display, use_container_width=True, hide_index=True)

    # 상품 랭킹
    top_prods = analyze_top_products(data, ym)
    top_gp = analyze_top_gp_products(data, ym)

    if not top_prods.empty or not top_gp.empty:
        _section_header("🏆", "상품 랭킹")
        col_rev, col_gp = st.columns(2)
        with col_rev:
            st.markdown("**매출 TOP 15**")
            if not top_prods.empty:
                display = top_prods[["product_name", "erp_category", "총매출", "판매수량"]].copy()
                display.columns = ["상품명", "카테고리", "매출", "수량"]
                st.dataframe(display, use_container_width=True, hide_index=True)
        with col_gp:
            st.markdown("**GP TOP 15**")
            if not top_gp.empty:
                display = top_gp[["name", "erp_category", "총이익", "GP율"]].copy()
                display.columns = ["상품명", "카테고리", "이익", "GP율(%)"]
                st.dataframe(display, use_container_width=True, hide_index=True)

    # 이상 징후
    outlier = analyze_outlier_days(data, ym)
    if outlier and "outliers" in outlier:
        outlier_df = outlier["outliers"]
        if not outlier_df.empty:
            _section_header("⚠️", "이상 징후 감지")
            st.caption(f"일평균 매출 {_fmt(outlier['mean_revenue'])} 기준, z-score >=1.0인 날")
            display = outlier_df[["날짜", "총매출", "주문수", "매출_z", "유형"]].copy()
            display.columns = ["날짜", "매출", "주문수", "z-score", "유형"]
            st.dataframe(display, use_container_width=True, hide_index=True)

    # 시간대/요일 패턴
    hourly = analyze_hourly_pattern(data, ym)
    weekday = analyze_weekday_pattern(data, ym)

    if not hourly.empty or not weekday.empty:
        _section_header("🕐", "판매 패턴")
        col_weekday, col_hourly = st.columns(2)
        with col_weekday:
            if not weekday.empty:
                fig = go.Figure()
                fig.add_trace(go.Bar(
                    x=weekday["weekday_name"], y=weekday["총매출"],
                    marker=dict(color=C_TEAL, cornerradius=4),
                ))
                fig.update_layout(**_chart_layout(
                    title=dict(text="요일별 매출", font=dict(size=14, family="Outfit, sans-serif")),
                    height=300,
                ))
                st.plotly_chart(fig, use_container_width=True)
        with col_hourly:
            if not hourly.empty:
                fig = go.Figure()
                fig.add_trace(go.Bar(
                    x=hourly["hour"], y=hourly["총매출"],
                    marker=dict(color=C_AMBER, cornerradius=4),
                ))
                fig.update_layout(**_chart_layout(
                    title=dict(text="시간대별 매출", font=dict(size=14, family="Outfit, sans-serif")),
                    height=300,
                ))
                st.plotly_chart(fig, use_container_width=True)


if __name__ == "__main__":
    main()
