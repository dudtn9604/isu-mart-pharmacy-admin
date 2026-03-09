"""
Action Item 자동 생성 엔진
- 일/주/월 단위 인사이트 자동 도출
- 각 인사이트: type, icon, title, detail, priority
"""

import pandas as pd
import numpy as np
from typing import Dict, List
from datetime import timedelta

from analysis import (
    filter_items,
    build_daily_summary,
    daily_report,
    daily_category_breakdown,
    daily_top_products,
    weekly_report,
    weekly_category_comparison,
    weekly_product_movers,
    monthly_report,
    monthly_category_movement,
)


def _insight(type_: str, icon: str, title: str, detail: str, priority: int = 3) -> Dict:
    return {"type": type_, "icon": icon, "title": title, "detail": detail, "priority": priority}


# ──────────────────────────────────────
# 일일 인사이트
# ──────────────────────────────────────

def generate_daily_insights(data: Dict, summary: pd.DataFrame, date: pd.Timestamp) -> List[Dict]:
    insights = []
    date = pd.Timestamp(date).normalize()

    # 7일 평균 대비 매출
    recent_7 = summary[(summary["date"] < date) & (summary["date"] >= date - timedelta(days=7))]
    today_row = summary[summary["date"] == date]

    if not recent_7.empty and not today_row.empty:
        avg_7 = recent_7["매출"].mean()
        today_rev = today_row.iloc[0]["매출"]
        if avg_7 > 0:
            pct = (today_rev - avg_7) / avg_7 * 100
            if pct >= 20:
                insights.append(_insight(
                    "positive", "📈", "매출 호조",
                    f"7일 평균 대비 +{pct:.0f}% (오늘 {today_rev:,.0f}원 vs 평균 {avg_7:,.0f}원)",
                    priority=1,
                ))
            elif pct <= -20:
                insights.append(_insight(
                    "alert", "📉", "매출 부진",
                    f"7일 평균 대비 {pct:.0f}% (오늘 {today_rev:,.0f}원 vs 평균 {avg_7:,.0f}원)",
                    priority=1,
                ))

    # GP율 하락
    if not today_row.empty:
        today_gp_rate = today_row.iloc[0].get("GP율", 0)
        if not recent_7.empty:
            avg_gp_rate = recent_7["GP율"].mean()
            if avg_gp_rate > 0 and (avg_gp_rate - today_gp_rate) >= 3:
                insights.append(_insight(
                    "alert", "⚠️", "마진율 하락",
                    f"GP율 {today_gp_rate:.1f}% (7일 평균 {avg_gp_rate:.1f}% 대비 {avg_gp_rate - today_gp_rate:.1f}%p 하락)",
                    priority=2,
                ))

    # 카테고리 급변
    cat = daily_category_breakdown(data, date)
    prev_cat = daily_category_breakdown(data, date - timedelta(days=1))
    if not cat.empty and not prev_cat.empty:
        merged = cat[["erp_category", "매출"]].merge(
            prev_cat[["erp_category", "매출"]], on="erp_category",
            how="outer", suffixes=("_오늘", "_전일"),
        ).fillna(0)
        for _, row in merged.iterrows():
            if row["매출_전일"] > 0:
                ratio = row["매출_오늘"] / row["매출_전일"]
                if ratio >= 2:
                    insights.append(_insight(
                        "positive", "🔥", f"{row['erp_category']} 급증",
                        f"전일 대비 {ratio:.1f}배 증가",
                        priority=2,
                    ))
                elif ratio <= 0.5 and row["매출_전일"] >= 50000:
                    insights.append(_insight(
                        "alert", "⬇️", f"{row['erp_category']} 급감",
                        f"전일 대비 {(1-ratio)*100:.0f}% 감소",
                        priority=2,
                    ))

    # TOP 5 서프라이즈 상품
    top_today = daily_top_products(data, date, top_n=5)
    top_yesterday = daily_top_products(data, date - timedelta(days=1), top_n=10)
    if not top_today.empty and not top_yesterday.empty:
        yesterday_names = set(top_yesterday["product_name"].tolist())
        for _, row in top_today.iterrows():
            if row["product_name"] not in yesterday_names:
                insights.append(_insight(
                    "action", "🆕", f"서프라이즈: {row['product_name']}",
                    f"TOP 5 신규 진입 — 매출 {row['매출']:,.0f}원",
                    priority=3,
                ))
                break  # 1개만

    insights.sort(key=lambda x: x["priority"])
    return insights


# ──────────────────────────────────────
# 주간 인사이트
# ──────────────────────────────────────

def generate_weekly_insights(data: Dict, summary: pd.DataFrame, date: pd.Timestamp) -> List[Dict]:
    insights = []
    wr = weekly_report(data, summary, date)
    tw = wr["this_week"]
    lw = wr["last_week"]

    # 전주 대비 매출
    if lw["매출"] > 0:
        pct = (tw["매출"] - lw["매출"]) / lw["매출"] * 100
        diff = tw["매출"] - lw["매출"]
        if pct >= 10:
            insights.append(_insight(
                "positive", "📈", f"주간 매출 성장 +{pct:.1f}%",
                f"이번주 {tw['매출']:,.0f}원 (전주 대비 +{diff:,.0f}원)",
                priority=1,
            ))
        elif pct <= -10:
            insights.append(_insight(
                "alert", "📉", f"주간 매출 하락 {pct:.1f}%",
                f"이번주 {tw['매출']:,.0f}원 (전주 대비 {diff:,.0f}원)",
                priority=1,
            ))

    # 카테고리 급변
    cat_comp = weekly_category_comparison(data, date)
    if not cat_comp.empty:
        big_movers = cat_comp[cat_comp["매출증감률"].abs() >= 30]
        if not big_movers.empty:
            names = big_movers.apply(
                lambda r: f"{r['erp_category']}({r['매출증감률']:+.0f}%)", axis=1
            ).tolist()
            insights.append(_insight(
                "action", "🔀", "카테고리 급변",
                ", ".join(names[:5]),
                priority=2,
            ))

    # 4주 연속 객단가 추세
    date_norm = pd.Timestamp(date).normalize()
    weekly_aovs = []
    for i in range(4):
        w_date = date_norm - timedelta(weeks=i)
        wr_i = weekly_report(data, summary, w_date)
        weekly_aovs.append(wr_i["this_week"]["객단가"])
    weekly_aovs.reverse()
    if all(weekly_aovs[i] > 0 and weekly_aovs[i] > weekly_aovs[i-1] for i in range(1, 4)):
        insights.append(_insight(
            "positive", "📊", "4주 연속 객단가 상승",
            f"{weekly_aovs[0]:,.0f}원 → {weekly_aovs[3]:,.0f}원",
            priority=2,
        ))
    elif all(weekly_aovs[i] > 0 and weekly_aovs[i] < weekly_aovs[i-1] for i in range(1, 4)):
        insights.append(_insight(
            "alert", "📊", "4주 연속 객단가 하락",
            f"{weekly_aovs[0]:,.0f}원 → {weekly_aovs[3]:,.0f}원",
            priority=2,
        ))

    # 급상승 상품 TOP 3
    movers = weekly_product_movers(data, date)
    rising = movers.get("rising", pd.DataFrame())
    if not rising.empty:
        top3 = rising.head(3)
        names = top3.apply(
            lambda r: f"{r['product_name']}(+{r['증감률']:.0f}%)", axis=1
        ).tolist()
        insights.append(_insight(
            "positive", "🚀", "급상승 상품",
            ", ".join(names),
            priority=3,
        ))

    insights.sort(key=lambda x: x["priority"])
    return insights


# ──────────────────────────────────────
# 월간 인사이트
# ──────────────────────────────────────

def generate_monthly_insights(data: Dict, summary: pd.DataFrame, ym: str) -> List[Dict]:
    insights = []
    mr = monthly_report(data, summary, ym)
    tm = mr["this_month"]
    pm = mr["prev_month"]

    # 전월 대비 매출/GP 성장
    if pm["매출"] > 0:
        rev_pct = (tm["매출"] - pm["매출"]) / pm["매출"] * 100
        rev_diff = tm["매출"] - pm["매출"]
        type_ = "positive" if rev_pct >= 0 else "alert"
        icon = "📈" if rev_pct >= 0 else "📉"
        insights.append(_insight(
            type_, icon,
            f"월매출 {rev_pct:+.1f}%",
            f"{tm['매출']:,.0f}원 (전월 대비 {rev_diff:+,.0f}원)",
            priority=1,
        ))

    if pm["GP"] > 0:
        gp_pct = (tm["GP"] - pm["GP"]) / pm["GP"] * 100
        type_ = "positive" if gp_pct >= 0 else "alert"
        icon = "💰" if gp_pct >= 0 else "⚠️"
        insights.append(_insight(
            type_, icon,
            f"매출총이익 {gp_pct:+.1f}%",
            f"GP {tm['GP']:,.0f}원 (GP율 {tm['GP율']:.1f}%)",
            priority=1,
        ))

    # 카테고리 비중 변화 TOP 3
    cat_mv = monthly_category_movement(data, ym)
    if not cat_mv.empty and "비중변화" in cat_mv.columns:
        top_changes = cat_mv.reindex(cat_mv["비중변화"].abs().sort_values(ascending=False).index).head(3)
        if not top_changes.empty:
            names = top_changes.apply(
                lambda r: f"{r['erp_category']}({r['비중변화']:+.1f}%p)", axis=1
            ).tolist()
            insights.append(_insight(
                "action", "🔀", "카테고리 비중 변화",
                ", ".join(names),
                priority=2,
            ))

    # GP율 하위 카테고리 경고
    if not cat_mv.empty and "GP율" in cat_mv.columns:
        low_gp = cat_mv[(cat_mv["GP율"] > 0) & (cat_mv["GP율"] < 20) & (cat_mv["매출_이번달"] >= 100000)]
        if not low_gp.empty:
            names = low_gp.apply(
                lambda r: f"{r['erp_category']}(GP {r['GP율']:.1f}%)", axis=1
            ).tolist()
            insights.append(_insight(
                "alert", "⚠️", "GP율 주의 카테고리",
                ", ".join(names[:3]),
                priority=2,
            ))

    # 이상 징후 요약
    from analysis import analyze_outlier_days
    outlier = analyze_outlier_days(data, ym)
    if outlier and "outliers" in outlier:
        outlier_df = outlier["outliers"]
        if not outlier_df.empty:
            n_good = len(outlier_df[outlier_df["유형"] == "호조"])
            n_bad = len(outlier_df[outlier_df["유형"] == "부진"])
            detail_parts = []
            if n_good > 0:
                detail_parts.append(f"호조일 {n_good}일")
            if n_bad > 0:
                detail_parts.append(f"부진일 {n_bad}일")
            insights.append(_insight(
                "action", "🔍", f"특이일 {len(outlier_df)}건 감지",
                " / ".join(detail_parts),
                priority=3,
            ))

    insights.sort(key=lambda x: x["priority"])
    return insights
