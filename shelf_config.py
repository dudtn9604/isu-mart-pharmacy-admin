"""
매대 구조 상수 정의
실제 매장 매대 사양 기준 (단위: cm, 원본은 mm)
단면 기준 — 매대 1대 = 5단 = 5개 위치
"""

# 매대 타입별 설정
# tiers: 각 단의 높이(cm), 하단(1단)부터 순서
# count: 해당 타입 매대 수 (단면 기준)
# width: 가로 폭(cm)
# note: 비고
SHELF_CONFIGS = {
    "A": {
        "name": "기본매대",
        "width": 90.0,        # 900mm
        "tiers": [25, 25, 25, 25, 999],  # 5단, 맨 윗단(5단) 높이 제한 없음
        "count": 21,
        "note": "맨 윗단(5단) 높이 제한 없음 (위로 뚫려 있음)",
    },
    "B": {
        "name": "연결매대",
        "width": 93.0,        # 930mm
        "tiers": [25, 25, 25, 25, 25],   # 5단 균일
        "count": 15,
        "note": "",
    },
    "C": {
        "name": "엔드캡매대",
        "width": 63.6,        # 636mm
        "tiers": [25, 25, 25, 25, 25],   # 5단 균일
        "count": 14,
        "note": "",
    },
}


def get_total_locations() -> int:
    """전체 선반 위치 수 계산 (단면 기준)"""
    total = 0
    for cfg in SHELF_CONFIGS.values():
        total += cfg["count"] * len(cfg["tiers"])
    return total


def generate_display_label(shelf_type: str, fixture_no: int, tier: int) -> str:
    """사람이 읽기 쉬운 위치 라벨 생성"""
    name = SHELF_CONFIGS.get(shelf_type, {}).get("name", "")
    return f"{shelf_type}-{fixture_no} / {tier}단"
