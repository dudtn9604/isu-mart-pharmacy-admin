"""
트렌드 분석 모듈 - 설정 파일
네이버 Datalab API 키 및 시장 탐색용 키워드 관리

[설계 철학]
기존 카테고리를 추적하는 것이 아니라,
"시장에서 지금 뭐가 뜨고 있는가?"를 넓게 탐색한 후
현재 SKU에 없는 것을 발견 → SKU 추가 제안
"""

import os


def _secret(key: str, default: str = "") -> str:
    """Streamlit secrets → 환경변수 → 기본값 순으로 조회"""
    # 1) Streamlit secrets
    try:
        import streamlit as st
        if hasattr(st, "secrets") and key in st.secrets:
            return st.secrets[key]
    except Exception:
        pass
    # 2) 환경변수
    return os.environ.get(key, default)


# ──────────────────────────────────────
# 네이버 API 설정
# ──────────────────────────────────────
NAVER_CLIENT_ID = _secret("NAVER_CLIENT_ID", "GmVfscAWvNgXjBHkze8r")
NAVER_CLIENT_SECRET = _secret("NAVER_CLIENT_SECRET", "3xC8WyAmKO")


# ──────────────────────────────────────
# 시장 탐색용 키워드 그룹
# ──────────────────────────────────────
# 목적: 약국/건강기능식품 시장의 검색 트렌드를 넓게 탐색
# 기존 카테고리에 국한하지 않고, 새로운 트렌드를 발견하는 데 초점
#
# 네이버 Datalab API 제약:
# - 1회 호출당 최대 5개 키워드 그룹
# - 그룹당 최대 20개 키워드
# - 일 1,000회 호출 제한

TREND_KEYWORD_GROUPS = {
    # ──────────────────────────────────────
    # 1. 주요 건강기능식품 성분 트렌드
    # ──────────────────────────────────────
    "핫성분_비타민미네랄": {
        "display_name": "비타민·미네랄 성분",
        "keywords": [
            "비타민D", "마그네슘", "아연", "비타민B12",
            "철분", "비타민K", "셀레늄", "크롬",
            "비타민C 메가도스", "리포좀비타민C",
            "킬레이트마그네슘", "액티브비타민",
        ],
        "related_category": "피로회복·종합영양",
    },
    "핫성분_장건강": {
        "display_name": "장건강·프로바이오틱스",
        "keywords": [
            "포스트바이오틱스", "프리바이오틱스", "신바이오틱스",
            "장누수", "장건강영양제", "유산균추천",
            "모유유산균", "김치유산균", "스포어유산균",
            "장내미생물", "마이크로바이옴", "SIBO",
        ],
        "related_category": "위장 건강",
    },
    "핫성분_항노화": {
        "display_name": "항노화·안티에이징",
        "keywords": [
            "NMN", "NAD", "레스베라트롤", "코엔자임Q10",
            "글루타치온", "PQQ", "스퍼미딘",
            "항산화영양제", "텔로미어", "미토콘드리아영양제",
            "노화방지영양제", "안티에이징영양제",
        ],
        "related_category": "이너뷰티",
    },
    "핫성분_콜라겐뷰티": {
        "display_name": "콜라겐·이너뷰티",
        "keywords": [
            "저분자콜라겐", "피쉬콜라겐", "콜라겐펩타이드",
            "먹는히알루론산", "세라마이드", "엘라스틴",
            "피부영양제", "먹는콜라겐추천", "이너뷰티추천",
            "피부탄력영양제", "기미영양제",
        ],
        "related_category": "이너뷰티",
    },
    "핫성분_오메가3지방산": {
        "display_name": "오메가3·지방산",
        "keywords": [
            "알티지오메가3", "식물성오메가3", "크릴오일",
            "DHA", "EPA", "오메가3추천",
            "고함량오메가3", "초임계오메가3",
            "오메가3효능", "혈행건강",
        ],
        "related_category": "뇌건강·수면·스트레스",
    },

    # ──────────────────────────────────────
    # 2. 건강 고민별 트렌드 (사람들이 검색하는 증상/고민)
    # ──────────────────────────────────────
    "고민_수면스트레스": {
        "display_name": "수면·스트레스 고민",
        "keywords": [
            "수면영양제", "불면증영양제", "멜라토닌",
            "테아닌", "가바GABA", "트립토판",
            "수면유도제", "수면건강", "수면질개선",
            "스트레스해소", "코르티솔낮추기", "마그네슘수면",
        ],
        "related_category": "뇌건강·수면·스트레스",
    },
    "고민_다이어트체중": {
        "display_name": "다이어트·체중관리",
        "keywords": [
            "다이어트영양제", "가르시니아", "CLA",
            "그린커피빈", "키토산", "카르니틴",
            "식욕억제", "체지방감소", "내장지방",
            "GLP1다이어트", "위고비", "삭센다",
            "다이어트유산균", "모나콜린K",
        ],
        "related_category": "생활건강",
    },
    "고민_혈당혈압": {
        "display_name": "혈당·혈압 관리",
        "keywords": [
            "혈당영양제", "바나바잎", "여주",
            "혈당스파이크", "당뇨영양제", "혈당관리",
            "혈압영양제", "코엔자임Q10혈압", "혈압낮추기",
            "콜레스테롤영양제", "홍국", "오메가3콜레스테롤",
        ],
        "related_category": "생활건강",
    },
    "고민_면역력": {
        "display_name": "면역력 강화",
        "keywords": [
            "면역력영양제", "프로폴리스", "베타글루칸",
            "면역력높이는방법", "비타민D면역",
            "초유", "아연면역", "홍삼면역",
            "엘더베리", "에키네시아", "면역력강화식품",
        ],
        "related_category": "피로회복·종합영양",
    },
    "고민_탈모두피": {
        "display_name": "탈모·두피 고민",
        "keywords": [
            "탈모영양제", "비오틴탈모", "판시딜",
            "미녹시딜", "탈모샴푸추천", "여성탈모",
            "두피케어", "두피스케일링", "모발영양제",
            "탈모병원", "핀페시아",
        ],
        "related_category": "두피·탈모",
    },

    # ──────────────────────────────────────
    # 3. 새로운/떠오르는 트렌드 영역
    # ──────────────────────────────────────
    "신트렌드_간건강": {
        "display_name": "간건강·해독",
        "keywords": [
            "밀크씨슬", "간영양제", "실리마린",
            "UDCA", "간해독", "숙취해소영양제",
            "글루타치온간", "간수치낮추기",
            "지방간영양제", "간건강추천",
        ],
        "related_category": "생활건강",
    },
    "신트렌드_갱년기": {
        "display_name": "갱년기·호르몬",
        "keywords": [
            "갱년기영양제", "이소플라본", "승마추출물",
            "갱년기증상", "폐경영양제", "에스트로겐",
            "남성갱년기", "테스토스테론",
            "호르몬밸런스", "갱년기관절",
        ],
        "related_category": "여성건강",
    },
    "신트렌드_눈건강": {
        "display_name": "눈건강·블루라이트",
        "keywords": [
            "루테인지아잔틴", "아스타잔틴", "빌베리",
            "눈영양제추천", "블루라이트차단",
            "안구건조증", "눈피로영양제",
            "루테인추천", "망막건강", "눈건강식품",
        ],
        "related_category": "눈 건강",
    },
    "신트렌드_관절연골": {
        "display_name": "관절·연골 건강",
        "keywords": [
            "MSM", "보스웰리아", "콘드로이틴",
            "글루코사민", "관절영양제추천", "무릎영양제",
            "연골주사", "히알루론산관절",
            "SAMe관절", "호관원", "관절보궁",
        ],
        "related_category": "관절",
    },
    "신트렌드_어린이성장": {
        "display_name": "어린이 성장·면역",
        "keywords": [
            "키성장영양제", "어린이칼슘", "성장판",
            "어린이유산균", "어린이비타민D",
            "어린이오메가3", "초등학생영양제",
            "성장호르몬", "HMB어린이", "키크는영양제",
        ],
        "related_category": "어린이 건강",
    },
    "신트렌드_반려동물건강": {
        "display_name": "반려동물 건강",
        "keywords": [
            "강아지영양제", "고양이영양제", "반려동물관절",
            "펫유산균", "강아지피부영양제",
            "고양이비타민", "반려동물오메가3",
            "강아지눈물", "펫건강식품", "동물영양제",
        ],
        "related_category": "반려동물",
    },

    # ──────────────────────────────────────
    # 4. 약국 구매 행동 트렌드
    # ──────────────────────────────────────
    "구매행동_약국추천": {
        "display_name": "약국 추천·구매",
        "keywords": [
            "약국영양제추천", "약국비타민", "약사추천영양제",
            "약국유산균", "약국콜라겐",
            "약국다이어트", "약국탈모약",
            "처방전없이사는약", "편의점영양제", "약국필수템",
        ],
        "related_category": None,  # 전반적 구매 행동
    },
    "구매행동_연령대별": {
        "display_name": "연령대별 건강",
        "keywords": [
            "20대영양제", "30대영양제", "40대영양제",
            "50대영양제", "60대영양제",
            "부모님영양제", "직장인영양제",
            "수험생영양제", "임산부영양제", "시니어영양제",
        ],
        "related_category": None,  # 전반적 시장 트렌드
    },
}


# ──────────────────────────────────────
# 홈쇼핑모아 트렌드 설정
# ──────────────────────────────────────
HSMOA_EMAIL = _secret("HSMOA_EMAIL", "ys@hanah1.com")
HSMOA_PASSWORD = _secret("HSMOA_PASSWORD", "$Kys0803")
HSMOA_TREND_URL = "https://trend.hsmoa-ad.com"
HSMOA_SEARCH_URL = "https://hsmoa.com"

# 홈쇼핑 건강식품 검색 키워드 (hsmoa.com 검색용)
HSMOA_HEALTH_KEYWORDS = [
    # 주요 건강기능식품 카테고리
    "비타민", "오메가3", "유산균", "프로바이오틱스", "콜라겐",
    "루테인", "밀크씨슬", "홍삼", "관절영양제", "칼슘",
    # 인기 성분
    "마그네슘", "아연", "비오틴", "코엔자임Q10", "글루타치온",
    "보스웰리아", "MSM", "이소플라본", "프로폴리스",
    # 건강 고민
    "다이어트영양제", "혈당관리", "수면영양제", "간영양제",
    "눈영양제", "탈모영양제", "갱년기영양제",
    # 대상별
    "어린이영양제", "임산부영양제", "시니어영양제",
]

# 홈쇼핑모아 트렌드 사이트 카테고리 ID
HSMOA_CATEGORIES = {
    "건강": 6039,
    "식품": 8380,
    "건강식품": 8566,
    "뷰티": 6820,
    "반려동물": 6673,
    # 건강식품 3차 카테고리
    "비타민": 8622,
    "오메가3": 8643,
    "관절영양식품": 8601,
    "뷰티푸드": 8613,
    "개별인정건강식품": 8569,
    "어린이영양제": 8636,
    "홍삼인삼": 8663,
    "프로폴리스": 8648,
}


# ──────────────────────────────────────
# 바로팜 설정
# ──────────────────────────────────────
BAROPHARM_USERNAME = _secret("BAROPHARM_USERNAME", "isumartpharmacy@gmail.com")
BAROPHARM_PASSWORD = _secret("BAROPHARM_PASSWORD", "isumart4565@")
BAROPHARM_LOGIN_URL = "https://api.baropharm.com/api/rest-auth/login/"
BAROPHARM_API_URL = "https://api-v2.baropharm.com"
BAROPHARM_WEB_VERSION = "8.20.1"

# 바로팜 검색 키워드 (의약품 + 건강식품)
BAROPHARM_HEALTH_KEYWORDS = [
    # 주요 의약품 카테고리
    "탈모", "여드름", "무좀", "비염", "소화제", "변비", "수면",
    "피로회복", "감기", "진통제", "파스", "안약",
    # 건강기능식품 카테고리
    "비타민", "오메가3", "유산균", "콜라겐", "루테인",
    "밀크씨슬", "마그네슘", "아연", "비오틴", "홍삼",
    "프로바이오틱스", "코큐텐", "글루타치온", "보스웰리아",
    "칼슘", "철분", "프로폴리스", "경옥고",
]


# ──────────────────────────────────────
# Supabase ERP DB 설정 (이수마트약국)
# ──────────────────────────────────────
SUPABASE_URL = _secret("SUPABASE_URL", "https://kxkmsiyjtleqxitdjggr.supabase.co")
SUPABASE_ANON_KEY = _secret("SUPABASE_ANON_KEY", "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Imt4a21zaXlqdGxlcXhpdGRqZ2dyIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzE4MTEzNTMsImV4cCI6MjA4NzM4NzM1M30.iG7CVTyOTAH6tBpmEEF-A_uIUrVCCys_sXBtAcPbD9U")
SUPABASE_SERVICE_ROLE_KEY = _secret("SUPABASE_SERVICE_ROLE_KEY", "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Imt4a21zaXlqdGxlcXhpdGRqZ2dyIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3MTgxMTM1MywiZXhwIjoyMDg3Mzg3MzUzfQ.24lKO7tv_LpNSWCDxxm6ENTlNOBJ3jc5j7cQuf_AipI")
SUPABASE_STORE_ID = _secret("SUPABASE_STORE_ID", "1f1b2ffc-12de-407a-8478-bde8799701ac")

# 테이블 구조 참고:
# - products (~2,000) : 상품 마스터 (erp_category 사용, toss 카테고리 사용 금지)
# - toss_orders (~24,000) : 주문 헤더 (store_id 필터 필수)
# - order_items (~43,000) : 주문 상세
# - receiving_lots : 입고/발주
# - sale_cost_records (~24,000) : 매출원가 기록
# - card_expenses : 카드 비용
# - stores : 매장 정보


# ──────────────────────────────────────
# API 호출 설정
# ──────────────────────────────────────
API_BATCH_SIZE = 5
API_CALL_DELAY = 0.5

# ──────────────────────────────────────
# 트렌드 데이터 저장 설정
# ──────────────────────────────────────
TREND_DATA_DIR = "trend_data"
TREND_CACHE_HOURS = 24
