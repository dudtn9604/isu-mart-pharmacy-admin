#!/bin/bash
# Shelf 위치별 성과 트래킹 대시보드 실행
cd "$(dirname "$0")"
streamlit run shelf_dashboard.py --server.port 8502
