"""
레이아웃 저장 API 서버 (포트 8503)
이수/포레온 매대 배치도 에디터에서 레이아웃 저장 시 사용
shelf_dashboard.py 에서 subprocess로 시작됨
"""
import json
import sys
from http.server import HTTPServer, BaseHTTPRequestHandler
from pathlib import Path

BASE_DIR = Path(__file__).parent
LAYOUT_FILE = BASE_DIR / "shelf_layout.json"
FOREON_LAYOUT_FILE = BASE_DIR / "foreon_layout.json"


class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        if self.path == "/save-layout":
            length = int(self.headers["Content-Length"])
            data = json.loads(self.rfile.read(length))
            fx_list = data.get("fixtures", [])
            # DB 저장
            try:
                from shelf_data import bulk_update_fixture_positions
                if fx_list:
                    bulk_update_fixture_positions(fx_list)
            except Exception:
                pass
            # Supabase Storage + 로컬 파일 저장
            saved_storage = False
            try:
                from shelf_data import save_isu_layout
                saved_storage = save_isu_layout(data)
            except Exception as e:
                print(f"[API] save_isu_layout error: {e}", flush=True)
            if not saved_storage:
                try:
                    with open(str(LAYOUT_FILE), "w", encoding="utf-8") as f:
                        json.dump(data, f, ensure_ascii=False, indent=2)
                except Exception:
                    pass
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(json.dumps({"ok": True, "count": len(fx_list), "storage": saved_storage}).encode())

        elif self.path == "/save-foreon-layout":
            length = int(self.headers["Content-Length"])
            data = json.loads(self.rfile.read(length))
            saved_db = False
            try:
                from shelf_data import save_foreon_layout
                saved_db = save_foreon_layout(data)
            except Exception as e:
                print(f"[API] save_foreon_layout error: {e}", flush=True)
            # 로컬 파일 백업 (save_foreon_layout 내부에서도 하지만 안전하게)
            if not saved_db:
                try:
                    with open(str(FOREON_LAYOUT_FILE), "w", encoding="utf-8") as f:
                        json.dump(data, f, ensure_ascii=False, indent=2)
                except Exception:
                    pass
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            fx_count = len(data.get("fixtures", []))
            self.wfile.write(json.dumps({"ok": True, "count": fx_count, "db": saved_db}).encode())

        elif self.path == "/foreon-select-fixture":
            length = int(self.headers["Content-Length"])
            data = json.loads(self.rfile.read(length))
            fx_id = data.get("fixture_id", "")
            try:
                with open("/tmp/foreon_selected_fx.txt", "w") as f:
                    f.write(fx_id)
            except Exception:
                pass
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(json.dumps({"ok": True, "fixture_id": fx_id}).encode())

        else:
            self.send_response(404)
            self.end_headers()

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def log_message(self, *args):
        pass


def main():
    import time
    for attempt in range(5):
        try:
            server = HTTPServer(("0.0.0.0", 8503), Handler)
            print(f"Layout API server started on port 8503", flush=True)
            server.serve_forever()
            break
        except OSError as e:
            print(f"Port 8503 attempt {attempt} failed: {e}", flush=True)
            time.sleep(1)


if __name__ == "__main__":
    main()
