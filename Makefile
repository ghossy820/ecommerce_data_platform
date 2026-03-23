# ==========================================
# MAKEFILE - E-COMMERCE DATA LAKEHOUSE
# ==========================================

.PHONY: help infra-up infra-down simulate ingest dbt-run dbt-test dash clean pipeline

help:
	@echo "🛠️ Hệ thống tự động hóa Pipeline dữ liệu:"
	@echo "--------------------------------------------------------"
	@echo "make infra-up    : Khởi động Docker (Postgres, MinIO)"
	@echo "make simulate    : Chạy script giả lập dữ liệu vào Postgres"
	@echo "make ingest      : Trích xuất dữ liệu từ Postgres sang Lakehouse (S3)"
	@echo "make dbt-run     : Chạy dbt để biến đổi dữ liệu trong DuckDB"
	@echo "make dash        : Khởi động Streamlit Dashboard"
	@echo "make pipeline    : Chạy toàn bộ quy trình từ Simulate đến dbt"
	@echo "make clean       : Dọn dẹp dự án"

# --- 1. HẠ TẦNG ---
infra-up:
	@echo "🚀 Khởi động Docker..."
	cd infra && docker compose --env-file ../.env up -d

infra-down:
	@echo "🛑 Tắt Docker..."
	cd infra && docker compose --env-file ../.env down

# --- 2. GIẢ LẬP & TRÍCH XUẤT (INGESTION) ---
simulate:
	@echo "🧪 Đang giả lập dữ liệu mua hàng..."
	./venv/bin/python3 scripts/simulator/generate_mock_data.py

ingest:
	@echo "📥 Đang trích xuất dữ liệu sang Parquet (Lakehouse)..."
	./venv/bin/python3 scripts/ingestion/extract_to_parquet.py

# --- 3. BIẾN ĐỔI (dbt) ---
dbt-run:
	@echo "⚙️ Chạy dbt pipeline..."
	cd dbt_project/ecommerce_dbt && dbt run

# --- 4. GIAO DIỆN ---
dash:
	@echo "📊 Khởi động Dashboard..."
	./venv/bin/streamlit run dashboard.py

# --- 5. QUY TRÌNH TỰ ĐỘNG TOÀN BỘ (E2E) ---
pipeline: simulate ingest dbt-run
	@echo "✅ Pipeline đã hoàn thành trọn vẹn!"

# --- 6. DỌN DẸP ---
clean:
	@echo "🧹 Dọn dẹp..."
	rm -f dbt_project/ecommerce_dbt/*.wal
	find . -type d -name "__pycache__" -exec rm -r {} +