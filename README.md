🛒 E-Commerce Data Lakehouse: Hệ thống Xử lý Dữ liệu Toàn diện (End-to-End)
📌 Tổng quan dự án
Dự án này xây dựng một hệ thống Data Lakehouse hoàn chỉnh, mô phỏng môi trường thương mại điện tử thực tế. Hệ thống thực hiện trích xuất dữ liệu từ nguồn vận hành (Operational Data), lưu trữ vào kho dữ liệu thô (Data Lake), sau đó chuẩn hóa và biến đổi thành mô hình Star Schema sẵn sàng cho việc phân tích báo cáo (BI).

Điểm cốt lõi của dự án là áp dụng kiến trúc Tách biệt Lưu trữ và Tính toán (Storage-Compute Separation) và tư duy Lập trình Phòng thủ (Defensive Programming), giúp hệ thống vận hành mượt mà ngay cả trên máy cá nhân có cấu hình hạn chế (RAM 8GB).

🛠️ Công nghệ sử dụng & Kiến trúc
Trình giả lập dữ liệu (Simulator): Python (Faker, psycopg2) – Tự động sinh dữ liệu "bẩn" để thử thách hệ thống.

Cơ sở dữ liệu vận hành (OLTP): PostgreSQL (Dockerized).

Kho lưu trữ đối tượng (Object Storage): MinIO (Chuẩn S3-compatible, Dockerized) – Đóng vai trò Data Lake.

Công cụ xử lý & Truy vấn: DuckDB (Công cụ truy vấn phân tích cực nhanh trên RAM).

Khung biến đổi dữ liệu: dbt (Data Build Tool) – Quản lý logic SQL chuyên nghiệp.

Trực quan hóa (BI): Streamlit (Dashboard điều hành).

💡 Các quyết định kỹ thuật quan trọng
1. Tại sao chọn MinIO + DuckDB thay vì Spark/Hadoop?
Các công cụ Big Data truyền thống như Spark đòi hỏi tài nguyên RAM và cấu hình cực kỳ phức tạp. Trong dự án này, tôi chọn DuckDB vì khả năng quét trực tiếp các file Parquet trên MinIO qua mạng mà không cần nạp toàn bộ vào RAM. Điều này mô phỏng hoàn hảo kiến trúc Cloud-native (như AWS S3 + Athena) nhưng vẫn chạy cực nhanh trên máy tính cá nhân.

2. Kiểm soát chất lượng dữ liệu (Data Quality)
Dữ liệu thực tế không bao giờ sạch. Trình giả lập Python được lập trình để cố tình "bơm" dữ liệu lỗi (doanh thu âm, email sai định dạng).

Tầng Silver (Staging): Sử dụng SQL để cắm cờ và lọc bỏ các dòng lỗi.

Tầng Gold (Marts): Áp dụng dbt tests (unique, not_null, accepted_values) để đảm bảo 100% dữ liệu trên báo cáo cuối cùng là chính xác.

3. Tối ưu hóa lưu trữ với Parquet
Toàn bộ dữ liệu từ Postgres được chuyển đổi sang định dạng cột Parquet (nén cao, tốc độ đọc nhanh). Dữ liệu được phân vùng (Partitioning) theo cấu trúc thư mục year/month/day để hỗ trợ việc nạp bù dữ liệu (Backfill) và nạp thêm (Incremental Load) hiệu quả.

🚀 Hướng dẫn triển khai nhanh
Yêu cầu hệ thống
Docker & Docker Compose.

Python 3.10+ & pip.

WSL2 (Nếu dùng Windows).

Các bước thực hiện
Khởi động hạ tầng:

Bash
docker-compose --env-file .env -f infra/docker-compose.yml up -d
Sinh dữ liệu mẫu (Postgres):

Bash
python3 scripts/simulator/generate_mock_data.py
Chạy Pipeline nạp dữ liệu (Postgres -> MinIO):

Bash
python3 scripts/ingestion/extract_to_parquet.py
Biến đổi dữ liệu & Chạy bài kiểm tra (dbt):

Bash
cd dbt_project/ecommerce_dbt
dbt build --profiles-dir .
Mở Dashboard báo cáo:

Bash
python3 -m streamlit run scripts/dashboard.py
📊 Mô hình dữ liệu (Star Schema)
Tầng Gold cuối cùng bao gồm:

fact_sales: Chứa các chỉ số giao dịch cốt lõi, đã tính toán Doanh thu thực tế (Net Revenue) sau khi loại bỏ các đơn bị hủy/hoàn.

dim_customers: Danh mục khách hàng đã được chuẩn hóa email.

dim_products: Danh mục sản phẩm hiện có.