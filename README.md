# 🛒 E-commerce Modern Data Lakehouse

Hệ thống Data Lakehouse End-to-End mô phỏng luồng xử lý dữ liệu thực tế của một nền tảng Thương mại Điện tử. Dự án tự động hóa hoàn toàn từ khâu sinh dữ liệu giả lập, trích xuất, biến đổi theo kiến trúc Medallion (Bronze - Silver - Gold), cho đến trực quan hóa trên Dashboard.

## 1. Tổng quan Dự án (Project Overview)
Dự án được xây dựng nhằm giải quyết bài toán xử lý dữ liệu phân tích với hiệu suất cao, chi phí thấp, và không phụ thuộc vào các công cụ trả phí. Thay vì dùng Data Warehouse truyền thống, dự án áp dụng kiến trúc **Lakehouse**, kết hợp sự linh hoạt của Object Storage (MinIO) và sức mạnh xử lý OLAP siêu tốc của DuckDB.

## 2. Điểm sáng Kỹ thuật (Technical Highlights)
* **Kiến trúc Lakehouse Hiện đại:** Xây dựng luồng dữ liệu chuẩn Medallion Architecture. Dữ liệu thô được lưu trữ dưới định dạng Parquet siêu nhẹ, sau đó xử lý trực tiếp bằng DuckDB.
* **Tự động hóa toàn diện (DataOps):** Đóng gói toàn bộ hạ tầng bằng Docker và tự động hóa quy trình chạy pipeline E2E chỉ bằng một lệnh Make (`make pipeline`).
* **Tiêu chuẩn Clean Code:** Ứng dụng triệt để nguyên tắc *Do One Thing* và đặt tên tường minh trong quá trình xây dựng Dashboard bằng Python Native.
* **Đảm bảo Chất lượng Dữ liệu (Data Quality):** Tích hợp bộ test của dbt (Unique, Not Null, Accepted Values) để chặn đứng dữ liệu rác trước khi lên Dashboard.
* **Xử lý Concurrent Lock:** Giải quyết thành công bài toán "Lock File" đặc trưng của DuckDB giữa luồng Transform (dbt) và luồng Read (Streamlit) bằng cơ chế quản lý Context Manager (with statement).

## 3. Tech Stack Sử dụng

| Lớp (Layer) | Công nghệ | Vai trò |
| :--- | :--- | :--- |
| **Source Database** | PostgreSQL | Lưu trữ dữ liệu giao dịch gốc (OLTP) |
| **Storage (Lakehouse)**| MinIO (S3 API) | Lưu trữ định dạng Parquet (Bronze Layer) |
| **Compute & DWH** | DuckDB | OLAP Engine xử lý dữ liệu siêu tốc |
| **Transformation** | dbt (data build tool)| Biến đổi logic Silver & Gold Layer |
| **Programming** | Python 3.10 | Viết script Ingestion, Simulator & Dashboard |
| **Visualization** | Streamlit | Giao diện Dashboard tương tác |
| **Infrastructure** | Docker & Makefile | Đóng gói và tự động hóa vận hành |

## 4. Kiến trúc Dữ liệu (Data Architecture)
1. **Simulator:** Python Script sinh dữ liệu giả lập (Khách hàng, Sản phẩm, Đơn hàng) đẩy vào PostgreSQL.
2. **Ingestion:** Trích xuất dữ liệu từ Postgres, chuyển đổi sang định dạng Parquet và tải lên MinIO (S3 Bucket).
3. **Transformation (dbt):** - *Staging (Silver):* Làm sạch kiểu dữ liệu, lọc email hợp lệ.
   - *Marts (Gold):* Tổng hợp doanh thu, tạo bảng Fact & Dim.