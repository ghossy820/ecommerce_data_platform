import pandas as pd
import io
from datetime import datetime, timedelta
from db_connector import get_postgres_engine, get_minio_client

def setup_bronze_bucket(minio_client, bucket_name="lakehouse"):
    is_bucket_exists = minio_client.bucket_exists(bucket_name)
    if not is_bucket_exists:
        minio_client.make_bucket(bucket_name)
        print(f"[Cập nhật] Đã tạo mới bucket: {bucket_name}")

def extract_daily_data_to_bronze(table_name, target_date, date_column_name=None):
    db_engine = get_postgres_engine()
    minio_client = get_minio_client()
    bucket_name = "lakehouse"
    setup_bronze_bucket(minio_client, bucket_name)

    if date_column_name:
        extract_query = f"SELECT * FROM {table_name} WHERE DATE({date_column_name}) = '{target_date}'"
        load_type = "INCREMENTAL"
    else:
        extract_query = f"SELECT * FROM {table_name}"
        load_type = "FULL"

    daily_data_df = pd.read_sql(extract_query, db_engine)

    if daily_data_df.empty:
        print(f"[{load_type}] Bảng {table_name}: Không có dữ liệu.")
        return
    
    parquet_buffer = io.BytesIO()
    daily_data_df.to_parquet(parquet_buffer, index=False, engine='pyarrow')
    parquet_buffer.seek(0)

    target_date_obj = datetime.strptime(target_date, "%Y-%m-%d") if target_date else datetime.now()
    partition_path = f"bronze/{table_name}/year={target_date_obj.strftime('%Y')}/month={target_date_obj.strftime('%m')}/day={target_date_obj.strftime('%d')}/data.parquet"

    minio_client.put_object(bucket_name, partition_path, data=parquet_buffer, length=parquet_buffer.getbuffer().nbytes)
    print(f"[Cập nhật] {table_name} ({load_type}): Đã tải {len(daily_data_df)} dòng lên {partition_path}")

if __name__ == "__main__":
    print("--- BẮT ĐẦU INGESTION PIPELINE ---")

    today_str = datetime.now().strftime('%Y-%m-%d')

    try:
        extract_daily_data_to_bronze("customers", today_str, "created_at")
        extract_daily_data_to_bronze("products", today_str)
        extract_daily_data_to_bronze("sales_orders", today_str, "updated_at")
        extract_daily_data_to_bronze("inventory_logs", today_str, "log_timestamp")

    except Exception as e:
        print(f"[Lỗi] Quá trình trích xuất thất bại: {e}")
        
    print("--- KẾT THÚC INGESTION PIPELINE ---")