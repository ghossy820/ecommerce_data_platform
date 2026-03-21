import os
import duckdb
import pandas as pd
import streamlit as st

st.set_page_config(page_title="DE Project - Dashboard", layout="wide")

# ==========================================
# MODULE 1: XỬ LÝ LOGIC & TRÍCH XUẤT (EXTRACT)
# ==========================================

def check_has_database_file(file_path: str) -> bool:
    """Kiểm tra sự tồn tại của file database."""
    is_file_existing = os.path.exists(file_path)
    return is_file_existing

def extract_records_from_database(sql_query: str, database_path: str) -> pd.DataFrame:
    """Trích xuất dữ liệu từ DuckDB và trả về DataFrame."""
    try:
        with duckdb.connect(database=database_path, read_only=True) as database_connection:
            extracted_dataframe = database_connection.execute(sql_query).df()
            return extracted_dataframe
    except Exception as execution_error:
        st.error(f"Lỗi hệ thống khi truy vấn: {execution_error}")
        return pd.DataFrame()

# ==========================================
# MODULE 2: HIỂN THỊ GIAO DIỆN (UI RENDER)
# ==========================================

def render_system_tables_information(database_path: str):
    """Hiển thị danh sách các bảng đang có trong hệ thống."""
    with st.expander("🕵️‍♂️ Kiểm tra danh sách bảng thực tế"):
        sql_query = "SELECT table_schema, table_name FROM information_schema.tables WHERE table_schema != 'information_schema'"
        system_tables_dataframe = extract_records_from_database(sql_query, database_path)
        
        if not system_tables_dataframe.empty:
            st.write(system_tables_dataframe)
        else:
            st.error("Không tìm thấy thông tin bảng trong cơ sở dữ liệu!")

def render_fact_sales_metrics(database_path: str):
    """Hiển thị số liệu và bảng dữ liệu giao dịch (Fact)."""
    st.subheader("📦 Marts: Fact Sales")
    sql_query = "SELECT * FROM main.fact_sales"
    fact_sales_dataframe = extract_records_from_database(sql_query, database_path)
    
    if not fact_sales_dataframe.empty:
        total_sales_records = len(fact_sales_dataframe)
        st.metric("Tổng đơn hàng", total_sales_records)
        st.dataframe(fact_sales_dataframe, use_container_width=True)
    else:
        st.warning("Chưa có dữ liệu trong bảng fact_sales.")

def render_dim_customers_metrics(database_path: str):
    """Hiển thị số liệu và bảng danh mục khách hàng (Dim)."""
    st.subheader("👥 Marts: Dim Customers")
    sql_query = "SELECT * FROM main.dim_customers"
    dim_customers_dataframe = extract_records_from_database(sql_query, database_path)
    
    if not dim_customers_dataframe.empty:
        total_customer_records = len(dim_customers_dataframe)
        st.metric("Tổng khách hàng", total_customer_records)
        st.dataframe(dim_customers_dataframe, use_container_width=True)
    else:
        st.warning("Chưa có dữ liệu trong bảng dim_customers.")

# ==========================================
# MODULE 3: ĐIỀU PHỐI CHÍNH (MAIN ORCHESTRATOR)
# ==========================================

def build_dashboard_interface():
    """Hàm main điều phối toàn bộ quá trình chạy của Dashboard."""
    st.title("🚀 E-commerce Data Lakehouse")
    
    database_file_path = 'dbt_project/ecommerce_dbt/lakehouse.duckdb'
    
    # Biến logic kiểm tra file
    is_database_ready = check_has_database_file(database_file_path)
    
    if not is_database_ready:
        st.error(f"❌ File không tồn tại tại: {os.path.abspath(database_file_path)}")
        st.stop()
        
    render_system_tables_information(database_file_path)
    
    st.divider()
    left_column, right_column = st.columns(2)
    
    with left_column:
        render_fact_sales_metrics(database_file_path)
        
    with right_column:
        render_dim_customers_metrics(database_file_path)

if __name__ == "__main__":
    build_dashboard_interface()