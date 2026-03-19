import os
import random
import psycopg2
from faker import Faker
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '../../.env'))
fake_generator = Faker()

def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("PG_HOST", "localhost"),
        port=os.getenv("PG_PORT", "5432"),
        database=os.getenv("PG_DB", "ecommerce_production"),
        user=os.getenv("PG_USER", "admin"),
        password=os.getenv("PG_PASSWORD", "secretpassword")
    )

def create_database_schema(db_connection):
    create_tables_query = """
    CREATE TABLE IF NOT EXISTS customers (
        customer_id SERIAL PRIMARY KEY,
        full_name VARCHAR(255),
        email_address VARCHAR(255),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS products (
        product_id SERIAL PRIMARY KEY,
        product_name VARCHAR(255),
        category_name VARCHAR(100),
        unit_price DECIMAL(10, 2)
    );

    CREATE TABLE IF NOT EXISTS sales_orders (
        order_id VARCHAR(50) PRIMARY KEY,
        customer_id INT REFERENCES customers(customer_id),
        order_date TIMESTAMP,
        total_amount DECIMAL(10, 2),
        order_status VARCHAR(50),
        updated_at TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS inventory_logs (
        log_id SERIAL PRIMARY KEY,
        product_id INT REFERENCES products(product_id),
        quantity_changed INT,
        log_timestamp TIMESTAMP
    );
    """
    with db_connection.cursor() as cursor:
        cursor.execute(create_tables_query)
    db_connection.commit()
    print("[Cập nhật] Đã khởi tạo cấu trúc 4 bảng thành công.")

def generate_foundation_data(db_connection, num_customers=50, num_products=20):
    with db_connection.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) FROM customers")
        if cursor.fetchone()[0] > 0:
            print("Dữ liệu nền tảng đã tồn tại, bỏ qua bước tạo mới.")
            return

        for _ in range(num_customers):
            customer_name = fake_generator.name()
            customer_email = fake_generator.email()

            is_flawed_email = random.random() < 0.05
            if is_flawed_email:
                customer_email = customer_email.replace('@', '_at_')

            cursor.execute(
                "INSERT INTO customers (full_name, email_address) VALUES (%s, %s)",
                (customer_name, customer_email)
            )

        categories = ['Điện thoại', 'Ốp lưng', 'Sạc cáp', 'Tai nghe']
        for _ in range(num_products):
            product_name = f"{random.choice(categories)} {fake_generator.word().capitalize()}"
            category = random.choice(categories)
            price = round(random.uniform(5.0, 1000.0), 2)
                
            cursor.execute(
                "INSERT INTO products (product_name, category_name, unit_price) VALUES (%s, %s, %s)",
                (product_name, category, price)
            )

    db_connection.commit()
    print(f"[Cập nhật] Đã sinh {num_customers} khách hàng và {num_products} sản phẩm.")

def generate_daily_transactions(db_connection, target_date, num_orders=100):
    with db_connection.cursor() as cursor:
        cursor.execute("SELECT customer_id FROM customers")
        valid_customer_ids = [row[0] for row in cursor.fetchall()]

        cursor.execute("SELECT product_id FROM products")
        valid_product_ids = [row[0] for row in cursor.fetchall()]

        if not valid_customer_ids or not valid_product_ids:
            return
        
        for _ in range(num_orders):
            order_id = fake_generator.uuid4()
            customer_id = random.choice(valid_customer_ids)
            amount = round(random.uniform(20.0, 2000.0), 2)
            
            is_negative_amount = random.random() < 0.02
            if is_negative_amount:
                amount = -amount
            
            status = random.choices(['completed', 'canceled', 'refunded'], weights=[80, 10, 10])[0]
            
            cursor.execute(
                "INSERT INTO sales_orders (order_id, customer_id, order_date, total_amount, order_status, updated_at) VALUES (%s, %s, %s, %s, %s, %s)",
                (order_id, customer_id, target_date, amount, status, target_date)
            )

            num_items = random.randint(1, 3)
            for _ in range(num_items):
                product_id = random.choice(valid_product_ids)
                quantity_change = -random.randint(1, 5) 
                cursor.execute(
                    "INSERT INTO inventory_logs (product_id, quantity_changed, log_timestamp) VALUES (%s, %s, %s)",
                    (product_id, quantity_change, target_date)
                )

    db_connection.commit()
    print(f"[Cập nhật] Đã sinh {num_orders} đơn hàng ngày {target_date.strftime('%Y-%m-%d')}.")

if __name__ == "__main__":
    postgres_conn = None
    try:
        print("--- BẮT ĐẦU SIMULATOR ---")
        postgres_conn = get_db_connection()
        
        create_database_schema(postgres_conn)
        generate_foundation_data(postgres_conn)
        
        today = datetime.now()
        for days_ago in range(3, -1, -1):
            simulation_date = today - timedelta(days=days_ago)
            generate_daily_transactions(postgres_conn, target_date=simulation_date, num_orders=50)
            
    except Exception as e:
        print(f"[Lỗi] Quá trình giả lập thất bại: {e}")
    finally:
        if postgres_conn:
            postgres_conn.close()
            print("--- KẾT THÚC SIMULATOR ---")