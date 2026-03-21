import streamlit as st
import duckdb
import pandas as pd

st.title("📊 E-commerce Executive Dashboard")

con = duckdb.connect(database='dbt_project/ecommerce_dbt/lakehouse.duckdb', read_only=True)

df_revenue = con.execute("SELECT transaction_date, SUM(net_revenue_amount) as revenue FROM fact_sales GROUP BY 1 ORDER BY 1").df()

st.line_chart(df_revenue.set_index('transaction_date'))

total_rev = df_revenue['revenue'].sum()
st.metric(label="Total Net Revenue", value=f"${total_rev:,.2f}")