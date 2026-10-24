import os
import altair as alt
import duckdb
import pandas as pd
import streamlit as st

data_folder = "C:/pipeline-data/"
quarantine_folder = "data/quarantine/"

st.set_page_config(page_title="E-Commerce Pipeline", layout="wide")

streams = {"orders": "orders", "customers": "customers",
           "products": "products", "clicks": "clickstream"}

con = duckdb.connect()
for name, folder in streams.items():
    con.execute(
        "create view " + name + " as select * from read_parquet('"
        + data_folder + "processed/" + folder + "/*.parquet')"
    )


def query(sql):
    return con.execute(sql).df()


def draw_line(df, x, y):
    top = float(df[y].max()) * 1.1
    chart = alt.Chart(df).mark_line(point=True).encode(
        x=alt.X(x + ":N"),
        y=alt.Y(y + ":Q", scale=alt.Scale(domain=[0, top])),
    )
    st.altair_chart(chart, use_container_width=True)


def draw_bar(df, x, y):
    top = float(df[y].max()) * 1.1
    chart = alt.Chart(df).mark_bar().encode(
        x=alt.X(x + ":N", sort="-y"),
        y=alt.Y(y + ":Q", scale=alt.Scale(domain=[0, top])),
    )
    st.altair_chart(chart, use_container_width=True)


st.title("Real-Time E-Commerce Data Pipeline")
st.caption(
    "Orders, customers and products: Olist Brazilian E-Commerce dataset "
    "(real, anonymized). Clickstream: synthetic. Amounts are in Brazilian reais (BRL)."
)

# ---- main numbers ----
summary = query(
    "select count(distinct order_id) as orders, sum(total_amount) as revenue "
    "from orders where order_status != 'canceled'"
)
orders = int(summary["orders"][0])
revenue = float(summary["revenue"][0])

col1, col2, col3 = st.columns(3)
col1.metric("Total orders", f"{orders:,}")
col2.metric("Total revenue (BRL)", f"{revenue:,.0f}")
col3.metric("Average order value (BRL)", f"{revenue / orders:,.2f}")

# ---- monthly revenue ----
st.subheader("Monthly revenue")
monthly = query(
    "select strftime(order_date, '%Y-%m') as month, sum(total_amount) as revenue "
    "from orders where order_status != 'canceled' group by month order by month"
)
draw_line(monthly, "month", "revenue")

# ---- categories and states ----
left, right = st.columns(2)

with left:
    st.subheader("Top 10 categories by revenue")
    categories = query(
        "select p.category, sum(o.total_amount) as revenue "
        "from orders o join products p on o.product_id = p.product_id "
        "where o.order_status != 'canceled' "
        "group by p.category order by revenue desc limit 10"
    )
    draw_bar(categories, "category", "revenue")

with right:
    st.subheader("Top 10 states by orders")
    states = query(
        "select c.state, count(distinct o.order_id) as orders "
        "from orders o join customers c on o.customer_id = c.customer_id "
        "group by c.state order by orders desc limit 10"
    )
    draw_bar(states, "state", "orders")

# ---- status and payment ----
left, right = st.columns(2)

with left:
    st.subheader("Orders by status")
    status = query(
        "select order_status, count(distinct order_id) as orders "
        "from orders group by order_status order by orders desc"
    )
    st.dataframe(status, hide_index=True)

with right:
    st.subheader("Payment methods")
    payment = query(
        "select payment_method, count(distinct order_id) as orders "
        "from orders group by payment_method order by orders desc"
    )
    st.dataframe(payment, hide_index=True)

# ---- click funnel ----
st.subheader("Click funnel (synthetic data)")
funnel = query("select action, count(*) as events from clicks group by action order by events desc")
draw_bar(funnel, "action", "events")

# ---- data quality ----
st.header("Data quality")
rows = []
for stream in ["orders", "customers", "products", "clickstream"]:
    raw = con.execute(
        "select count(*) from read_parquet('" + data_folder + "raw/" + stream + "/*.parquet')"
    ).fetchone()[0]
    processed = con.execute(
        "select count(*) from read_parquet('" + data_folder + "processed/" + stream + "/*.parquet')"
    ).fetchone()[0]

    quarantined = 0
    bad_file = quarantine_folder + stream + "_bad.jsonl"
    if os.path.exists(bad_file):
        quarantined = len(open(bad_file).readlines())

    rows.append({
        "stream": stream,
        "raw_records": raw,
        "processed_records": processed,
        "removed_by_spark": raw - processed,
        "quarantined_by_validator": quarantined,
    })

st.dataframe(pd.DataFrame(rows), hide_index=True)