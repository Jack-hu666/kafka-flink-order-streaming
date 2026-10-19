"""订单实时看板 —— Streamlit。

读取 PostgreSQL 结果表，每 5 秒自动刷新：
  - 顶部：最近更新时间 / 已处理窗口数
  - GMV 分钟级趋势折线
  - 分省份 GMV 柱状图
  - 商品金额 Top5 条形图

运行：streamlit run dashboard/app.py
"""
import os
from datetime import datetime

import pandas as pd
import plotly.express as px
import streamlit as st
import psycopg2

# dataviz 参考配色
CAT = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#6250d6", "#e34948"]
BLUE = "#2a78d6"
INK = "#0b0b0b"
MUTED = "#898781"
GRID = "#e1e0d9"

PG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "dbname": os.getenv("POSTGRES_DB", "orders"),
    "user": os.getenv("POSTGRES_USER", "postgres"),
    "password": os.getenv("POSTGRES_PASSWORD", "postgres"),
}


def _query(sql: str) -> pd.DataFrame:
    conn = psycopg2.connect(**PG)
    df = pd.read_sql(sql, conn)
    conn.close()
    return df


def _style(fig):
    fig.update_layout(
        font=dict(family="system-ui, Segoe UI, sans-serif", color=INK),
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        margin=dict(l=10, r=10, t=40, b=10),
    )
    fig.update_xaxes(gridcolor=GRID, linecolor="#c3c2b7", tickfont_color=MUTED)
    fig.update_yaxes(gridcolor=GRID, linecolor="#c3c2b7", tickfont_color=MUTED, zeroline=False)
    return fig


st.set_page_config(page_title="订单实时看板", layout="wide")
st.title("订单实时流处理看板")
st.caption("数据为模拟生成，管道为真实实现 · Kafka → PyFlink → PostgreSQL")

# 每 5 秒自动刷新
st_autorefresh(interval=5000, key="refresh")

try:
    minutely = _query("SELECT * FROM minutely_metrics ORDER BY window_start DESC LIMIT 120")
    province = _query("SELECT province, SUM(gmv) AS gmv, SUM(order_cnt) AS cnt "
                      "FROM province_metrics GROUP BY province")
    top_items = _query("SELECT item_id, SUM(total_amount) AS total_amount "
                       "FROM top_items GROUP BY item_id ORDER BY total_amount DESC LIMIT 5")
except Exception as exc:  # noqa: BLE001
    st.error(f"无法连接 PostgreSQL（{PG['host']}:5432）。请先 `docker compose up -d` 并启动 Flink 作业。\n\n{exc}")
    st.stop()

# ---- 顶部指标 ----
c1, c2, c3 = st.columns(3)
c1.metric("最近更新时间", datetime.now().strftime("%H:%M:%S"))
c2.metric("已处理窗口数", int(len(minutely)))
c3.metric("累计 GMV", f"¥{minutely['gmv'].sum():,.0f}" if len(minutely) else "¥0")

# ---- 图 1：GMV 分钟趋势 ----
st.subheader("GMV 分钟级趋势")
if len(minutely):
    fig1 = px.line(minutely.sort_values("window_start"), x="window_start", y="gmv",
                   color_discrete_sequence=[BLUE],
                   labels={"window_start": "窗口", "gmv": "GMV(元)"})
    fig1.update_traces(line=dict(width=2), mode="lines+markers", marker=dict(size=4))
    st.plotly_chart(_style(fig1), use_container_width=True)
else:
    st.info("暂无数据，等待 Flink 作业产出窗口…")

# ---- 图 2：分省份 GMV ----
st.subheader("分省份 GMV")
if len(province):
    fig2 = px.bar(province.sort_values("gmv", ascending=False), x="province", y="gmv",
                  color_discrete_sequence=[BLUE],
                  labels={"province": "省份", "gmv": "GMV(元)"})
    st.plotly_chart(_style(fig2), use_container_width=True)

# ---- 图 3：Top5 商品 ----
st.subheader("商品金额 Top5")
if len(top_items):
    fig3 = px.bar(top_items.sort_values("total_amount"), x="total_amount", y="item_id",
                  orientation="h", color_discrete_sequence=[BLUE],
                  labels={"item_id": "商品", "total_amount": "金额(元)"})
    st.plotly_chart(_style(fig3), use_container_width=True)
