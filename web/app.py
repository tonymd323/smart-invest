#!/usr/bin/env python3
"""
投资系统 2.0 — Web 前端
发现池 / 扫描结果 / 事件 / 回测
"""
import streamlit as st
import sqlite3
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta

DB_PATH = Path(__file__).parent.parent / "data" / "smart_invest.db"

st.set_page_config(
    page_title="投资系统 2.0",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── 数据加载 ─────────────────────────────────────────────────────────────────

@st.cache_data(ttl=60)
def load_discovery_pool():
    """加载发现池"""
    conn = sqlite3.connect(str(DB_PATH))
    df = pd.read_sql_query("""
        SELECT stock_code, stock_name, industry, source, score, signal,
               status, discovered_at, expires_at
        FROM discovery_pool
        WHERE status = 'active'
        ORDER BY score DESC
    """, conn)
    conn.close()
    return df

@st.cache_data(ttl=60)
def load_scan_results():
    """加载最新扫描结果（超预期 + 扣非新高）"""
    conn = sqlite3.connect(str(DB_PATH))
    df = pd.read_sql_query("""
        SELECT ar.stock_code, COALESCE(s.name, ar.stock_code, dp.stock_code) as stock_name, s.industry,
               ar.analysis_type, ar.score, ar.signal, ar.summary, ar.created_at
        FROM analysis_results ar
        LEFT JOIN stocks s ON ar.stock_code = s.code
        WHERE ar.created_at >= datetime('now', '-7 days')
        ORDER BY ar.created_at DESC, ar.score DESC
    """, conn)
    conn.close()
    return df

@st.cache_data(ttl=60)
def load_events():
    """加载事件"""
    conn = sqlite3.connect(str(DB_PATH))
    df = pd.read_sql_query("""
        SELECT e.stock_code, COALESCE(s.name, ar.stock_code, dp.stock_code) as stock_name, e.event_type,
               e.title, e.content, e.severity, e.sentiment, e.published_at
        FROM events e
        LEFT JOIN stocks s ON e.stock_code = s.code
        WHERE e.published_at >= datetime('now', '-30 days') OR e.created_at >= datetime('now', '-30 days')
        ORDER BY e.published_at DESC
        LIMIT 100
    """, conn)
    conn.close()
    return df

@st.cache_data(ttl=60)
def load_consensus():
    """加载一致预期数据"""
    conn = sqlite3.connect(str(DB_PATH))
    df = pd.read_sql_query("""
        SELECT c.stock_code, COALESCE(s.name, ar.stock_code, dp.stock_code) as stock_name, s.industry,
               c.year, c.net_profit_yoy, c.rev_yoy, c.num_analysts
        FROM consensus c
        LEFT JOIN stocks s ON c.stock_code = s.code
        WHERE c.net_profit_yoy IS NOT NULL AND c.net_profit_yoy != 0
        ORDER BY c.stock_code, c.year
    """, conn)
    conn.close()
    return df

@st.cache_data(ttl=60)
def load_backtest():
    """加载回测数据"""
    conn = sqlite3.connect(str(DB_PATH))
    df = pd.read_sql_query("""
        SELECT stock_code, event_date, event_type, entry_price,
               return_5d, return_10d, return_20d, return_60d,
               benchmark_20d, alpha_20d, is_win
        FROM backtest
        WHERE return_5d IS NOT NULL
        ORDER BY event_date DESC
        LIMIT 200
    """, conn)
    conn.close()
    return df

# ── 侧边栏 ──────────────────────────────────────────────────────────────────

st.sidebar.title("📊 投资系统 2.0")
page = st.sidebar.radio("导航", ["🔍 发现池", "📋 扫描结果", "📰 事件流", "🎯 一致预期", "📈 回测"])

st.sidebar.markdown("---")
st.sidebar.caption(f"数据库: {DB_PATH}")
if DB_PATH.exists():
    mtime = datetime.fromtimestamp(DB_PATH.stat().st_mtime)
    st.sidebar.caption(f"最后更新: {mtime.strftime('%Y-%m-%d %H:%M')}")
else:
    st.sidebar.error("数据库不存在!")

# ── 发现池页面 ───────────────────────────────────────────────────────────────

if page == "🔍 发现池":
    st.title("🔍 发现池")
    st.caption("自动发现的候选股 — 超预期/扣非新高/回调买入")

    df = load_discovery_pool()

    if df.empty:
        st.info("暂无活跃发现池数据。晚间 21:00 扫描后自动更新。")
    else:
        # 统计卡片
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("活跃数量", len(df))
        col2.metric("buy 信号", len(df[df["signal"] == "buy"]))
        col3.metric("watch 信号", len(df[df["signal"] == "watch"]))
        col4.metric("平均评分", f"{df['score'].mean():.1f}")

        st.markdown("---")

        # 筛选
        col_filter1, col_filter2 = st.columns(2)
        with col_filter1:
            signal_filter = st.multiselect("信号筛选", ["buy", "watch", "hold"], default=["buy", "watch"])
        with col_filter2:
            source_filter = st.multiselect("来源筛选", df["source"].unique().tolist(), default=df["source"].unique().tolist())

        filtered = df[(df["signal"].isin(signal_filter)) & (df["source"].isin(source_filter))]

        # 表格
        st.dataframe(
            filtered[["stock_code", "stock_name", "industry", "source", "score", "signal", "discovered_at", "expires_at"]],
            use_container_width=True,
            hide_index=True,
            column_config={
                "stock_code": "代码",
                "stock_name": "名称",
                "industry": "行业",
                "source": "来源",
                "score": st.column_config.NumberColumn("评分", format="%.1f"),
                "signal": "信号",
                "discovered_at": "入池时间",
                "expires_at": "过期时间",
            },
        )

# ── 扫描结果页面 ─────────────────────────────────────────────────────────────

elif page == "📋 扫描结果":
    st.title("📋 扫描结果")
    st.caption("近 2 天分析结果 — 超预期 + 扣非新高")

    df = load_scan_results()

    if df.empty:
        st.info("暂无扫描结果。")
    else:
        # 分类统计
        beat = df[df["analysis_type"].str.contains("beat", na=False)]
        new_high = df[df["analysis_type"].str.contains("new_high|profit", na=False, case=False)]

        col1, col2, col3 = st.columns(3)
        col1.metric("超预期", len(beat))
        col2.metric("扣非新高", len(new_high))
        col3.metric("buy 信号", len(df[df["signal"] == "buy"]))

        st.markdown("---")

        # 超预期表格
        if not beat.empty:
            st.subheader("🔥 超预期信号")
            buy_signals = beat[beat["signal"] == "buy"]
            if not buy_signals.empty:
                st.dataframe(
                    buy_signals[["stock_code", "stock_name", "industry", "score", "signal", "created_at"]],
                    use_container_width=True, hide_index=True,
                )
            else:
                st.caption("暂无 buy 信号")

        # 扣非新高表格
        if not new_high.empty:
            st.subheader("📈 扣非净利润新高")
            st.dataframe(
                new_high[["stock_code", "stock_name", "score", "signal", "created_at"]],
                use_container_width=True, hide_index=True,
            )

# ── 事件流页面 ───────────────────────────────────────────────────────────────

elif page == "📰 事件流":
    st.title("📰 事件流")
    st.caption("近 7 天结构化事件")

    df = load_events()

    if df.empty:
        st.info("暂无事件数据。")
    else:
        for _, row in df.iterrows():
            severity_emoji = {"high": "🔴", "medium": "🟡", "low": "🟢"}.get(row["severity"], "⚪")
            sentiment_emoji = {"positive": "📈", "negative": "📉", "neutral": "➡️"}.get(row["sentiment"], "")

            with st.container():
                col1, col2 = st.columns([1, 10])
                with col1:
                    st.markdown(f"### {severity_emoji}")
                with col2:
                    st.markdown(f"**{row['title']}** {sentiment_emoji}")
                    st.caption(f"{row['stock_name'] or row['stock_code']} · {row['event_type']} · {row['published_at']}")
                    if row["content"]:
                        with st.expander("详情"):
                            st.text(row["content"])
            st.divider()

# ── 一致预期页面 ─────────────────────────────────────────────────────────────

elif page == "🎯 一致预期":
    st.title("🎯 一致预期")
    st.caption("AkShare 东方财富增长对比 — 多年净利润增速预期")

    df = load_consensus()

    if df.empty:
        st.info("暂无一致预期数据。")
    else:
        # 按股票展示
        stock_list = sorted(df["stock_code"].unique())
        selected = st.selectbox("选择股票", stock_list, format_func=lambda x: f"{x} {df[df['stock_code']==x]['stock_name'].iloc[0] if len(df[df['stock_code']==x]) > 0 else ''}")

        stock_df = df[df["stock_code"] == selected]

        if not stock_df.empty:
            st.subheader(f"📊 {stock_df['stock_name'].iloc[0]} ({selected})")

            # 年份对比表
            st.dataframe(
                stock_df[["year", "net_profit_yoy", "rev_yoy", "num_analysts"]],
                use_container_width=True, hide_index=True,
                column_config={
                    "year": "预期年份",
                    "net_profit_yoy": st.column_config.NumberColumn("净利润增速(%)", format="%.2f"),
                    "rev_yoy": st.column_config.NumberColumn("营收增速(%)", format="%.2f"),
                    "num_analysts": "分析师数",
                },
            )

            # 柱状图
            import plotly.express as px
            fig = px.bar(stock_df, x="year", y="net_profit_yoy",
                        title="净利润增速预期",
                        labels={"net_profit_yoy": "增速(%)", "year": "年份"},
                        color="year")
            st.plotly_chart(fig, use_container_width=True)

# ── 回测页面 ─────────────────────────────────────────────────────────────────

elif page == "📈 回测":
    st.title("📈 回测")
    st.caption("历史信号收益验证")

    df = load_backtest()

    if df.empty:
        st.info("暂无回测数据。")
    else:
        # 统计
        wins = df[df["is_win"] == True] if "is_win" in df.columns else pd.DataFrame()
        col1, col2, col3 = st.columns(3)
        col1.metric("总信号数", len(df))
        col2.metric("胜率", f"{len(wins)/len(df)*100:.1f}%" if len(df) > 0 else "N/A")
        col3.metric("平均 20 日收益", f"{df['return_20d'].mean():.2f}%" if df['return_20d'].notna().any() else "N/A")

        st.markdown("---")

        # 收益分布图
        import plotly.express as px
        fig = px.histogram(df, x="return_20d", nbins=20, title="20日收益分布",
                          labels={"return_20d": "收益(%)"})
        st.plotly_chart(fig, use_container_width=True)

        # 表格
        st.dataframe(
            df.head(50),
            use_container_width=True, hide_index=True,
            column_config={
                "stock_code": "代码",
                "event_date": "日期",
                "event_type": "事件",
                "entry_price": st.column_config.NumberColumn("入池价", format="%.2f"),
                "return_5d": st.column_config.NumberColumn("5日(%)", format="%.2f"),
                "return_10d": st.column_config.NumberColumn("10日(%)", format="%.2f"),
                "return_20d": st.column_config.NumberColumn("20日(%)", format="%.2f"),
                "return_60d": st.column_config.NumberColumn("60日(%)", format="%.2f"),
                "alpha_20d": st.column_config.NumberColumn("超额(%)", format="%.2f"),
            },
        )
