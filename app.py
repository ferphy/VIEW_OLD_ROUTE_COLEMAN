import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import text
from db.session import db_session
from datetime import datetime, timedelta, date
import calendar
import os
from dotenv import load_dotenv

# --- CONFIG LOADING ---
load_dotenv()
APP_TITLE = os.getenv("APP_TITLE", "Route Timeline Viewer")
TECH_TIMELINE_QUERY = os.getenv("TECH_TIMELINE_QUERY")

if not TECH_TIMELINE_QUERY:
    st.error("Missing TECH_TIMELINE_QUERY in .env")
    st.stop()

# --- PAGE CONFIG ---
st.set_page_config(
    page_title=APP_TITLE,
    page_icon="🚚",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# --- CUSTOM CSS ---
st.markdown(
    """
<style>
    .main {
        background-color: #0e1117;
    }
    .stApp {
        background: radial-gradient(circle at top right, #1a1c23, #0e1117);
    }
    [data-testid="stHeader"] {
        background: rgba(14, 17, 23, 0.8);
        backdrop-filter: blur(10px);
    }
    .title-text {
        font-family: 'Inter', sans-serif;
        font-weight: 800;
        background: linear-gradient(90deg, #00C9FF 0%, #92FE9D 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        font-size: 3rem;
        margin-bottom: 0.5rem;
    }
    .card {
        background: rgba(255, 255, 255, 0.05);
        padding: 1.5rem;
        border-radius: 15px;
        border: 1px solid rgba(255, 255, 255, 0.1);
        margin-bottom: 2rem;
    }
</style>
""",
    unsafe_allow_html=True,
)

# Queries are now loaded from .env


# --- DATA FETCHING ---
@st.cache_data(ttl=3600)
def get_route_data(start_date_obj, end_date_obj):
    query = text(TECH_TIMELINE_QUERY)
    start_str = start_date_obj.strftime("%Y-%m-%d")
    end_str = end_date_obj.strftime("%Y-%m-%d")

    with next(db_session()) as session:
        result = session.execute(query, {"start_date": start_str, "end_date": end_str})
        return pd.DataFrame(result.all(), columns=result.keys())


# --- PROCESSING LOGIC ---
def calculate_efficiency_stats(
    df, target_date, day_start_calc, day_end_calc, total_work_mins_per_day
):
    if df.empty:
        return pd.DataFrame(), 0, 0, 0, 0

    rows = []
    # Note: For monthly stats we don't need a vis window, but we reuse the clipping logic

    # Group by Technician and Date to handle multiple days in month
    # First, flatten the data for easy interval processing
    for _, row in df.iterrows():
        tech = row["LeadTechnician"] if row["LeadTechnician"] else "Unknown"

        def add_interval(start, end, task, session_id):
            if not start or not end or start >= end:
                return
            rows.append(
                {
                    "Technician": tech,
                    "Start": start,
                    "End": end,
                    "Task": task,
                    "Date": start.date(),
                }
            )

        add_interval(
            row["StartedTravel"], row["ArrivalTimeReal"], "Travel", row["SessionID"]
        )
        add_interval(
            row["ArrivalTimeReal"],
            row["DepartureTimeReal"],
            "Service",
            row["SessionID"],
        )

    plot_df = pd.DataFrame(rows)
    if plot_df.empty:
        return pd.DataFrame(), 0, 0, 0, 0

    # --- FILTER MINIMAL DATA (< 5 mins total active time per technician per day) ---
    plot_df["Duration_Min"] = (
        plot_df["End"] - plot_df["Start"]
    ).dt.total_seconds() / 60
    tech_daily_active = (
        plot_df.groupby(["Technician", "Date"])["Duration_Min"].sum().reset_index()
    )
    valid_threshold = tech_daily_active[tech_daily_active["Duration_Min"] >= 5]

    # Merge back to keep only valid days/technicians
    plot_df = plot_df.merge(
        valid_threshold[["Technician", "Date"]], on=["Technician", "Date"]
    )

    # --- METRICS ---
    global_idle_secs = 0
    global_travel_secs = 0
    global_service_secs = 0
    total_tech_days = 0

    stats_data = []

    # Iterate by Technician and Date
    grouped = plot_df.groupby(["Technician", "Date"])
    for (tech, task_date), tech_tasks in grouped:
        tech_tasks = tech_tasks.sort_values("Start")

        # Adjust calculation window for THIS specific date
        d_start_c = datetime.combine(task_date, day_start_calc.time())
        d_end_c = datetime.combine(task_date, day_end_calc.time())

        tech_idle_secs = 0
        tech_travel_secs = 0
        tech_service_secs = 0
        current_time = d_start_c

        for _, task in tech_tasks.iterrows():
            t_start_c = max(task["Start"], d_start_c)
            t_end_c = min(task["End"], d_end_c)

            if t_start_c < t_end_c:
                if t_start_c > current_time:
                    tech_idle_secs += (t_start_c - current_time).total_seconds()

                duration = (t_end_c - t_start_c).total_seconds()
                if task["Task"] == "Travel":
                    tech_travel_secs += duration
                else:
                    tech_service_secs += duration

                current_time = max(current_time, t_end_c)

        if current_time < d_end_c:
            tech_idle_secs += (d_end_c - current_time).total_seconds()

        global_idle_secs += tech_idle_secs
        global_travel_secs += tech_travel_secs
        global_service_secs += tech_service_secs
        total_tech_days += 1

        stats_data.append(
            {
                "Technician": tech,
                "Date": task_date,
                "Idle Secs": tech_idle_secs,
                "Travel Secs": tech_travel_secs,
                "Service Secs": tech_service_secs,
            }
        )

    # Aggregate by Technician (for the summary cards)
    # Sum only numeric columns to avoid TypeError with Date column
    agg_stats = (
        pd.DataFrame(stats_data)
        .groupby("Technician")[["Idle Secs", "Travel Secs", "Service Secs"]]
        .sum()
        .reset_index()
    )
    agg_stats["Idle Mins"] = (agg_stats["Idle Secs"] / 60).astype(int)
    agg_stats["Travel Mins"] = (agg_stats["Travel Secs"] / 60).astype(int)

    # Total work mins for the period for EACH technician is: (number of days worked) * 540
    tech_days_worked = (
        plot_df[["Technician", "Date"]]
        .drop_duplicates()
        .groupby("Technician")
        .size()
        .reset_index(name="Days")
    )
    agg_stats = agg_stats.merge(tech_days_worked, on="Technician")
    agg_stats["Total Work Mins"] = agg_stats["Days"] * total_work_mins_per_day

    agg_stats["Idle %"] = round(
        (agg_stats["Idle Mins"] / agg_stats["Total Work Mins"]) * 100, 1
    )
    agg_stats["Travel %"] = round(
        (agg_stats["Travel Mins"] / agg_stats["Total Work Mins"]) * 100, 1
    )
    agg_stats["Opt %"] = round(
        (
            (agg_stats["Idle Mins"] + agg_stats["Travel Mins"])
            / agg_stats["Total Work Mins"]
        )
        * 100,
        1,
    )

    total_possible_secs = total_tech_days * total_work_mins_per_day * 60

    global_stats = {
        "idle_pct": round((global_idle_secs / total_possible_secs) * 100, 1)
        if total_possible_secs > 0
        else 0,
        "travel_pct": round((global_travel_secs / total_possible_secs) * 100, 1)
        if total_possible_secs > 0
        else 0,
        "service_pct": round((global_service_secs / total_possible_secs) * 100, 1)
        if total_possible_secs > 0
        else 0,
    }

    return (
        agg_stats,
        global_stats["idle_pct"],
        global_stats["travel_pct"],
        global_stats["service_pct"],
        plot_df,
    )


# --- APP UI ---
st.markdown(f'<h1 class="title-text">{APP_TITLE}</h1>', unsafe_allow_html=True)

# Sidebar filters
with st.sidebar:
    st.header("Filters")
    selected_date = st.date_input("Focus Date", value=date(2025, 10, 1))
    enable_clipping = st.checkbox("Crop Workday (06:00 - 19:00)", value=True)
    st.info(
        "The monthly dashboard aggregates data based on the month of the Focus Date."
    )

# Constants
DAY_START_CALC = datetime.combine(date.today(), datetime.min.time()).replace(hour=8)
DAY_END_CALC = datetime.combine(date.today(), datetime.min.time()).replace(hour=17)
TOTAL_WORK_MINS_PER_DAY = 540

try:
    # --- MONTHLY DASHBOARD ---
    m_start = selected_date.replace(day=1)
    m_end = m_start + timedelta(
        days=calendar.monthrange(selected_date.year, selected_date.month)[1]
    )

    with st.spinner("Fetching Monthly Records..."):
        month_df = get_route_data(m_start, m_end)

    if not month_df.empty:
        agg_month, g_idle_m, g_travel_m, g_service_m, _ = calculate_efficiency_stats(
            month_df,
            selected_date,
            DAY_START_CALC,
            DAY_END_CALC,
            TOTAL_WORK_MINS_PER_DAY,
        )

        st.markdown(f"### Performance Summary - {selected_date.strftime('%B %Y')}")
        m_col1, m_col2, m_col3 = st.columns(3)

        with m_col1:
            st.markdown(
                f"""
                <div class="card" style="text-align: center; border-left: 5px solid #FF4B4B;">
                    <div style="color: #888; font-size: 0.9rem;">Monthly Idle Time</div>
                    <div style="font-size: 1.8rem; font-weight: bold; color: #FF4B4B;">{g_idle_m}%</div>
                    <div style="font-size: 0.8rem; color: #666;">Aggregate inefficiency</div>
                </div>
            """,
                unsafe_allow_html=True,
            )
        with m_col2:
            st.markdown(
                f"""
                <div class="card" style="text-align: center; border-left: 5px solid #00C9FF;">
                    <div style="color: #888; font-size: 0.9rem;">Monthly Travel Time</div>
                    <div style="font-size: 1.8rem; font-weight: bold; color: #00C9FF;">{g_travel_m}%</div>
                    <div style="font-size: 0.8rem; color: #666;">On the road</div>
                </div>
            """,
                unsafe_allow_html=True,
            )
        with m_col3:
            st.markdown(
                f"""
                <div class="card" style="text-align: center; border-left: 5px solid #92FE9D;">
                    <div style="color: #888; font-size: 0.9rem;">Monthly Productive Time</div>
                    <div style="font-size: 1.8rem; font-weight: bold; color: #92FE9D;">{g_service_m}%</div>
                    <div style="font-size: 0.8rem; color: #666;">Services performed</div>
                </div>
            """,
                unsafe_allow_html=True,
            )
    else:
        st.warning("No monthly data available for this period.")

    st.divider()

    # --- DAILY VIEW ---
    d_start = selected_date
    d_end = selected_date + timedelta(days=1)

    daily_df = get_route_data(d_start, d_end)

    if daily_df.empty:
        st.warning(f"No daily data for {selected_date.strftime('%Y-%m-%d')}")
    else:
        # Preprocessing for Daily
        agg_day, g_idle_d, g_travel_d, g_service_d, plot_df = (
            calculate_efficiency_stats(
                daily_df,
                selected_date,
                DAY_START_CALC,
                DAY_END_CALC,
                TOTAL_WORK_MINS_PER_DAY,
            )
        )

        st.markdown(f"### Daily Summary - {selected_date.strftime('%Y-%m-%d')}")
        d_col1, d_col2, d_col3 = st.columns(3)

        with d_col1:
            st.markdown(
                f'<div class="card" style="text-align: center; border-left: 5px solid #FF4B4B;"><div style="color: #888; font-size: 0.9rem;">Idle Time</div><div style="font-size: 1.8rem; font-weight: bold; color: #FF4B4B;">{g_idle_d}%</div></div>',
                unsafe_allow_html=True,
            )
        with d_col2:
            st.markdown(
                f'<div class="card" style="text-align: center; border-left: 5px solid #00C9FF;"><div style="color: #888; font-size: 0.9rem;">Travel Time</div><div style="font-size: 1.8rem; font-weight: bold; color: #00C9FF;">{g_travel_d}%</div></div>',
                unsafe_allow_html=True,
            )
        with d_col3:
            st.markdown(
                f'<div class="card" style="text-align: center; border-left: 5px solid #92FE9D;"><div style="color: #888; font-size: 0.9rem;">Productive Time</div><div style="font-size: 1.8rem; font-weight: bold; color: #92FE9D;">{g_service_d}%</div></div>',
                unsafe_allow_html=True,
            )

        # --- UI LAYOUT ---
        st.markdown("### Technician Detail")
        col_main, col_stats = st.columns([2, 1])

        with col_main:
            if not plot_df.empty:
                fig = px.timeline(
                    plot_df,
                    x_start="Start",
                    x_end="End",
                    y="Technician",
                    color="Task",
                    color_discrete_map={"Travel": "#00C9FF", "Service": "#92FE9D"},
                    category_orders={
                        "Technician": sorted(plot_df["Technician"].unique())
                    },
                )
                fig.update_yaxes(autorange="reversed")

                day_start_vis = datetime.combine(
                    selected_date, datetime.min.time()
                ).replace(hour=6)
                day_end_vis = datetime.combine(
                    selected_date, datetime.min.time()
                ).replace(hour=20)

                xaxis_config = dict(gridcolor="rgba(255,255,255,0.1)")
                if enable_clipping:
                    xaxis_config["range"] = [day_start_vis, day_end_vis]

                fig.update_layout(
                    paper_bgcolor="rgba(0,0,0,0)",
                    plot_bgcolor="rgba(0,0,0,0)",
                    font_color="white",
                    xaxis=xaxis_config,
                    yaxis=dict(gridcolor="rgba(255,255,255,0.1)"),
                    height=max(400, len(plot_df["Technician"].unique()) * 40),
                    margin=dict(l=0, r=0, t=30, b=0),
                )
                st.plotly_chart(fig, width="stretch")
            else:
                st.info("No visualizable data.")

        with col_stats:
            st.markdown("#### Optimizable Time per Technician")
            for _, row in agg_day.iterrows():
                total_opt_m = row["Idle Mins"] + row["Travel Mins"]
                st.markdown(
                    f"""
                <div style="margin-bottom: 15px; padding: 10px; background: rgba(255,255,255,0.03); border-radius: 8px;">
                    <div style="font-weight: bold; color: #00C9FF;">{row["Technician"]}</div>
                    <div style="display: flex; justify-content: space-between; font-size: 0.85rem;">
                        <span>Inactivity: {row["Idle Mins"]} min</span><span>{row["Idle %"]}%</span>
                    </div>
                    <div style="display: flex; justify-content: space-between; font-size: 0.85rem;">
                        <span>Travel: {row["Travel Mins"]} min</span><span>{row["Travel %"]}%</span>
                    </div>
                    <div style="display: flex; justify-content: space-between; font-weight: bold; margin-top: 5px; color: #FF4B4B;">
                        <span>Total: {total_opt_m} min</span><span>{row["Opt %"]}%</span>
                    </div>
                    <div style="background: rgba(255,255,255,0.1); height: 6px; border-radius: 3px; display: flex; overflow: hidden; margin-top: 5px;">
                        <div style="background: #FF4B4B; width: {row["Idle %"]}%;"></div>
                        <div style="background: #00C9FF; width: {row["Travel %"]}%;"></div>
                    </div>
                </div>
                """,
                    unsafe_allow_html=True,
                )

        # Row 2: Map
        st.markdown("### Route Map")
        st.markdown('<div class="card">', unsafe_allow_html=True)

        map_data = daily_df.dropna(subset=["Latitude", "Longitude"]).copy()
        if not map_data.empty:
            map_data["Technician"] = map_data.apply(
                lambda r: r["LeadTechnician"] if r["LeadTechnician"] else "Unknown",
                axis=1,
            )

            # Filter map data as well for the 5-min rule
            valid_techs = agg_day["Technician"].tolist()
            map_data = map_data[map_data["Technician"].isin(valid_techs)]

            map_data["Latitude"] = pd.to_numeric(map_data["Latitude"])
            map_data["Longitude"] = pd.to_numeric(map_data["Longitude"])
            map_data = map_data.sort_values(by=["Technician", "ArrivalTimeReal"])

            fig_map = px.line_map(
                map_data,
                lat="Latitude",
                lon="Longitude",
                color="Technician",
                hover_name="CompanyName",
                hover_data={
                    "Latitude": False,
                    "Longitude": False,
                    "Technician": True,
                    "ServiceCode": True,
                    "ArrivalTimeReal": True,
                    "DepartureTimeReal": True,
                },
                zoom=8,
                height=600,
            )

            fig_markers = px.scatter_map(
                map_data,
                lat="Latitude",
                lon="Longitude",
                color="Technician",
                hover_name="CompanyName",
                hover_data={
                    "Latitude": False,
                    "Longitude": False,
                    "Technician": True,
                    "ServiceCode": True,
                    "ArrivalTimeReal": True,
                    "DepartureTimeReal": True,
                },
            )

            for trace in fig_markers.data:
                trace.update(showlegend=False)
                fig_map.add_trace(trace)

            fig_map.update_layout(
                map_style="dark",
                margin={"r": 0, "t": 0, "l": 0, "b": 0},
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                font_color="white",
            )
            st.plotly_chart(fig_map, width="stretch")
        else:
            st.info("No coordinate data found for current day.")
        st.markdown("</div>", unsafe_allow_html=True)

        with st.expander("Show Raw Data"):
            st.dataframe(daily_df, width="stretch")

except Exception as e:
    st.error(f"Error: {e}")
    st.exception(e)
