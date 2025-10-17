import streamlit as st
from snowflake.snowpark.context import get_active_session
import pandas as pd
import altair as alt

# -------------------------------------------------
# Page Configuration
# -------------------------------------------------
st.set_page_config(page_title="Data Center Intelligence Dashboard", layout="wide")

session = get_active_session()

# =================================================
# DATACENTER DASHBOARD
# =================================================
def datacenter_dashboard(df):
    st.subheader("Executive Overview: Power, Cooling, and Efficiency Insights")

    # KPIs
    st.markdown("### Key Performance Indicators")
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Average PUE", f"{df['PUE'].mean():.2f}")
    col2.metric("Average Efficiency", f"{df['EFFICIENCY'].mean():.2%}")
    col3.metric("Total Power (kW)", f"{df['TOTAL_POWER_KW'].sum():,.0f}")
    col4.metric("Total Cooling (kW)", f"{df['COOLING_KW'].sum():,.0f}")
    st.divider()

    # Power vs Cooling Trend
    st.markdown("### Power vs Cooling Trends (Daily)")
    chart_power = (
        alt.Chart(df)
        .mark_line(point=True)
        .encode(
            x="TIME_WINDOW:T",
            y="TOTAL_POWER_KW:Q",
            color="DATACENTER_ID:N",
            tooltip=["DATACENTER_ID", "TIME_WINDOW", "TOTAL_POWER_KW", "COOLING_KW"],
        )
    )
    chart_cooling = (
        alt.Chart(df)
        .mark_line(strokeDash=[3, 3], opacity=0.6)
        .encode(
            x="TIME_WINDOW:T",
            y="COOLING_KW:Q",
            color="DATACENTER_ID:N",
            tooltip=["DATACENTER_ID", "TIME_WINDOW", "COOLING_KW"],
        )
    )
    st.altair_chart(chart_power + chart_cooling, use_container_width=True)

    # PUE Comparison
    st.markdown("### Average PUE by Datacenter")
    chart_pue = (
        alt.Chart(df)
        .mark_bar()
        .encode(
            x="DATACENTER_ID:N",
            y="mean(PUE):Q",
            color="DATACENTER_ID:N",
            tooltip=["DATACENTER_ID", "mean(PUE):Q"],
        )
    )
    st.altair_chart(chart_pue, use_container_width=True)

    # Efficiency Trend
    st.markdown("### Efficiency Trend Over Time")
    chart_eff = (
        alt.Chart(df)
        .mark_area(opacity=0.6)
        .encode(
            x="TIME_WINDOW:T",
            y="EFFICIENCY:Q",
            color="DATACENTER_ID:N",
            tooltip=["DATACENTER_ID", "TIME_WINDOW", "EFFICIENCY"],
        )
        .interactive()
    )
    st.altair_chart(chart_eff, use_container_width=True)


# =================================================
# FACILITY DASHBOARD
# =================================================
def facility_dashboard(df):
    st.subheader("Operational Insights by Facility: Energy and Temperature Overview")

    # Global KPIs (unfiltered)
    st.markdown("### Global Key Performance Indicators (All Facilities)")
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Average Power (kW)", f"{df['TOTAL_POWER_KW'].mean():,.2f}")
    col2.metric("Average Temperature (°C)", f"{df['AVG_TEMP_C'].mean():,.2f}")
    col3.metric("Average PUE", f"{df['PUE'].mean():,.2f}")
    col4.metric("Average Active Racks", f"{df['RACKS_ACTIVE'].mean():,.0f}")
    st.divider()

    # Power vs Temperature
    st.markdown("### Power vs Temperature Correlation (Hourly Trend)")
    chart_power = (
        alt.Chart(df)
        .mark_line(point=True)
        .encode(
            x="TIME_WINDOW:T",
            y="TOTAL_POWER_KW:Q",
            color="FACILITY_ID:N",
            tooltip=["FACILITY_ID", "TIME_WINDOW", "TOTAL_POWER_KW", "AVG_TEMP_C"],
        )
    )
    chart_temp = (
        alt.Chart(df)
        .mark_line(strokeDash=[5, 3], color="orange", opacity=0.8)
        .encode(
            x="TIME_WINDOW:T",
            y=alt.Y("AVG_TEMP_C:Q", title="Temperature (°C)", axis=alt.Axis(labelColor="orange")),
            tooltip=["FACILITY_ID", "TIME_WINDOW", "AVG_TEMP_C"],
        )
    )
    st.altair_chart(alt.layer(chart_power, chart_temp).resolve_scale(y="independent"), use_container_width=True)

    # Efficiency Comparison
    st.markdown("### Facility Efficiency Comparison")
    chart_eff = (
        alt.Chart(df)
        .mark_bar()
        .encode(
            x="FACILITY_ID:N",
            y="mean(PUE):Q",
            color="FACILITY_ID:N",
            tooltip=["FACILITY_ID", "mean(PUE):Q"],
        )
    )
    st.altair_chart(chart_eff, use_container_width=True)

    # Table
    st.markdown("### Facility Summary Table Preview")
    st.dataframe(df.sort_values("TIME_WINDOW", ascending=False).head(50), use_container_width=True)

# =================================================
# MAIN APP WITH FILTERS IN TABS
# =================================================
tab1, tab2, tab3 = st.tabs([
    "Datacenter Efficiency",
    "Facility Operations",
    "Rack Health and Anomalies",
])

# --- TAB 1: Datacenter Efficiency ---
with tab1:
    with st.expander("Filters", expanded=True):
        query_dc = "SELECT * FROM GROUP_5.GOLD.DATACENTER_EFFICIENCY ORDER BY TIME_WINDOW"
        df_dc = session.sql(query_dc).to_pandas()
        df_dc["TIME_WINDOW"] = pd.to_datetime(df_dc["TIME_WINDOW"]).dt.tz_localize(None)

        datacenters = df_dc["DATACENTER_ID"].unique().tolist()
        selected_dc = st.multiselect("Select Datacenter(s)", datacenters, default=datacenters, key="dc_select")
        date_range = st.date_input(
            "Select Date Range",
            [df_dc["TIME_WINDOW"].min().date(), df_dc["TIME_WINDOW"].max().date()],
            key="dc_date"
        )

        df_dc = df_dc[
            (df_dc["DATACENTER_ID"].isin(selected_dc))
            & (df_dc["TIME_WINDOW"].between(pd.to_datetime(date_range[0]), pd.to_datetime(date_range[1])))
        ]

    datacenter_dashboard(df_dc)

# --- TAB 2: Facility Operations ---
with tab2:
    st.title("Facility Operations Dashboard")
    st.markdown("Operational Insights by Facility: Energy and Temperature Overview")

    # =================================================
    # 1. Filters
    # =================================================
    with st.expander("Filters", expanded=True):
        query_fac = """
            SELECT 
                f.FACILITY_ID,
                f.TIME_WINDOW,
                f.TOTAL_POWER_KW,
                f.AVG_TEMP_C,
                f.RACKS_ACTIVE,
                f.PUE,
                d.DATACENTER_ID
            FROM GROUP_5.GOLD.FACILITY_SUMMARY f
            JOIN GROUP_5.GOLD.DIM_FACILITY d
              ON f.FACILITY_ID = d.FACILITY_ID
            ORDER BY f.TIME_WINDOW
        """
        df_fac = session.sql(query_fac).to_pandas()
        df_fac["TIME_WINDOW"] = pd.to_datetime(df_fac["TIME_WINDOW"]).dt.tz_localize(None)

        datacenters = df_fac["DATACENTER_ID"].unique().tolist()
        facilities = df_fac["FACILITY_ID"].unique().tolist()

        filter_type = st.radio("Filter by:", ["Datacenter", "Facility"], index=1, key="fac_filter_type")
        if filter_type == "Datacenter":
            selected_dc = st.selectbox("Select Datacenter", datacenters, key="fac_dc")
            df_fac = df_fac[df_fac["DATACENTER_ID"] == selected_dc]
        else:
            selected_fac = st.multiselect("Select Facility", facilities, default=facilities, key="fac_facility")
            df_fac = df_fac[df_fac["FACILITY_ID"].isin(selected_fac)]

        date_range = st.date_input(
            "Select Date Range",
            [df_fac["TIME_WINDOW"].min().date(), df_fac["TIME_WINDOW"].max().date()],
            key="fac_date"
        )
        df_fac = df_fac[
            df_fac["TIME_WINDOW"].between(pd.to_datetime(date_range[0]), pd.to_datetime(date_range[1]))
        ]

    
    # =================================================
    # 2. Facility Performance Summary (Aggregated per Facility)
    # =================================================
    st.markdown("### Facility Performance Summary (Aggregated per Facility)")

    # Use only the date range filter (ignore facility filter)
    df_summary = df_fac.copy()
    df_summary = df_summary[
        (df_summary["TIME_WINDOW"].between(pd.to_datetime(date_range[0]), pd.to_datetime(date_range[1])))
    ]

    # Group by facility to aggregate metrics
    facility_perf = (
        df_summary.groupby("FACILITY_ID", as_index=False)
        .agg({
            "TOTAL_POWER_KW": "sum",
            "AVG_TEMP_C": "mean",
            "RACKS_ACTIVE": "mean",
            "PUE": "mean"
        })
    )

    # Compute normalized metric
    facility_perf["AVG_POWER_PER_RACK"] = facility_perf["TOTAL_POWER_KW"] / facility_perf["RACKS_ACTIVE"].replace(0, pd.NA)

    # Format and display
    st.dataframe(
        facility_perf.rename(columns={
            "FACILITY_ID": "Facility",
            "TOTAL_POWER_KW": "Total Power (kW)",
            "AVG_TEMP_C": "Avg Temp (°C)",
            "RACKS_ACTIVE": "Avg Active Racks",
            "PUE": "Avg PUE",
            "AVG_POWER_PER_RACK": "Avg Power per Rack (kW)"
        }).style.format({
            "Total Power (kW)": "{:.2f}",
            "Avg Temp (°C)": "{:.2f}",
            "Avg Active Racks": "{:.0f}",
            "Avg PUE": "{:.2f}",
            "Avg Power per Rack (kW)": "{:.2f}"
        }),
        use_container_width=True
    )

    st.markdown("""
    **Interpretation:**  
    - This summary shows aggregated performance across all facilities for the selected date range.  
    - It allows comparing energy usage, temperature, and efficiency per facility.  
    - Facility-level filters below will refine detailed charts but do not affect this summary table.
    """)

    st.divider()

   # =================================================
    # 3. Average Power per Rack vs Temperature 
    # =================================================
    st.markdown("### Average Power per Rack vs Temperature (Hourly Trend)")
    
    # Calculate normalized power per rack to remove facility size bias
    df_fac["AVG_POWER_PER_RACK"] = df_fac["TOTAL_POWER_KW"] / df_fac["RACKS_ACTIVE"].replace(0, pd.NA)
    
    # Define consistent color scale
    color_scale = alt.Scale(
        domain=["F01", "F02", "F03"],
        range=["#1f77b4", "#ff7f0e", "#2ca02c"]
    )
    
    # Hide legends when only one facility is selected
    show_legend = len(df_fac["FACILITY_ID"].unique()) > 1
    legend_power = alt.Legend(title="Facility (Power per Rack)", orient="bottom") if show_legend else None
    legend_temp = alt.Legend(title="Facility (Temperature)", orient="bottom") if show_legend else None
    
    # --- Power per Rack Chart (solid line) ---
    power_chart = (
        alt.Chart(df_fac)
        .mark_line(point=True, strokeWidth=2)
        .encode(
            x=alt.X(
            "TIME_WINDOW:T",
            title="Time",
            axis=alt.Axis(format="%b %d")
        ),
            y=alt.Y(
                "AVG_POWER_PER_RACK:Q",
                title="Average Power per Rack (kW)",
                scale=alt.Scale(domain=[df_fac["AVG_POWER_PER_RACK"].min() * 0.9,
                                        df_fac["AVG_POWER_PER_RACK"].max() * 1.05]),
            ),
            color=alt.Color("FACILITY_ID:N", scale=color_scale, legend=legend_power),
            tooltip=[
                alt.Tooltip("FACILITY_ID", title="Facility"),
                alt.Tooltip("TIME_WINDOW:T", title="Timestamp"),
                alt.Tooltip("AVG_POWER_PER_RACK:Q", title="Avg Power per Rack (kW)", format=".2f"),
                alt.Tooltip("AVG_TEMP_C:Q", title="Temperature (°C)", format=".2f"),
            ],
        )
    )
    
    # --- Temperature Chart (dashed orange line) ---
    temp_chart = (
        alt.Chart(df_fac)
        .mark_line(strokeDash=[5, 3], opacity=0.8)
        .encode(
            x=alt.X(
            "TIME_WINDOW:T",
            title="Time",
            axis=alt.Axis(format="%b %d")
        ),
            y=alt.Y(
                "AVG_TEMP_C:Q",
                title="Temperature (°C)",
                scale=alt.Scale(domain=[df_fac["AVG_TEMP_C"].min() - 1,
                                        df_fac["AVG_TEMP_C"].max() + 1]),
                axis=alt.Axis(labelColor="orange"),
            ),
            color=alt.Color("FACILITY_ID:N", scale=color_scale, legend=legend_temp),
            tooltip=[
                alt.Tooltip("FACILITY_ID", title="Facility"),
                alt.Tooltip("TIME_WINDOW:T", title="Timestamp"),
                alt.Tooltip("AVG_TEMP_C:Q", title="Temperature (°C)", format=".2f"),
            ],
        )
    )
    
    # Combine both charts
    combined_chart = (
        alt.layer(power_chart, temp_chart)
        .resolve_scale(y="independent")
        .configure_legend(
            orient="bottom",
            labelFontSize=12,
            titleFontSize=10
        )
        .properties(height=400)
    )
    
    st.altair_chart(combined_chart, use_container_width=True)
    
    st.markdown("""
    **Interpretation:**  
    - Power is now normalized per active rack to remove bias from facility size.  
    - Dynamic scaling ensures temperature and power axes are visually balanced.  
    - Increases in both metrics may indicate reduced cooling efficiency, while stable temperature despite higher power shows strong environmental control.
    """)
    


    st.divider()

    # =================================================
    # 4. Efficiency Comparison
    # =================================================
    st.markdown("### Facility Efficiency Comparison")

    # Compute average PUE per facility
    pue_summary = (
        df_fac.groupby("FACILITY_ID", as_index=False)
        .agg(avg_pue=("PUE", "mean"))
        .sort_values("avg_pue", ascending=True)
    )

    target_pue = 1.5

    # Bar chart: sorted, with target line
    chart_eff = (
        alt.Chart(pue_summary)
        .mark_bar()
        .encode(
            x=alt.X("FACILITY_ID:N", sort=pue_summary["FACILITY_ID"].tolist(), title="Facility"),
            y=alt.Y("avg_pue:Q", title="Average PUE"),
            color=alt.Color(
                "FACILITY_ID:N",
                scale=alt.Scale(domain=["F01", "F02", "F03"], range=["#1f77b4", "#ff7f0e", "#2ca02c"]),
                legend=alt.Legend(title="Facility"),
            ),
            tooltip=[
                alt.Tooltip("FACILITY_ID", title="Facility"),
                alt.Tooltip("avg_pue:Q", title="Average PUE", format=".2f"),
            ],
        )
    )

    rule = (
        alt.Chart(pd.DataFrame({"target": [target_pue]}))
        .mark_rule(color="red", strokeDash=[4, 4])
        .encode(y="target:Q")
    )

    st.altair_chart(chart_eff + rule, use_container_width=True)
    st.markdown(f"**Target PUE Benchmark:** {target_pue} (Lower is better)")
    st.divider()

    # -------------------------------------------------
    # Facility Efficiency Rating (based on current data)
    # -------------------------------------------------
    def classify_efficiency(pue):
        if pue < 1.3:
            return "Excellent"
        elif pue < 1.6:
            return "Good"
        elif pue < 2.0:
            return "Moderate"
        else:
            return "Poor"

    pue_summary["Efficiency Rating"] = pue_summary["avg_pue"].apply(classify_efficiency)

    st.markdown("### Facility Efficiency Rating Table (Based on Current Data)")
    st.dataframe(
        pue_summary.rename(columns={"FACILITY_ID": "Facility", "avg_pue": "Average PUE"})
        .reset_index(drop=True)
        .style.format({"Average PUE": "{:.2f}"}),
        use_container_width=True,
    )

    st.divider()

    # -------------------------------------------------
    # PUE Efficiency Standards Reference
    # -------------------------------------------------
    st.markdown("### PUE Efficiency Reference Table")
    pue_reference = pd.DataFrame({
        "PUE Range": ["< 1.3", "1.3 – 1.6", "1.6 – 2.0", "more than 2.0"],
        "Efficiency Level": ["Excellent", "Good", "Moderate", "Poor"],
        "Description": [
            "State-of-the-art efficiency; minimal cooling overhead",
            "Optimized operations with moderate overhead",
            "Acceptable but with room for airflow and cooling improvement",
            "High overhead; significant efficiency loss, requires optimization"
        ]
    })
    st.table(pue_reference)

    st.markdown("""
    **Interpretation:**  
    - Facilities below the red line (PUE < 1.5) are considered efficient.  
    - The reference table provides industry-standard context for interpreting PUE values.  
    - Comparing the chart with the rating table helps prioritize efficiency improvement efforts.
    """)



    st.divider()

    # =================================================
    # 5. Facility Summary Table
    # =================================================
    st.markdown("### Facility Summary Table Preview")
    st.dataframe(df_fac.sort_values("TIME_WINDOW", ascending=False).head(50), use_container_width=True)


# --- TAB 3: Rack Health and Anomalies ---
# --- TAB 3: Rack Health and Anomalies ---
with tab3:
    st.subheader("Rack Health and Anomaly Detection")

    # -------------------------------------------------
    # Load Data
    # -------------------------------------------------
    query_rack = "SELECT * FROM GROUP_5.GOLD.RACK_PERFORMANCE ORDER BY TIME_WINDOW"
    df_rack = session.sql(query_rack).to_pandas()
    df_rack["TIME_WINDOW"] = pd.to_datetime(df_rack["TIME_WINDOW"]).dt.tz_localize(None)

    # -------------------------------------------------
    # 1. Topmost Filter - Date Range
    # -------------------------------------------------
    with st.expander("Date Range Filter", expanded=True):
        date_range = st.date_input(
            "Select Date Range",
            [df_rack["TIME_WINDOW"].min().date(), df_rack["TIME_WINDOW"].max().date()],
            key="rack_date_global"
        )

    df_rack_filtered = df_rack[
        df_rack["TIME_WINDOW"].between(pd.to_datetime(date_range[0]), pd.to_datetime(date_range[1]))
    ]

    # -------------------------------------------------
    # 2. Anomaly Alerts
    # -------------------------------------------------
    st.markdown("### Anomaly Alerts (Temperature > 30°C or Efficiency < 0.62)")
    alerts = df_rack_filtered[
        (df_rack_filtered["AVG_TEMP_C"] > 30) | (df_rack_filtered["EFFICIENCY"] < 0.62)
    ][["FACILITY_ID", "RACK_ID", "TIME_WINDOW", "AVG_TEMP_C", "EFFICIENCY", "PUE"]]

    if len(alerts) > 0:
        st.dataframe(alerts.sort_values("TIME_WINDOW", ascending=False), use_container_width=True)
    else:
        st.info("No anomalies detected for the selected date range.")

    st.divider()

    # -------------------------------------------------
    # 3. KPI Summary Table by Facility
    # -------------------------------------------------
    st.markdown("### Facility Performance Summary (Aggregated KPIs)")
    kpi_summary = (
        df_rack_filtered.groupby("FACILITY_ID")
        .agg(
            avg_temp=("AVG_TEMP_C", "mean"),
            avg_power=("AVG_POWER_KW", "mean"),
            avg_eff=("EFFICIENCY", "mean"),
            avg_pue=("PUE", "mean")
        )
        .reset_index()
    )
    kpi_summary.columns = ["Facility ID", "Avg Temp (°C)", "Avg Power (kW)", "Avg Efficiency", "Avg PUE"]

    st.dataframe(kpi_summary.round(3), use_container_width=True)
    st.divider()

    # -------------------------------------------------
    # 4. Facility Filter (for remaining visualizations)
    # -------------------------------------------------
    with st.expander("Facility Filter", expanded=True):
        facilities = df_rack_filtered["FACILITY_ID"].unique().tolist()
        selected_facilities = st.multiselect(
            "Select Facility", facilities, default=facilities, key="rack_fac_select_filtered"
        )

    df_fac_filtered = df_rack_filtered[df_rack_filtered["FACILITY_ID"].isin(selected_facilities)]
    st.divider()

    # -------------------------------------------------
    # 5. Charts & Analysis
    # -------------------------------------------------
    st.markdown("### Temperature Trend by Rack")
    
    # --- Dynamic temperature scaling ---
    temp_min = float(df_fac_filtered["AVG_TEMP_C"].min()) if len(df_fac_filtered) else 0.0
    temp_max = float(df_fac_filtered["AVG_TEMP_C"].max()) if len(df_fac_filtered) else 1.0
    
    chart_temp = (
        alt.Chart(df_fac_filtered)
        .mark_line(point=True)
        .encode(
            x=alt.X(
            "TIME_WINDOW:T",
            title="Time",
            axis=alt.Axis(format="%b %d")
        ),
            y=alt.Y(
                "AVG_TEMP_C:Q",
                title="Temperature (°C)",
                scale=alt.Scale(domain=[temp_min - 1, temp_max + 1]),
                axis=alt.Axis(labelColor="orange")
            ),
            color=alt.Color("RACK_ID:N", legend=alt.Legend(title="Rack")),
            tooltip=[
                alt.Tooltip("FACILITY_ID", title="Facility"),
                alt.Tooltip("RACK_ID", title="Rack"),
                alt.Tooltip("TIME_WINDOW:T", title="Timestamp"),
                alt.Tooltip("AVG_TEMP_C:Q", title="Temperature (°C)", format=".2f"),
            ],
        )
    )
    
    st.altair_chart(chart_temp, use_container_width=True)
    
    st.markdown("""
    **Interpretation:**  
    The dynamic axis ensures temperature changes between racks are visible.  
    If one rack shows consistently higher temperatures, it could indicate airflow obstruction or cooling imbalance.
    """)
    
    st.divider()
        
    # -------------------------------------------------
    # Efficiency Trend per Rack 
    # -------------------------------------------------
    st.markdown("### Efficiency Trend by Rack")
    
    # Dynamic scaling for efficiency axis
    eff_min = float(df_fac_filtered["EFFICIENCY"].min()) if len(df_fac_filtered) else 0.0
    eff_max = float(df_fac_filtered["EFFICIENCY"].max()) if len(df_fac_filtered) else 1.0
    
    chart_eff_trend = (
        alt.Chart(df_fac_filtered)
        .mark_line(point=True, strokeWidth=2)
        .encode(
            x=alt.X(
            "TIME_WINDOW:T",
            title="Time",
            axis=alt.Axis(format="%b %d")
        ),
            y=alt.Y(
                "EFFICIENCY:Q",
                title="Efficiency",
                scale=alt.Scale(domain=[eff_min - 0.05, eff_max + 0.05]),
            ),
            color=alt.Color("RACK_ID:N", legend=alt.Legend(title="Rack")),
            tooltip=[
                alt.Tooltip("FACILITY_ID", title="Facility"),
                alt.Tooltip("RACK_ID", title="Rack"),
                alt.Tooltip("TIME_WINDOW:T", title="Timestamp"),
                alt.Tooltip("EFFICIENCY:Q", title="Efficiency", format=".2f"),
            ],
        )
        .properties(height=400)
        .interactive()
    )
    
    st.altair_chart(chart_eff_trend, use_container_width=True)
    
    st.markdown("""
    **Interpretation:**  
    This dynamically scaled efficiency trend chart highlights subtle performance variations between racks.  
    A consistent downward slope or sudden drop in efficiency indicates localized energy imbalance,  
    possible hardware degradation, or cooling system inefficiency for that rack.
    """)








