import streamlit as st
import pandas as pd
import sqlite3
import plotly.express as px
import plotly.graph_objects as go
import io
import json


# ==========================================
# Database Setup & Migration
# ==========================================
def init_db():
    conn = sqlite3.connect('nea_ldc_data.db')
    c = conn.cursor()
    c.execute('''
              CREATE TABLE IF NOT EXISTS peak_loads
              (
                  id
                  INTEGER
                  PRIMARY
                  KEY
                  AUTOINCREMENT,
                  month_year
                  TEXT,
                  peak_day
                  TEXT,
                  peak_time
                  TEXT,
                  peak_load
                  REAL,
                  gen_71
                  REAL,
                  gen_78
                  REAL,
                  gen_92
                  REAL,
                  gen_96
                  REAL,
                  gen_108
                  REAL,
                  gen_114
                  REAL
              )
              ''')

    # Safely add columns for storing plot and new analytical data
    new_columns = [
        ("times_json", "TEXT"), ("total_profile_json", "TEXT"), ("profiles_json", "TEXT"),
        ("monthly_avg_load", "REAL"), ("monthly_load_factor", "REAL"),
        ("daily_metrics_json", "TEXT"), ("monthly_ldc_json", "TEXT")
    ]

    for col_name, col_type in new_columns:
        try:
            c.execute(f"ALTER TABLE peak_loads ADD COLUMN {col_name} {col_type}")
        except sqlite3.OperationalError:
            pass  # Column already exists

    conn.commit()
    conn.close()


def save_to_db(month_year, day, time, load, contributions, times_list, total_profile, profiles_all,
               monthly_avg_load, monthly_load_factor, daily_metrics, monthly_ldc):
    conn = sqlite3.connect('nea_ldc_data.db')
    c = conn.cursor()

    times_str = json.dumps(list(times_list))
    total_str = json.dumps(list(total_profile))
    profiles_str = json.dumps(profiles_all)
    daily_metrics_str = json.dumps(daily_metrics)
    ldc_str = json.dumps(monthly_ldc)

    c.execute('SELECT id FROM peak_loads WHERE month_year=?', (month_year,))
    if c.fetchone() is None:
        c.execute('''
                  INSERT INTO peak_loads (month_year, peak_day, peak_time, peak_load,
                                          gen_71, gen_78, gen_92, gen_96, gen_108, gen_114,
                                          times_json, total_profile_json, profiles_json,
                                          monthly_avg_load, monthly_load_factor, daily_metrics_json, monthly_ldc_json)
                  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                  ''', (month_year, day, time, load,
                        contributions.get('Total IPP', 0), contributions.get('Total NEA SUBSIDIARIES', 0),
                        contributions.get('Total ROR', 0), contributions.get('Total STORAGE', 0),
                        contributions.get('Total IMPORT', 0), contributions.get('Interruption/Tripping', 0),
                        times_str, total_str, profiles_str, monthly_avg_load, monthly_load_factor, daily_metrics_str,
                        ldc_str))
    else:
        c.execute('''
                  UPDATE peak_loads
                  SET times_json=?,
                      total_profile_json=?,
                      profiles_json=?,
                      peak_day=?,
                      peak_time=?,
                      peak_load=?,
                      gen_71=?,
                      gen_78=?,
                      gen_92=?,
                      gen_96=?,
                      gen_108=?,
                      gen_114=?,
                      monthly_avg_load=?,
                      monthly_load_factor=?,
                      daily_metrics_json=?,
                      monthly_ldc_json=?
                  WHERE month_year = ?
                  ''', (times_str, total_str, profiles_str, day, time, load,
                        contributions.get('Total IPP', 0), contributions.get('Total NEA SUBSIDIARIES', 0),
                        contributions.get('Total ROR', 0), contributions.get('Total STORAGE', 0),
                        contributions.get('Total IMPORT', 0), contributions.get('Interruption/Tripping', 0),
                        monthly_avg_load, monthly_load_factor, daily_metrics_str, ldc_str, month_year))
    conn.commit()
    conn.close()


def load_all_peaks():
    conn = sqlite3.connect('nea_ldc_data.db')
    df = pd.read_sql_query("SELECT * FROM peak_loads", conn)
    conn.close()
    return df


# ==========================================
# Data Processing Function
# ==========================================
def process_excel(file):
    xls = pd.read_excel(file, sheet_name=None, header=None, engine='openpyxl')

    monthly_peak = 0
    peak_day, peak_time = None, None
    peak_contributions, peak_hourly_profile, times_list, hourly_profiles_all = {}, [], [], {}

    all_month_loads = []
    daily_metrics = {}

    idx_time = 8

    # ---------------------------------------------------------
    # DYNAMIC SEARCH MAPPING
    # We define search terms to find the exact row regardless of row number
    # ---------------------------------------------------------
    search_mapping = {
        'Total IPP': ['total ipp', 'ipp'],
        'Total NEA SUBSIDIARIES': ['total nea subsidiaries', 'nea subsidiaries', 'nea subsidiary', 'subsidiaries',
                                   'subsidiary'],
        'Total ROR': ['total ror', 'nea ror', 'ror'],
        'Total STORAGE': ['total storage', 'nea storage', 'total storge', 'storge', 'storage'],
        'Total IMPORT': ['total import', 'import'],
        'Interruption/Tripping': ['interruption/tripping', 'interruption', 'tripping']
    }

    for sheet_name, df in xls.items():
        if not str(sheet_name).strip().isdigit():
            continue

        try:
            times = df.iloc[idx_time, 1:32].astype(str).values

            # Combine columns 0 and 1 to search for labels
            col_text = df.iloc[:, 0].astype(str).str.lower() + " " + df.iloc[:, 1].astype(str).str.lower()
            current_day_profiles = {}

            # Dynamically extract data rows based on text matching
            for category, terms in search_mapping.items():
                found = False
                for term in terms:
                    matches = col_text[col_text.str.contains(term, regex=False, na=False)]
                    if not matches.empty:
                        # Grab the first match if searching for specific "Total", otherwise grab the last match to avoid sub-items
                        match_idx = matches.index[0] if 'total' in term else matches.index[-1]

                        row_data = pd.to_numeric(df.iloc[match_idx, 1:32], errors='coerce').fillna(0).values
                        row_data = [v if v > 0 else 0 for v in row_data]  # Ignore exports

                        current_day_profiles[category] = row_data
                        found = True
                        break

                # Failsafe: if the category is totally missing in a specific sheet, fill with zeros
                if not found:
                    current_day_profiles[category] = [0.0] * 31

            # --- DYNAMIC TOTAL LOAD CALCULATION ---
            # Total load is strictly calculated by summing the 6 category rows we extracted above
            daily_total_loads = [0.0] * 31
            for name in search_mapping.keys():
                row_data = current_day_profiles[name]
                daily_total_loads = [t + r for t, r in zip(daily_total_loads, row_data)]

            total_loads = daily_total_loads
            # --------------------------------------

            valid_loads = [l for l in total_loads if l > 0]
            if not valid_loads: continue

            # Calculate daily metrics
            daily_max_load = max(total_loads)
            daily_max_idx = total_loads.index(daily_max_load)

            daily_avg = sum(valid_loads) / len(valid_loads)
            daily_lf = daily_avg / daily_max_load if daily_max_load > 0 else 0

            daily_metrics[sheet_name] = {
                "peak": daily_max_load,
                "avg": daily_avg,
                "lf": daily_lf
            }

            all_month_loads.extend(valid_loads)

            # Find exact properties for peak day
            if daily_max_load > monthly_peak:
                monthly_peak = daily_max_load
                peak_day = sheet_name
                peak_time = times[daily_max_idx]

                peak_hourly_profile = total_loads
                times_list = times.tolist()

                peak_contributions = {}
                hourly_profiles_all = current_day_profiles

                for name in search_mapping.keys():
                    peak_contributions[name] = current_day_profiles[name][daily_max_idx]

        except Exception:
            continue

    monthly_avg_load = sum(all_month_loads) / len(all_month_loads) if all_month_loads else 0
    monthly_load_factor = monthly_avg_load / monthly_peak if monthly_peak > 0 else 0
    monthly_ldc = sorted(all_month_loads, reverse=True)

    return (monthly_peak, peak_day, peak_time, peak_contributions, peak_hourly_profile, times_list, hourly_profiles_all,
            monthly_avg_load, monthly_load_factor, daily_metrics, monthly_ldc)


# ==========================================
# Streamlit Web Interface
# ==========================================
st.set_page_config(page_title="NEA LDC Load Analyzer", layout="wide")
st.title("⚡ NEA LDC Monthly Peak Load Analyzer")

init_db()

if 'selected_month' not in st.session_state:
    st.session_state.selected_month = None

st.sidebar.header("Upload Data")
uploaded_file = st.sidebar.file_uploader("Upload Monthly Logsheet (Excel)", type=['xlsx'])

if uploaded_file is not None:
    filename = uploaded_file.name.replace('.xlsx', '')

    with st.spinner("Analyzing dynamic rows & computing load factors..."):
        p_load, p_day, p_time, p_contrib, p_prof, t_list, prof_all, m_avg, m_lf, d_metrics, m_ldc = process_excel(
            uploaded_file)

    if p_load > 0:
        save_to_db(filename, p_day, p_time, p_load, p_contrib, t_list, p_prof, prof_all, m_avg, m_lf, d_metrics, m_ldc)
        st.session_state.selected_month = filename
    else:
        st.sidebar.error("Could not find relevant load data in this file. Please ensure it follows the correct format.")

# Load DB to get the list of available months
db_df = load_all_peaks()
available_months = db_df['month_year'].tolist() if not db_df.empty else []

# --- FILE ANALYSIS SECTION ---
if available_months:
    st.markdown("### 🔍 Select Analysis Parameters")

    col1, col2 = st.columns(2)
    with col1:
        current_idx = available_months.index(
            st.session_state.selected_month) if st.session_state.selected_month in available_months else 0
        selected_view = st.selectbox("1. Select Month/Year:", available_months, index=current_idx)
        st.session_state.selected_month = selected_view

    with col2:
        analysis_type = st.selectbox("2. Select Analysis Data:", [
            "Peak Load & Generation Contributions",
            "Daily & Monthly Average Load",
            "Daily & Monthly Load Factor",
            "Monthly Load Duration Curve (LDC)"
        ])

    st.divider()

if st.session_state.selected_month in available_months:
    record = db_df[db_df['month_year'] == st.session_state.selected_month].iloc[0]
    st.subheader(f"Results for: {record['month_year']}")

    # -------------------------------------------------------------
    # View 1: Peak Load & Generation Contributions
    # -------------------------------------------------------------
    if analysis_type == "Peak Load & Generation Contributions":
        st.success(
            f"**Monthly Peak Found!** Day: {record['peak_day']} | Time: {record['peak_time']} | Load: {record['peak_load']:.2f} MW")

        if pd.notna(record.get('times_json')) and pd.notna(record.get('profiles_json')):
            try:
                times = json.loads(record['times_json'])
                total_profile = json.loads(record['total_profile_json'])
                profiles = json.loads(record['profiles_json'])

                fig1 = go.Figure()
                for source_name, source_profile in profiles.items():
                    fig1.add_trace(
                        go.Scatter(x=times, y=source_profile, mode='lines', stackgroup='one', name=source_name))
                fig1.add_trace(go.Scatter(x=times, y=total_profile, mode='lines+markers', name='Total Load',
                                          line=dict(color='black', width=3)))

                fig1.update_layout(
                    title=f"Load Profile & Generation Contributions on Peak Day (Day {record['peak_day']})",
                    xaxis_title="Time", yaxis_title="Load (MW)", hovermode="x unified")
                fig1.add_annotation(x=record['peak_time'], y=record['peak_load'], text="Peak", showarrow=True,
                                    arrowhead=1, arrowcolor="red")
                st.plotly_chart(fig1, use_container_width=True)
            except Exception:
                st.warning("⚠️ Graph profile could not load. Re-upload this month's Excel file.")
        else:
            st.warning("⚠️ Graph data missing for this older record. Please re-upload.")

        contrib_data = {
            'Total IPP': record['gen_71'], 'Total NEA SUBSIDIARIES': record['gen_78'], 'Total ROR': record['gen_92'],
            'Total STORAGE': record['gen_96'], 'Total IMPORT': record['gen_108'],
            'Interruption/Tripping': record['gen_114']
        }
        contrib_df = pd.DataFrame(list(contrib_data.items()), columns=['Source', 'Contribution (MW)'])
        fig2 = px.pie(contrib_df, values='Contribution (MW)', names='Source',
                      title=f"Contributions at Peak Time ({record['peak_time']})")
        st.plotly_chart(fig2, use_container_width=True)

    # -------------------------------------------------------------
    # View 2: Daily & Monthly Average Load
    # -------------------------------------------------------------
    elif analysis_type == "Daily & Monthly Average Load":
        if pd.notna(record.get('daily_metrics_json')):
            m_avg = record['monthly_avg_load']
            st.info(f"📊 **Monthly Average Load:** {m_avg:.2f} MW")

            daily_metrics = json.loads(record['daily_metrics_json'])
            days = list(daily_metrics.keys())
            avgs = [daily_metrics[d]['avg'] for d in days]

            fig_avg = px.line(x=days, y=avgs, markers=True,
                              labels={'x': 'Day of Month', 'y': 'Average Load (MW)'},
                              title="Daily Average Load Progression")
            fig_avg.add_hline(y=m_avg, line_dash="dot", line_color="red",
                              annotation_text=f"Monthly Avg: {m_avg:.1f} MW")
            st.plotly_chart(fig_avg, use_container_width=True)
        else:
            st.warning("⚠️ Analytical data missing. Please re-upload this month's Excel file to generate averages.")

    # -------------------------------------------------------------
    # View 3: Daily & Monthly Load Factor
    # -------------------------------------------------------------
    elif analysis_type == "Daily & Monthly Load Factor":
        if pd.notna(record.get('daily_metrics_json')):
            m_lf = record['monthly_load_factor'] * 100
            st.info(f"📈 **Monthly Load Factor:** {m_lf:.2f}%")

            daily_metrics = json.loads(record['daily_metrics_json'])
            days = list(daily_metrics.keys())
            lfs = [daily_metrics[d]['lf'] * 100 for d in days]

            fig_lf = px.line(x=days, y=lfs, markers=True,
                             labels={'x': 'Day of Month', 'y': 'Load Factor (%)'},
                             title="Daily Load Factor Over the Month")
            fig_lf.update_yaxes(range=[0, 100])
            fig_lf.add_hline(y=m_lf, line_dash="dot", line_color="green", annotation_text=f"Monthly LF: {m_lf:.1f}%")
            st.plotly_chart(fig_lf, use_container_width=True)
        else:
            st.warning("⚠️ Analytical data missing. Please re-upload this month's Excel file to generate load factors.")

    # -------------------------------------------------------------
    # View 4: Monthly Load Duration Curve (LDC)
    # -------------------------------------------------------------
    elif analysis_type == "Monthly Load Duration Curve (LDC)":
        if pd.notna(record.get('monthly_ldc_json')):
            ldc_data = json.loads(record['monthly_ldc_json'])
            x_percent = [(i / len(ldc_data)) * 100 for i in range(len(ldc_data))]

            fig_ldc = go.Figure()
            fig_ldc.add_trace(
                go.Scatter(x=x_percent, y=ldc_data, mode='lines', fill='tozeroy', name='Load Duration Curve'))

            fig_ldc.update_layout(title="Monthly Load Duration Curve (LDC)",
                                  xaxis_title="Percentage of Time Exceeded (%)",
                                  yaxis_title="Load (MW)",
                                  hovermode="x unified")
            fig_ldc.update_xaxes(range=[0, 100])
            st.plotly_chart(fig_ldc, use_container_width=True)
        else:
            st.warning(
                "⚠️ Analytical data missing. Please re-upload this month's Excel file to generate the Load Duration Curve.")

st.divider()

# ==========================================
# Historical Database Dropdown Config
# ==========================================
st.subheader("📊 Historical Database & Overall Analytics")

if not db_df.empty:

    st.markdown("### 📈 Historical Monthly Peak Load Record")
    fig_hist_peak = px.line(db_df, x='month_year', y='peak_load', markers=True, text='peak_load',
                            title="Progression of Peak Loads Across Months",
                            labels={'month_year': 'Month/Year', 'peak_load': 'Peak Load (MW)'})
    fig_hist_peak.update_traces(textposition='top center', texttemplate='%{text:.1f}')
    st.plotly_chart(fig_hist_peak, use_container_width=True)

    st.divider()

    # Highest overall peak logic
    highest_record = db_df.loc[db_df['peak_load'].idxmax()]
    st.markdown("### 🏆 Highest Peak Recorded Across All Months")
    st.info(
        f"**Month/Year:** {highest_record['month_year']} | **Day:** {highest_record['peak_day']} | **Time:** {highest_record['peak_time']} | **Load:** {highest_record['peak_load']:.2f} MW")

    st.markdown("### 🗄️ Database Records")
    cols_to_drop = [col for col in
                    ['times_json', 'total_profile_json', 'profiles_json', 'daily_metrics_json', 'monthly_ldc_json'] if
                    col in db_df.columns]
    display_df = db_df.drop(columns=cols_to_drop)

    display_df = display_df.rename(columns={
        'gen_71': 'Total IPP', 'gen_78': 'Total NEA SUBSIDIARIES', 'gen_92': 'Total ROR',
        'gen_96': 'Total STORAGE', 'gen_108': 'Total IMPORT', 'gen_114': 'Interruption/Tripping',
        'monthly_avg_load': 'Monthly Avg (MW)', 'monthly_load_factor': 'Monthly LF'
    })

    st.dataframe(display_df, use_container_width=True)

    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
        display_df.to_excel(writer, index=False)
    st.download_button(label="📥 Download Database as Excel", data=buffer.getvalue(),
                       file_name="nea_ldc_historical_peaks.xlsx")
else:
    st.write("No historical data found. Please upload an Excel file to begin.")