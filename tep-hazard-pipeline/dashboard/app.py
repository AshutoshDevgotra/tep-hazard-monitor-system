"""
Streamlit Dashboard — TEP Chemical Hazard Monitor.

Provides real-time monitoring of the TEP pipeline with:
    - Project objective and context
    - ALARM SYSTEM: Red/Yellow/Green alerts for detected hazards
    - KPI metric cards (samples, faults, anomalies)
    - Fault type distribution bar chart
    - Reactor sensor time-series with anomaly overlay
    - Raw data table with anomaly highlighting

Reads from PostgreSQL via DB_URL in .env.
Auto-refreshes every 30 seconds.
"""

import os
import sys
import time
import logging
from datetime import datetime

import base64

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("dashboard")

# ---------------------------------------------------------------------------
# Load environment
# ---------------------------------------------------------------------------
ENV_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".env")
load_dotenv(dotenv_path=ENV_PATH)

DB_URL = os.getenv("DB_URL", "postgresql://tep_user:tep_pass@localhost/tep_warehouse")

# ---------------------------------------------------------------------------
# Alert thresholds
# ---------------------------------------------------------------------------
ALERT_CRITICAL_PCT = 1.0    # anomaly_pct > 1.0% → CRITICAL (red)
ALERT_WARNING_PCT = 0.7     # anomaly_pct > 0.7% → WARNING (yellow)
# Below 0.7% → NORMAL (green) — normal TEP data peaks at ~0.57%

# ---------------------------------------------------------------------------
# Alarm sound files
# ---------------------------------------------------------------------------
ALARMS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "alarms")
ALARM_SOUNDS = {
    "critical": os.path.join(ALARMS_DIR, "freesound_community-loud-emergency-alarm-54635.mp3"),
    "warning": os.path.join(ALARMS_DIR, "freesound_community-severe-warning-alarm-98704.mp3"),
    "inject_thermal": os.path.join(ALARMS_DIR, "twisted-colossus-fire-alarm-414915.mp3"),
    "inject_general": os.path.join(ALARMS_DIR, "jeremayjimenez-taiwan-eas-alarm-501825.mp3"),
}

# Fault type descriptions with sensor source and parameter info
# Each entry: { description, sensor, parameter, severity }
FAULT_INFO = {
    0:  {"desc": "Normal Operation — No fault",                                "sensor": "—",                "param": "—",             "severity": "safe"},
    1:  {"desc": "A/C Feed Ratio, B Composition Constant (Step)",              "sensor": "xmeas_1 (A Feed)", "param": "Flow Rate",     "severity": "critical"},
    2:  {"desc": "B Composition, A/C Ratio Constant (Step)",                   "sensor": "xmeas_1 (A Feed)", "param": "Composition",   "severity": "critical"},
    3:  {"desc": "D Feed Temperature (Step)",                                  "sensor": "xmeas_2 (D Feed)", "param": "Temperature",   "severity": "warning"},
    4:  {"desc": "Reactor Cooling Water Inlet Temperature (Step)",              "sensor": "xmeas_9 (Reactor Temp)",  "param": "Temperature",   "severity": "critical"},
    5:  {"desc": "Condenser Cooling Water Inlet Temperature (Step)",            "sensor": "xmeas_11 (Sep Temp)",     "param": "Temperature",   "severity": "warning"},
    6:  {"desc": "A Feed Loss (Step)",                                         "sensor": "xmeas_1 (A Feed)", "param": "Flow Rate",     "severity": "critical"},
    7:  {"desc": "C Header Pressure Loss (Step)",                              "sensor": "xmeas_7 (Reactor Press)", "param": "Pressure",     "severity": "critical"},
    8:  {"desc": "A, B, C Feed Composition (Random)",                          "sensor": "xmeas_1, 2, 4",   "param": "Composition",   "severity": "critical"},
    9:  {"desc": "D Feed Temperature (Random)",                                "sensor": "xmeas_2 (D Feed)", "param": "Temperature",   "severity": "warning"},
    10: {"desc": "C Feed Temperature (Random)",                                "sensor": "xmeas_4 (A+C Feed)","param": "Temperature",   "severity": "warning"},
    11: {"desc": "Reactor Cooling Water Inlet Temperature (Random)",            "sensor": "xmeas_9 (Reactor Temp)",  "param": "Temperature",   "severity": "critical"},
    12: {"desc": "Condenser Cooling Water Inlet Temperature (Random)",          "sensor": "xmeas_11 (Sep Temp)",     "param": "Temperature",   "severity": "warning"},
    13: {"desc": "Reaction Kinetics — Slow Drift",                             "sensor": "xmeas_9, 7 (Temp+Press)","param": "Kinetics",      "severity": "warning"},
    14: {"desc": "Reactor Cooling Water Valve — Sticking",                      "sensor": "xmv_10 (CW Valve)","param": "Valve Position", "severity": "critical"},
    15: {"desc": "Condenser Cooling Water Valve — Sticking",                    "sensor": "xmv_11 (CW Valve)","param": "Valve Position", "severity": "warning"},
    16: {"desc": "Unknown Fault 16 — Multiple Variable Interaction",            "sensor": "xmeas_7, 8, 9",   "param": "Multi-Sensor",   "severity": "critical"},
    17: {"desc": "Unknown Fault 17 — Multiple Variable Interaction",            "sensor": "xmeas_7, 8, 9",   "param": "Multi-Sensor",   "severity": "warning"},
    18: {"desc": "Unknown Fault 18 — Multiple Variable Interaction",            "sensor": "xmeas_7, 8, 9",   "param": "Multi-Sensor",   "severity": "warning"},
    19: {"desc": "Unknown Fault 19 — Multiple Variable Interaction",            "sensor": "xmeas_7, 8, 9",   "param": "Multi-Sensor",   "severity": "critical"},
    20: {"desc": "Unknown Fault 20 — Multiple Variable Interaction",            "sensor": "xmeas_7, 8, 9",   "param": "Multi-Sensor",   "severity": "critical"},
}

# Legacy dict for backward compat (used in time-series section)
FAULT_DESCRIPTIONS = {k: v["desc"] for k, v in FAULT_INFO.items() if k > 0}

# Parameter type emoji mapping
PARAM_EMOJI = {
    "Temperature": "🌡️",
    "Pressure": "💨",
    "Flow Rate": "🌊",
    "Composition": "🧪",
    "Valve Position": "🔧",
    "Kinetics": "⚗️",
    "Multi-Sensor": "📡",
    "—": "✅",
}

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="TEP Hazard Monitor",
    page_icon="🧪",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# Custom CSS
# ---------------------------------------------------------------------------
st.markdown(
    """
    <style>
    .main {background-color: #0e1117;}
    .metric-card {
        background: linear-gradient(135deg, #1e293b 0%, #334155 100%);
        border-radius: 12px;
        padding: 20px;
        text-align: center;
        border: 1px solid #475569;
    }
    .metric-value {
        font-size: 2.2rem;
        font-weight: 700;
        color: #38bdf8;
    }
    .metric-label {
        font-size: 0.9rem;
        color: #94a3b8;
        margin-top: 4px;
    }
    .stAlert {border-radius: 8px;}

    /* Alarm panel styles */
    .alarm-critical {
        background: linear-gradient(135deg, #7f1d1d 0%, #991b1b 100%);
        border: 2px solid #ef4444;
        border-radius: 12px;
        padding: 16px 20px;
        margin-bottom: 8px;
        animation: pulse-red 1.5s infinite;
    }
    .alarm-warning {
        background: linear-gradient(135deg, #78350f 0%, #92400e 100%);
        border: 2px solid #f59e0b;
        border-radius: 12px;
        padding: 16px 20px;
        margin-bottom: 8px;
        animation: pulse-yellow 2s infinite;
    }
    .alarm-safe {
        background: linear-gradient(135deg, #14532d 0%, #166534 100%);
        border: 2px solid #22c55e;
        border-radius: 12px;
        padding: 16px 20px;
        margin-bottom: 8px;
    }
    .alarm-title {
        font-size: 1.1rem;
        font-weight: 700;
        margin-bottom: 4px;
    }
    .alarm-detail {
        font-size: 0.85rem;
        color: #e2e8f0;
    }
    @keyframes pulse-red {
        0%, 100% { box-shadow: 0 0 8px rgba(239,68,68,0.3); }
        50% { box-shadow: 0 0 30px rgba(239,68,68,0.8), 0 0 60px rgba(239,68,68,0.4); }
    }
    @keyframes pulse-yellow {
        0%, 100% { box-shadow: 0 0 5px rgba(245,158,11,0.3); }
        50% { box-shadow: 0 0 20px rgba(245,158,11,0.6); }
    }

    /* Emergency screen flash overlay */
    .emergency-flash {
        position: fixed;
        top: 0; left: 0; right: 0; bottom: 0;
        pointer-events: none;
        z-index: 9999;
        animation: emergency-screen-flash 3s ease-out forwards;
    }
    @keyframes emergency-screen-flash {
        0% { background: rgba(239, 68, 68, 0.4); }
        15% { background: rgba(239, 68, 68, 0.0); }
        30% { background: rgba(239, 68, 68, 0.3); }
        45% { background: rgba(239, 68, 68, 0.0); }
        60% { background: rgba(239, 68, 68, 0.2); }
        75% { background: rgba(239, 68, 68, 0.0); }
        100% { background: rgba(239, 68, 68, 0.0); }
    }

    /* Siren bar animation */
    .siren-bar {
        height: 6px;
        background: linear-gradient(90deg, #ef4444, #f59e0b, #ef4444, #f59e0b, #ef4444);
        background-size: 200% 100%;
        animation: siren-sweep 1s linear infinite;
        border-radius: 3px;
        margin-bottom: 12px;
    }
    @keyframes siren-sweep {
        0% { background-position: 0% 50%; }
        100% { background-position: 200% 50%; }
    }

    /* Emergency banner */
    .emergency-banner {
        background: linear-gradient(135deg, #7f1d1d 0%, #450a0a 100%);
        border: 3px solid #ef4444;
        border-radius: 16px;
        padding: 24px;
        margin-bottom: 16px;
        text-align: center;
        animation: pulse-red 1.5s infinite;
    }
    .emergency-banner h2 {
        color: #fca5a5;
        margin: 0 0 8px 0;
        font-size: 1.8rem;
        letter-spacing: 4px;
    }
    .emergency-banner p {
        color: #fecaca;
        font-size: 1.05rem;
        margin: 0;
    }
    </style>
    """,
    unsafe_allow_html=True,
)


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------
@st.cache_resource
def get_engine():
    """Create a cached SQLAlchemy engine."""
    try:
        engine = create_engine(DB_URL)
        return engine
    except Exception as exc:
        logger.error("Database connection failed: %s", exc)
        return None


def test_connection(engine):
    """Test if the DB connection is alive."""
    if engine is None:
        return False
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return True
    except Exception:
        return False


@st.cache_data(ttl=30)
def load_fault_summary():
    """Load fault_summary from marts schema."""
    engine = get_engine()
    if engine is None:
        return pd.DataFrame()
    try:
        return pd.read_sql("SELECT * FROM marts.fault_summary ORDER BY fault_label", engine)
    except Exception as exc:
        logger.error("Failed to load fault_summary: %s", exc)
        return pd.DataFrame()


@st.cache_data(ttl=30)
def load_sensor_features(limit=200):
    """Load recent rows from staging.int_sensor_features."""
    engine = get_engine()
    if engine is None:
        return pd.DataFrame()
    try:
        query = (
            "SELECT simulation_run, sample_num, fault_label, reactor_temp, "
            "reactor_pressure, reactor_temp_rolling_mean, reactor_pressure_rolling_mean, "
            "temp_anomaly_flag, pressure_anomaly_flag, combined_anomaly_flag "
            "FROM staging.int_sensor_features "
            "ORDER BY simulation_run DESC, sample_num DESC "
            f"LIMIT {limit}"
        )
        return pd.read_sql(query, engine)
    except Exception as exc:
        logger.error("Failed to load sensor features: %s", exc)
        return pd.DataFrame()


@st.cache_data(ttl=30)
def load_simulation_data(sim_run):
    """Load all samples for a specific simulation run."""
    engine = get_engine()
    if engine is None:
        return pd.DataFrame()
    try:
        query = (
            "SELECT sample_num, fault_label, reactor_temp, reactor_pressure, "
            "temp_anomaly_flag, pressure_anomaly_flag, combined_anomaly_flag "
            "FROM staging.int_sensor_features "
            "WHERE simulation_run = %(sim)s "
            "ORDER BY sample_num"
        )
        return pd.read_sql(query, engine, params={"sim": int(sim_run)})
    except Exception as exc:
        logger.error("Failed to load simulation %s: %s", sim_run, exc)
        return pd.DataFrame()


@st.cache_data(ttl=30)
def get_simulation_runs():
    """Get list of distinct simulation runs."""
    engine = get_engine()
    if engine is None:
        return []
    try:
        df = pd.read_sql(
            "SELECT DISTINCT simulation_run FROM staging.int_sensor_features "
            "ORDER BY simulation_run LIMIT 100",
            engine,
        )
        return df["simulation_run"].tolist()
    except Exception:
        return []


# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------
with st.sidebar:
    st.title("🧪 TEP Hazard Monitor")
    st.markdown("---")

    # Connection status
    engine = get_engine()
    connected = test_connection(engine)
    if connected:
        st.success("🟢 Database Connected")
    else:
        st.error("🔴 Database Disconnected")

    # Last refresh
    st.caption(f"Last refresh: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # Fault label filter
    st.markdown("### Filters")
    fault_options = list(range(0, 21))
    selected_faults = st.multiselect(
        "Fault Labels",
        options=fault_options,
        default=fault_options,
        help="Select fault types to display",
    )

    # Refresh button
    if st.button("🔄 Refresh Now", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

    # Alarm sound toggle
    st.toggle(
        "🔊 Alarm Sound",
        value=st.session_state.get("alarm_sound_enabled", True),
        key="alarm_sound_enabled",
        help="Enable/disable alarm sounds",
    )

    st.markdown("---")

    # ===== LIVE DEMO: Fault Injection =====
    st.markdown("### 🧪 Live Demo: Inject Fault")
    st.caption("Simulate a hazard to trigger alarms")

    # Import the injector
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from fault_injector import FAULT_SCENARIOS, inject_fault, clear_injected_data

    scenario_labels = {k: v["name"] for k, v in FAULT_SCENARIOS.items()}
    selected_scenario = st.selectbox(
        "Select Hazard Scenario",
        options=list(scenario_labels.keys()),
        format_func=lambda x: scenario_labels[x],
    )

    if selected_scenario:
        sc = FAULT_SCENARIOS[selected_scenario]
        st.caption(sc["description"])

        # Show what will change
        st.markdown(
            f"""
            **Normal → Dangerous:**
            - Temp: 120.4°C → **{sc['reactor_temp']}°C**
            - Pressure: 2705 → **{sc['reactor_pressure']} kPa**
            """
        )

    inject_col, clear_col = st.columns(2)
    with inject_col:
        if st.button("⚠️ INJECT", use_container_width=True, type="primary"):
            with st.spinner("Injecting fault..."):
                result = inject_fault(selected_scenario, num_samples=100)
            if result:
                st.success(f"Injected {result['samples_injected']} faulty readings!")
                # Store which scenario was injected so we can play the right alarm
                st.session_state["just_injected"] = selected_scenario
                st.cache_data.clear()
                time.sleep(1)
                st.rerun()
    with clear_col:
        if st.button("🧹 CLEAR", use_container_width=True):
            with st.spinner("Clearing..."):
                clear_injected_data()
            st.success("Demo data cleared!")
            st.session_state.pop("just_injected", None)
            st.cache_data.clear()
            time.sleep(1)
            st.rerun()

    st.markdown("---")

    # Project info
    with st.expander("ℹ️ About This Project"):
        st.markdown(
            """
            **Tennessee Eastman Process (TEP)**
            is a simulation of a real chemical plant
            with 52 sensor variables and 20 fault types.

            This dashboard monitors sensor data in
            real-time and raises alarms when anomalies
            are detected — like an **Early Warning System**
            for chemical plant safety.
            """
        )

    st.caption("TEP Chemical Hazard Detection Pipeline")
    st.caption("Data Engineering Project")

# ---------------------------------------------------------------------------
# Main area
# ---------------------------------------------------------------------------
st.title("⚗️ TEP Chemical Hazard Detection Dashboard")
st.markdown("Real-time monitoring of the Tennessee Eastman Process fault detection pipeline.")

# Load data
df_summary = load_fault_summary()
df_features = load_sensor_features(limit=200)

if not df_summary.empty:
    filtered_summary = df_summary[df_summary["fault_label"].isin(selected_faults)]

    # =========================================================================
    # 🚨 ALARM SYSTEM — Row 0
    # All 20 fault types (1-20) trigger alarms. Only fault 0 is safe.
    # =========================================================================
    st.markdown("---")
    st.subheader("🚨 Hazard Alarm System")

    # Classify each fault type into alarm levels
    # Critical: fault types whose severity is "critical" OR anomaly_pct > threshold
    # Warning: all other non-zero fault types
    # Safe: only fault_label == 0 (normal operation)
    all_faults = df_summary[df_summary["fault_label"] > 0]

    def classify_fault_severity(row):
        fid = int(row["fault_label"])
        info = FAULT_INFO.get(fid, {})
        sev = info.get("severity", "warning")
        if sev == "critical" or row["anomaly_pct"] > ALERT_CRITICAL_PCT:
            return "critical"
        return "warning"

    if not all_faults.empty:
        all_faults = all_faults.copy()
        all_faults["alarm_level"] = all_faults.apply(classify_fault_severity, axis=1)
        critical_faults = all_faults[all_faults["alarm_level"] == "critical"]
        warning_faults = all_faults[all_faults["alarm_level"] == "warning"]
    else:
        critical_faults = pd.DataFrame()
        warning_faults = pd.DataFrame()

    safe_faults = df_summary[df_summary["fault_label"] == 0]

    # Alarm sound toggle in sidebar state
    if "alarm_sound_enabled" not in st.session_state:
        st.session_state.alarm_sound_enabled = True

    def play_alarm_sound_js(sound_key):
        """Play alarm sound using JavaScript autoplay (more reliable than st.audio)."""
        if not st.session_state.get("alarm_sound_enabled", True):
            return
        sound_path = ALARM_SOUNDS.get(sound_key)
        if sound_path and os.path.exists(sound_path):
            with open(sound_path, "rb") as f:
                audio_bytes = f.read()
            audio_b64 = base64.b64encode(audio_bytes).decode()
            st.markdown(
                f"""
                <audio id="alarm-audio-{sound_key}" autoplay>
                    <source src="data:audio/mp3;base64,{audio_b64}" type="audio/mpeg">
                </audio>
                <script>
                    (function() {{
                        var audio = document.getElementById('alarm-audio-{sound_key}');
                        if (audio) {{
                            audio.volume = 0.8;
                            audio.play().catch(function(e) {{ console.log('Autoplay blocked:', e); }});
                        }}
                    }})();
                </script>
                """,
                unsafe_allow_html=True,
            )

    # Determine which alarm sound to play
    # If we just injected a fault, play the scenario-specific alarm
    just_injected = st.session_state.pop("just_injected", None)
    inject_alarm_key = None
    if just_injected:
        if just_injected == "thermal_runaway":
            inject_alarm_key = "inject_thermal"
        else:
            inject_alarm_key = "inject_general"

    # Show alarm status banner
    # ALL fault types 1-20 trigger alarms; only fault 0 = safe
    has_any_faults = len(critical_faults) > 0 or len(warning_faults) > 0

    if len(critical_faults) > 0:
        # Emergency flash + siren bar for critical
        st.markdown('<div class="emergency-flash"></div>', unsafe_allow_html=True)
        st.markdown('<div class="siren-bar"></div>', unsafe_allow_html=True)

        st.markdown(
            f"""
            <div class="emergency-banner">
                <h2>🚨 EMERGENCY ALERT 🚨</h2>
                <p>⚠️ {len(critical_faults)} CRITICAL + {len(warning_faults)} WARNING fault type(s) active</p>
                <p style="margin-top:8px; font-size:0.9rem; color:#fca5a5;">IMMEDIATE ACTION REQUIRED — {len(critical_faults) + len(warning_faults)} of 20 fault types detected in sensor data</p>
            </div>
            """,
            unsafe_allow_html=True,
        )

        st.markdown('<div class="siren-bar"></div>', unsafe_allow_html=True)

        # Play injection-specific sound or fall back to critical alarm
        if inject_alarm_key:
            play_alarm_sound_js(inject_alarm_key)
        else:
            play_alarm_sound_js("critical")

    elif len(warning_faults) > 0:
        st.markdown(
            f"""
            <div class="alarm-warning">
                <div class="alarm-title" style="color: #fde68a;">
                    🟡 WARNING — {len(warning_faults)} fault type(s) detected in sensor data
                </div>
                <div class="alarm-detail">
                    Sensor readings show deviations from normal operation. Monitor closely.
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )
        if inject_alarm_key:
            play_alarm_sound_js(inject_alarm_key)
        else:
            play_alarm_sound_js("warning")
    else:
        # Only shown when ALL data is fault_label == 0 (truly normal operation)
        st.markdown(
            """
            <div class="alarm-safe">
                <div class="alarm-title" style="color: #86efac;">
                    🟢 ALL CLEAR — Normal Operation — No faults detected
                </div>
                <div class="alarm-detail">
                    Only normal (Fault 0) data present. All 52 sensors within safe operating ranges.
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    # Detailed alarm cards in columns (show ALL fault types 1-20)
    if len(critical_faults) > 0 or len(warning_faults) > 0:
        total_alarm_faults = len(critical_faults) + len(warning_faults)
        alarm_cols = st.columns(min(total_alarm_faults, 4))
        alarm_idx = 0

        for _, row in critical_faults.iterrows():
            fault_id = int(row["fault_label"])
            info = FAULT_INFO.get(fault_id, {"desc": f"Fault Type {fault_id}", "sensor": "—", "param": "—"})
            emoji = PARAM_EMOJI.get(info["param"], "⚠️")
            with alarm_cols[alarm_idx % len(alarm_cols)]:
                st.markdown(
                    f"""
                    <div class="alarm-critical">
                        <div class="alarm-title" style="color: #fca5a5;">
                            🔴 FAULT {fault_id}
                        </div>
                        <div class="alarm-detail">
                            <b>Cause:</b> {info['desc']}<br>
                            <b>{emoji} Parameter:</b> {info['param']}<br>
                            <b>📡 Sensor:</b> {info['sensor']}<br>
                            <b>Anomaly Rate:</b> {row['anomaly_pct']:.2f}%<br>
                            <b>Samples:</b> {int(row['total_samples']):,}<br>
                            <b>Action:</b> 🚨 Inspect reactor immediately
                        </div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )
            alarm_idx += 1

        for _, row in warning_faults.iterrows():
            fault_id = int(row["fault_label"])
            info = FAULT_INFO.get(fault_id, {"desc": f"Fault Type {fault_id}", "sensor": "—", "param": "—"})
            emoji = PARAM_EMOJI.get(info["param"], "⚠️")
            with alarm_cols[alarm_idx % len(alarm_cols)]:
                st.markdown(
                    f"""
                    <div class="alarm-warning">
                        <div class="alarm-title" style="color: #fde68a;">
                            🟡 FAULT {fault_id}
                        </div>
                        <div class="alarm-detail">
                            <b>Cause:</b> {info['desc']}<br>
                            <b>{emoji} Parameter:</b> {info['param']}<br>
                            <b>📡 Sensor:</b> {info['sensor']}<br>
                            <b>Anomaly Rate:</b> {row['anomaly_pct']:.2f}%<br>
                            <b>Samples:</b> {int(row['total_samples']):,}<br>
                            <b>Action:</b> ⚠️ Increase monitoring frequency
                        </div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )
            alarm_idx += 1

    # Alarm history log — ALL 20 faults are alarmed, only fault 0 is safe
    with st.expander("📋 Full Alarm Log — All 20 Fault Types + Normal"):
        alarm_log = df_summary.copy()

        def get_alarm_status(row):
            fid = int(row["fault_label"])
            if fid == 0:
                return "🟢 NORMAL"
            info = FAULT_INFO.get(fid, {})
            sev = info.get("severity", "warning")
            if sev == "critical" or row["anomaly_pct"] > ALERT_CRITICAL_PCT:
                return "🔴 CRITICAL"
            return "🟡 WARNING"

        alarm_log["status"] = alarm_log.apply(get_alarm_status, axis=1)
        alarm_log["description"] = alarm_log["fault_label"].map(
            lambda fid: FAULT_INFO.get(int(fid), {}).get("desc", f"Fault {fid}")
        )
        alarm_log["sensor_source"] = alarm_log["fault_label"].map(
            lambda fid: FAULT_INFO.get(int(fid), {}).get("sensor", "—")
        )
        alarm_log["parameter_type"] = alarm_log["fault_label"].map(
            lambda fid: FAULT_INFO.get(int(fid), {}).get("param", "—")
        )
        alarm_log["recommended_action"] = alarm_log.apply(
            lambda row: "No action needed" if int(row["fault_label"]) == 0
            else ("🚨 IMMEDIATE INSPECTION" if row["status"] == "🔴 CRITICAL" else "⚠️ Increase monitoring"),
            axis=1,
        )
        st.dataframe(
            alarm_log[["fault_label", "status", "parameter_type", "sensor_source",
                        "anomaly_pct", "total_samples",
                        "avg_reactor_temp", "avg_reactor_pressure",
                        "description", "recommended_action"]].sort_values(
                "fault_label", ascending=True
            ),
            use_container_width=True,
            height=500,
        )

    st.markdown("---")

    # =========================================================================
    # Row 1 — Metric Cards
    # =========================================================================
    total_samples = int(filtered_summary["total_samples"].sum()) if not filtered_summary.empty else 0
    fault_types = int(filtered_summary[filtered_summary["fault_label"] > 0].shape[0])
    overall_anomaly = round(filtered_summary["anomaly_pct"].mean(), 2) if not filtered_summary.empty else 0.0
    active_alarms = len(critical_faults) + len(warning_faults)

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.markdown(
            f"""
            <div class="metric-card">
                <div class="metric-value">{total_samples:,}</div>
                <div class="metric-label">Total Samples Ingested</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    with col2:
        st.markdown(
            f"""
            <div class="metric-card">
                <div class="metric-value">{fault_types}</div>
                <div class="metric-label">Fault Types Detected</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    with col3:
        color = "#ef4444" if overall_anomaly > ALERT_CRITICAL_PCT else "#38bdf8"
        st.markdown(
            f"""
            <div class="metric-card">
                <div class="metric-value" style="color: {color}">{overall_anomaly}%</div>
                <div class="metric-label">Overall Anomaly Rate</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    with col4:
        alarm_color = "#ef4444" if active_alarms > 0 else "#22c55e"
        st.markdown(
            f"""
            <div class="metric-card">
                <div class="metric-value" style="color: {alarm_color}">{active_alarms}</div>
                <div class="metric-label">Active Alarms</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    st.markdown("<br>", unsafe_allow_html=True)

    # =========================================================================
    # Row 2 — Fault Type Distribution Bar Chart
    # =========================================================================
    st.subheader("📊 Fault Type Distribution")

    if not filtered_summary.empty:
        fig_bar = px.bar(
            filtered_summary,
            x="fault_label",
            y="total_samples",
            color="anomaly_pct",
            color_continuous_scale="Reds",
            title="Fault Type Distribution — Sample Count & Anomaly Rate",
            labels={
                "fault_label": "Fault Label",
                "total_samples": "Total Samples",
                "anomaly_pct": "Anomaly %",
            },
        )
        # Add threshold lines
        fig_bar.add_hline(
            y=None,  # We don't add hline to bar, we annotate instead
        ) if False else None
        fig_bar.update_layout(
            plot_bgcolor="#0e1117",
            paper_bgcolor="#0e1117",
            font_color="#e2e8f0",
            xaxis=dict(dtick=1),
            height=450,
        )
        st.plotly_chart(fig_bar, use_container_width=True)
    else:
        st.info("No data available for selected fault labels.")

    # =========================================================================
    # Row 3 — Time Series: Reactor Temp & Pressure with Anomaly Zones
    # =========================================================================
    st.subheader("📈 Reactor Sensor Time Series")

    sim_runs = get_simulation_runs()
    if sim_runs:
        selected_sim = st.selectbox("Select Simulation Run", sim_runs, index=0)
        df_sim = load_simulation_data(selected_sim)

        if not df_sim.empty:
            # Show fault info for this simulation
            sim_fault = df_sim["fault_label"].mode()
            if len(sim_fault) > 0 and int(sim_fault.iloc[0]) > 0:
                fault_id = int(sim_fault.iloc[0])
                desc = FAULT_DESCRIPTIONS.get(fault_id, "Unknown")
                anomaly_count = int(df_sim["combined_anomaly_flag"].sum())
                anomaly_pct = round(anomaly_count / len(df_sim) * 100, 2)

                if anomaly_pct > ALERT_CRITICAL_PCT:
                    st.markdown(
                        f"""
                        <div class="alarm-critical">
                            <div class="alarm-title" style="color: #fca5a5;">
                                🔴 Fault {fault_id} Detected — {desc}
                            </div>
                            <div class="alarm-detail">
                                {anomaly_count} anomalous samples ({anomaly_pct}%) in this simulation run
                            </div>
                        </div>
                        """,
                        unsafe_allow_html=True,
                    )
                elif anomaly_pct > ALERT_WARNING_PCT:
                    st.markdown(
                        f"""
                        <div class="alarm-warning">
                            <div class="alarm-title" style="color: #fde68a;">
                                🟡 Fault {fault_id} Detected — {desc}
                            </div>
                            <div class="alarm-detail">
                                {anomaly_count} anomalous samples ({anomaly_pct}%) in this simulation run
                            </div>
                        </div>
                        """,
                        unsafe_allow_html=True,
                    )

            fig_ts = make_subplots(specs=[[{"secondary_y": True}]])

            # Reactor temperature
            fig_ts.add_trace(
                go.Scatter(
                    x=df_sim["sample_num"],
                    y=df_sim["reactor_temp"],
                    name="Reactor Temp",
                    line=dict(color="#38bdf8", width=2),
                ),
                secondary_y=False,
            )

            # Reactor pressure (dual axis)
            fig_ts.add_trace(
                go.Scatter(
                    x=df_sim["sample_num"],
                    y=df_sim["reactor_pressure"],
                    name="Reactor Pressure",
                    line=dict(color="#a78bfa", width=2),
                ),
                secondary_y=True,
            )

            # Temperature anomaly flags as red dots
            anomaly_rows = df_sim[df_sim["temp_anomaly_flag"] == 1]
            if not anomaly_rows.empty:
                fig_ts.add_trace(
                    go.Scatter(
                        x=anomaly_rows["sample_num"],
                        y=anomaly_rows["reactor_temp"],
                        name="⚠ Temp Anomaly",
                        mode="markers",
                        marker=dict(color="#ef4444", size=10, symbol="x",
                                    line=dict(width=2, color="#ef4444")),
                    ),
                    secondary_y=False,
                )

            # Pressure anomaly flags as orange triangles
            pressure_anomalies = df_sim[df_sim["pressure_anomaly_flag"] == 1]
            if not pressure_anomalies.empty:
                fig_ts.add_trace(
                    go.Scatter(
                        x=pressure_anomalies["sample_num"],
                        y=pressure_anomalies["reactor_pressure"],
                        name="⚠ Pressure Anomaly",
                        mode="markers",
                        marker=dict(color="#f59e0b", size=10, symbol="triangle-up",
                                    line=dict(width=2, color="#f59e0b")),
                    ),
                    secondary_y=True,
                )

            fig_ts.update_layout(
                title=f"Simulation Run {selected_sim} — Reactor Sensors",
                plot_bgcolor="#0e1117",
                paper_bgcolor="#0e1117",
                font_color="#e2e8f0",
                height=500,
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            )
            fig_ts.update_xaxes(title_text="Sample Number")
            fig_ts.update_yaxes(title_text="Temperature", secondary_y=False)
            fig_ts.update_yaxes(title_text="Pressure", secondary_y=True)

            st.plotly_chart(fig_ts, use_container_width=True)
        else:
            st.info("No data for selected simulation run.")
    else:
        st.info("No simulation runs found in the database.")

    # =========================================================================
    # Row 4 — Raw Data Table with sensor source & parameter info
    # =========================================================================
    st.subheader("🗂️ Recent Sensor Readings")

    if not df_features.empty:
        display_cols = [
            "sample_num", "fault_label", "reactor_temp",
            "reactor_pressure", "combined_anomaly_flag",
        ]
        available_cols = [c for c in display_cols if c in df_features.columns]
        df_display = df_features[available_cols].copy()

        # Add fault name, sensor source, and parameter type columns
        df_display["fault_name"] = df_display["fault_label"].map(
            lambda fid: FAULT_INFO.get(int(fid), {}).get("desc", "—") if pd.notna(fid) else "—"
        )
        df_display["sensor_source"] = df_display["fault_label"].map(
            lambda fid: FAULT_INFO.get(int(fid), {}).get("sensor", "—") if pd.notna(fid) else "—"
        )
        df_display["parameter_type"] = df_display["fault_label"].map(
            lambda fid: FAULT_INFO.get(int(fid), {}).get("param", "—") if pd.notna(fid) else "—"
        )

        # Reorder columns for clarity
        ordered_cols = ["sample_num", "fault_label", "fault_name", "parameter_type",
                        "sensor_source", "reactor_temp", "reactor_pressure",
                        "combined_anomaly_flag"]
        ordered_cols = [c for c in ordered_cols if c in df_display.columns]
        df_display = df_display[ordered_cols]

        # Style anomaly rows
        def highlight_anomaly(row):
            if row.get("combined_anomaly_flag", 0) == 1:
                return ["background-color: #7f1d1d; color: #fca5a5"] * len(row)
            fid = row.get("fault_label", 0)
            if pd.notna(fid) and int(fid) > 0:
                return ["background-color: #1e293b; color: #fde68a"] * len(row)
            return [""] * len(row)

        styled = df_display.style.apply(highlight_anomaly, axis=1)
        st.dataframe(styled, use_container_width=True, height=400)
    else:
        st.info("No sensor feature data available.")

    # =========================================================================
    # Project Objective Section
    # =========================================================================
    st.markdown("---")
    st.subheader("🎯 Project Objective")
    st.markdown(
        """
        ### What does this system do?

        This is an **Early Warning System for Chemical Plant Safety**. It monitors
        sensor data from the **Tennessee Eastman Process** — a simulation of a real
        industrial chemical plant — and automatically detects dangerous conditions
        before they cause accidents.

        ### How does it work?

        | Step | Component | What it does |
        |------|-----------|-------------|
        | 1️⃣ | **Data Ingestion** | Reads 52 sensor measurements from the plant (temperature, pressure, flow rates, valve positions) |
        | 2️⃣ | **Data Warehouse** | Stores raw data in PostgreSQL, transforms it using dbt into clean analytics tables |
        | 3️⃣ | **Anomaly Detection** | Uses rolling Z-score statistics to flag when sensor readings deviate > 3σ from normal |
        | 4️⃣ | **ML Classification** | RandomForest classifier identifies which of the 20 fault types is occurring |
        | 5️⃣ | **Alarm System** | Dashboard raises 🔴 CRITICAL / 🟡 WARNING / 🟢 NORMAL alerts based on anomaly rates |
        | 6️⃣ | **Orchestration** | Airflow DAG runs the entire pipeline hourly to catch new faults |

        ### Why does it matter?

        In a real chemical plant, **undetected faults cause**:
        - 💥 **Explosions** from reactor pressure surges (Fault 7)
        - 🔥 **Thermal runaway** from cooling system failures (Fault 4, 11)
        - ☠️ **Toxic releases** from feed composition shifts (Fault 1)
        - 💰 **Production losses** from equipment damage

        This system catches these faults **early**, giving operators time to respond
        before a minor sensor anomaly becomes a major industrial accident.
        """
    )

else:
    st.warning("⚠️ No data available. Please run the ingestion and dbt pipelines first.")
    st.markdown(
        """
        **Steps to get started:**
        1. Set up PostgreSQL: `python ingestion/setup_db.py`
        2. Run ingestion: `python ingestion/batch_ingest.py`
        3. Run dbt models: `dbt run --project-dir dbt_project`
        4. Refresh this dashboard
        """
    )

# ---------------------------------------------------------------------------
# Auto-refresh every 30 seconds
# ---------------------------------------------------------------------------
time.sleep(30)
st.rerun()
