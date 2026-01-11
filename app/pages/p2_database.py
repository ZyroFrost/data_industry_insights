# -*- coding: utf-8 -*-
"""
S2 PIPELINE — GLOBAL DATA CONSOLIDATION & DATABASE LOADING

This page manages the final global steps of the pipeline,
operating on the fully processed dataset (no per-file tracking).

Scope:
- STEP 2.8: Combine all validated source files into a single dataset
- STEP 2.9: Split combined data into ERD-compliant tables
- STEP 3.0: Export CSV data to PostgreSQL SQL scripts
- STEP 3.1: Load data into local PostgreSQL
- STEP 3.2: Load data into Neon (cloud PostgreSQL)

Execution Model:
- Pipeline is STEP-BASED (global execution, not file-based)
- Each step runs exactly once and produces a global output
- Status is determined by output existence and execution result

Rules:
- Steps must follow strict order (2.8 → 3.2)
- No column mapping or manual interaction is involved
- Steps are idempotent: re-running regenerates outputs

Input:
- data/data_processing/s2.7_data_salary_exp_validated/*.csv

Output:
- data/data_processing/s2.8_data_combined/
- data/data_processed/ (ERD tables)
- database/ (SQL scripts or loaded tables)

Purpose:
- Finalize clean, normalized data into analytical & database-ready form
- Bridge data processing with persistent storage layers
"""

from calendar import c
import streamlit as st
from streamlit_extras.stylable_container import stylable_container
from assets.styles import (
    stylable_container_pipeline_css,
    stylable_container_pipeline_monitor_css,
    stylable_container_logs_css,
    custom_line_vertical,
)

from pathlib import Path
import importlib.util
import io
import sys
import traceback
import pandas as pd

# =====================================================
# PATH
# =====================================================
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# =====================================================
# PRINT CAPTURE
# =====================================================
class PrintCapture(io.StringIO):
    def __init__(self, log_placeholder):
        super().__init__()
        self.log_placeholder = log_placeholder

    def write(self, text):
        if text.strip():
            if "database_logs" not in st.session_state:
                st.session_state.database_logs = []

            st.session_state.database_logs.append(text)
            self.log_placeholder.code(
                "".join(st.session_state.database_logs[-500:]),
                language="bash"
            )
        return len(text)

# =====================================================
# PIPELINE STEPS (GLOBAL)
# =====================================================
PIPELINE_STEPS = {
    "s2.8": ROOT / "pipeline" / "step2_processing" / "s2_8_combining_data.py",
    "s2.9": ROOT / "pipeline" / "step2_processing" / "s2_9_splitting_tables_erd.py",
    "s3.0": ROOT / "pipeline" / "step3_database_upload" / "s3.0_export_csv_to_postgresql.py",
    "s3.1": ROOT / "pipeline" / "step3_database_upload" / "s3.1_loading_data_to_local_postgre.py",
    "s3.2": ROOT / "pipeline" / "step3_database_upload" / "s3.2_loading_data_to_neon.py",
}

STEP_LABELS = {
    "s2.8": "s2.8 – Combine validated data",
    "s2.9": "s2.9 – Split ERD tables",
    "s3.0": "s3.0 – Export PostgreSQL SQL",
    "s3.1": "s3.1 – Load local PostgreSQL",
    "s3.2": "s3.2 – Load Neon PostgreSQL",
}

STEP_OUTPUT_CHECK = {
    "s2.8": ROOT / "data" / "data_processing" / "s2.8_data_combined" / "combined_all_sources.csv",
    "s2.9": ROOT / "data" / "data_processed",
    "s3.0": ROOT / "database",
    "s3.1": ROOT / "database",
    "s3.2": ROOT / "database",
}

STATUS_ICON = {
    "done": "🟢",
    "fail": "🔴",
    "not_started": "⚪",
    "skipped": "⬛",
}

STEP_OUTPUT_FILES = {
    "s2.8": {
        "type": "file",
        "path": ROOT / "data" / "data_processing" / "s2.8_data_combined",
    },
    "s2.9": {
        "type": "folder",
        "path": ROOT / "data" / "data_processed",
    }
}

# =====================================================
# INIT DATABASE STATE
# =====================================================
def init_database_state():
    state = {}
    for step, path in STEP_OUTPUT_CHECK.items():
        state[step] = "done" if path.exists() else "not_started"
    return state

# =====================================================
# RUN STEP RANGE CONTROL
# =====================================================
def render_run_steps_range(
    pipeline_steps: dict,
    log_placeholder,
    status_placeholder,
):
    step_keys = list(pipeline_steps.keys())

    cRunsteps, cRunsteps_all = st.columns([1, 2])

    # ---- LEFT: title ----
    with cRunsteps:
        st.markdown("#### ▶ Run Steps")

    # ---- RIGHT: range selector + button ----
    with cRunsteps_all:
        col_from, col_arrow, col_to, col_btn = st.columns([2, 0.5, 2, 1.5])

        with col_from:
            step_from = st.selectbox(
                "From",
                step_keys,
                index=0,
                label_visibility="collapsed",
            )

        with col_arrow:
            st.markdown(
                "<div style='text-align:center;padding-top:6px;'>→</div>",
                unsafe_allow_html=True
            )

        with col_to:
            step_to = st.selectbox(
                "To",
                step_keys,
                index=len(step_keys) - 1,
                label_visibility="collapsed"
            )

        with col_btn:
            if st.button("Run", use_container_width=True):

                start_idx = step_keys.index(step_from)
                end_idx = step_keys.index(step_to)

                if start_idx > end_idx:
                    st.warning("⚠️ Step FROM must be before TO")
                    return

                steps_to_run = step_keys[start_idx : end_idx + 1]

                for step in steps_to_run:
                    run_step_wrapper(
                        step_key=step,
                        log_placeholder=log_placeholder,
                        status_placeholder=status_placeholder
                    )

                st.rerun()

# =====================================================
# RUN STEP
# =====================================================
def run_step_from_path(step_key):
    step_path = PIPELINE_STEPS[step_key]
    spec = importlib.util.spec_from_file_location(step_key, step_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    if hasattr(module, "run"):
        module.run()
    else:
        raise RuntimeError(f"{step_key} has no run() function")

def run_step_wrapper(step_key, log_placeholder, status_placeholder):
    database_state = st.session_state.database_state
    database_state[step_key] = "running"
    st.session_state.database_state = database_state

    with status_placeholder:
        with st.status(f"⏳ Running {step_key}...", expanded=True) as status:

            old_stdout = sys.stdout
            old_stderr = sys.stderr
            sys.stdout = PrintCapture(log_placeholder)
            sys.stderr = PrintCapture(log_placeholder)

            try:
                run_step_from_path(step_key)
                database_state[step_key] = "done"
                status.update(label=f"✅ {step_key} completed successfully", state="complete")

            except Exception:
                print(traceback.format_exc())
                database_state[step_key] = "fail"
                status.update(label=f"❌ {step_key} failed", state="error")

            finally:
                sys.stdout = old_stdout
                sys.stderr = old_stderr
                st.session_state.database_state = database_state

@st.dialog("Run Database Steps")
def render_run_steps_dialog(
    pipeline_steps,
    log_placeholder,
    status_placeholder,
):
    step_keys = list(pipeline_steps.keys())

    st.markdown("#### ▶ Run Steps")

    col_from, col_arrow, col_to, col_btn = st.columns([2, 0.5, 2, 1.5])

    with col_from:
        step_from = st.selectbox(
            "From",
            step_keys,
            index=0,
            label_visibility="collapsed",
        )

    with col_arrow:
        st.markdown(
            "<div style='text-align:center;padding-top:6px;'>→</div>",
            unsafe_allow_html=True
        )

    with col_to:
        step_to = st.selectbox(
            "To",
            step_keys,
            index=len(step_keys) - 1,
            label_visibility="collapsed",
        )

    with col_btn:
        if st.button("Run", use_container_width=True):
            start_idx = step_keys.index(step_from)
            end_idx = step_keys.index(step_to)

            if start_idx > end_idx:
                st.warning("⚠️ Step FROM must be before TO")
                return

            steps_to_run = step_keys[start_idx : end_idx + 1]

            for step in steps_to_run:
                run_step_wrapper(
                    step_key=step,
                    log_placeholder=log_placeholder,
                    status_placeholder=status_placeholder
                )

            st.session_state.confirm_run_all_database = False
            st.rerun()

# =====================================================
# RENDER STEP TABLE
# =====================================================
def render_step_table(database_state):

    cStep_desc, cStep_popover = st.columns([1.1, 1], gap=None, vertical_alignment="bottom")
    with cStep_desc:
        st.markdown("#### Steps description")

    with cStep_popover.popover("ℹ️ Database steps", width="stretch"):
        st.markdown("""
        - **s2.8 (Combine data)**: Merge all validated source files.
        - **s2.9 (Split ERD tables)**: Split combined data into ERD tables.
        - **s3.0 (Export SQL)**: Generate PostgreSQL SQL scripts.
        - **s3.1 (Load local DB)**: Load data into local PostgreSQL.
        - **s3.2 (Load Neon)**: Load data into Neon PostgreSQL.
        """)

    st.divider()

    st.markdown("""
    #### Status Legend:
    - 🟢 **Done**: Step completed successfully
    - 🔴 **Fail**: Step failed
    - ⚪ **Not started**: Step has not been executed
    - ⬛ **Skipped**: Skipped due to previous failure
    """)

    st.divider()

    header = st.columns([3, 1])
    header[0].write("STEP")
    header[1].write("STATUS")

    for step in PIPELINE_STEPS:
        cols = st.columns([3, 1])
        cols[0].write(STEP_LABELS[step])
        cols[1].write(STATUS_ICON[database_state.get(step, "not_started")])

# =====================================================
# MAIN UI
# =====================================================
def render_database():

    if "database_state" not in st.session_state:
        st.session_state.database_state = init_database_state()

    if "database_logs" not in st.session_state:
        st.session_state.database_logs = []

    if "open_run_range_database" not in st.session_state:
        st.session_state.open_run_range_database = False

    with stylable_container(key="database_container", css_styles=stylable_container_pipeline_css()):
        cLeft, cDivider, cRight = st.columns([0.8, 0.01, 1.6])

        with cDivider:
            custom_line_vertical()

        with cRight:
            st.subheader("Output & Connection")
            st.divider()

            tLogs, tFiles, tERD, tCloud = st.tabs(["Logs", "Files", "Database ERD", "Cloud Connection"])

            with tLogs:
                with stylable_container(key="database_logs_container", css_styles=stylable_container_logs_css()):
                    status_placeholder = st.empty()
                    log_placeholder = st.empty()

                    if not st.session_state.database_logs:
                        log_placeholder.code("📟 DATABASE TERMINAL OUTPUT\n\nRun a step to see logs.", language="bash")
                    else:
                        log_placeholder.code("".join(st.session_state.database_logs[-500:]), language="bash")

            with tFiles:
                with stylable_container(key="database_files_container", css_styles=stylable_container_logs_css()):
                    st.markdown("#### Database Output Files")
                    st.caption("Outputs generated by each pipeline step")

                    for step, cfg in STEP_OUTPUT_FILES.items():
                        step_label = STEP_LABELS.get(step, step)
                        base_path = cfg["path"]

                        with st.expander(f"📦 {step_label}", expanded=False):

                            if not base_path.exists():
                                st.warning("No output found for this step.")
                                continue

                            # ---- CASE 1: single file step (s2.8) ----
                            if cfg["type"] == "file":
                                files = list(base_path.glob("*.csv"))
                            else:
                                files = sorted(p for p in base_path.iterdir() if p.suffix in (".csv", ".sql"))

                            if not files:
                                st.info("No files available.")
                                continue

                            for f in files:
                                st.markdown(f"📄 **{f.name}**")

                                if f.suffix == ".csv":
                                    try:
                                        df = pd.read_csv(f, nrows=200)
                                        st.dataframe(df, width="stretch")
                                        st.caption(f"Preview first {len(df)} rows")
                                    except Exception as e:
                                        st.error(f"Cannot preview CSV: {e}")

                                elif f.suffix == ".sql":
                                    try:
                                        content = f.read_text(encoding="utf-8")[:5000]
                                        st.code(content, language="sql")
                                        st.caption("Preview first 5000 characters")
                                    except Exception as e:
                                        st.error(f"Cannot preview SQL: {e}")

            with tERD:
                with stylable_container(key="database_erd_container", css_styles=stylable_container_logs_css()):
                    cTitle, cBtn = st.columns([2, 1], gap="small", vertical_alignment="bottom")
                    cTitle.markdown("#### Database ERD Diagram")
                    cBtn.link_button(
                        "🔗 Open Diagram", 
                        "https://dbdocs.io/zyrofrost/data_industry_insights_2020-2025?schema=public&view=relationships&table=role_names", 
                        width="stretch")
                    
                    st.markdown("<br>", unsafe_allow_html=True)
                    st.info("Entity-Relationship Diagram of the analytical database schema")
                    st.image("database/ERD.png", width="stretch")

            with tCloud:
                with stylable_container(key="database_cloud_container", css_styles=stylable_container_logs_css()):
                    cTitle, cBtn = st.columns([2, 1], gap="small", vertical_alignment="bottom")
                    cTitle.markdown("#### Cloud Database Snapshot")
                    cBtn.link_button(
                        "☁ Open Neon Console", 
                        "https://console.neon.tech/app/projects/green-shape-95811574/branches/br-twilight-dawn-a136tl7t/tables", 
                        width="stretch")
                    
                    st.markdown("<br>", unsafe_allow_html=True)
                    st.info(
                        "This is a static snapshot of the Neon database console for reference only. "
                        "Database interactions are intentionally handled outside the application."
                    )
                    st.image("database/Neon.png", width="stretch")

        with cLeft:
            st.subheader("Pipeline Execution")
            st.divider()

            with stylable_container(key="database_container_monitor", css_styles=stylable_container_pipeline_monitor_css()):
                render_step_table(st.session_state.database_state)

                st.divider()

                cRunsteps, cRunsteps_all = st.columns(2)
                cRunsteps.markdown("#### ▶ Run Steps")

                with cRunsteps_all:
                    if st.button("Run All Steps", use_container_width=True):
                        st.session_state.confirm_run_all_database = True

                st.markdown("<br>", unsafe_allow_html=True)

                steps = list(PIPELINE_STEPS.keys())
                BUTTONS_PER_ROW = 4

                for i in range(0, len(steps), BUTTONS_PER_ROW):
                    row = steps[i:i + BUTTONS_PER_ROW]
                    cols = st.columns(BUTTONS_PER_ROW)
                    for col, step in zip(cols, row):
                        if col.button(f"Run {step}", use_container_width=True):
                            run_step_wrapper(step, log_placeholder, status_placeholder)
                            st.rerun()

    if st.session_state.get("confirm_run_all_database"):
        render_run_steps_dialog(
            pipeline_steps=PIPELINE_STEPS,
            log_placeholder=log_placeholder,
            status_placeholder=status_placeholder,
        )