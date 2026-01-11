import streamlit as st
from pathlib import Path
import pandas as pd
import sys

from streamlit_extras.stylable_container import stylable_container
from assets.styles import stylable_container_pipeline_css, stylable_container_mapping_app_css, styable_ml_logs_css
from pipeline.tools.s2_0_column_mapper_app import render as render_s2_0
from analysis.prediction_model.salary_prediction import SalaryPredictionPipeline

from dotenv import load_dotenv
import os
import psycopg2
load_dotenv()

# -------------------------------
# PATH CONFIG
# -------------------------------
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
    
ANALYSIS_DIR = ROOT / "analysis"
DATA_DIR = ANALYSIS_DIR / "data"
EXTERNAL_DIR = ANALYSIS_DIR / "external_data"
EXTERNAL_MAPPED_DIR = EXTERNAL_DIR / "external_mapped_data"


# Cloud Config
BASE_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = BASE_DIR / "data" / "data_processed"

DB_CONFIG = {
    "host": os.getenv("DB_NEON_HOST"),
    "port": os.getenv("DB_NEON_PORT"),
    "dbname": os.getenv("DB_NEON_NAME"),
    "user": os.getenv("DB_NEON_USER"),
    "password": os.getenv("DB_NEON_PASS"),
}

# Initialize pipeline ONCE
salary_predict = SalaryPredictionPipeline()


PREDICT_TYPES = {
    "salary": {
        "label": "Salary Prediction",
        "enabled": True,
    },
}

# -------------------------------
# PREVIEW (NON-PIPELINE) STEPS
# -------------------------------
PREVIEW_STEPS = {
    "step_0_1_raw": {
        "order": 0.1,
        "label": "Raw Data Preview",
    },
    "step_0_2_mapped": {
        "order": 0.2,
        "label": "Mapped Data Preview",
    },
}

ML_STEPS = {
    "step_1_diagnostic": {
        "order": 1,
        "label": "Data Diagnostic",
        "func": salary_predict.step_1_data_diagnostic,
        "required_columns": ["min_salary", "max_salary", "posted_date"],
    },
    "step_2_baseline": {
        "order": 2,
        "label": "Baseline Training",
        "func": salary_predict.step_2_baseline_training,
    },
    "step_3_enrichment": {
        "order": 3,
        "label": "Feature Enrichment",
        "func": salary_predict.step_3_enrichment,
    },
    "step_4_forecasting_holt": {
        "order": 4,
        "label": "External Trend Learning (Holt)",
        "func": salary_predict.step_4_forecasting_holt,
    },

    "step_5_internal_anchoring": {
        "order": 5,
        "label": "Internal Anchoring & Final Forecast",
        "func": salary_predict.step_5_internal_anchoring,
    }
}

STATUS_ICON = {
    "done": "🟢",
    "fail": "🔴",
    "running": "🟡",
    "not_started": "⚪",
}

def get_cloud_tables() -> list[str]:
    """Lấy danh sách tables từ cloud database"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_type = 'BASE TABLE'
            ORDER BY table_name;
        """)
        
        tables = [row[0] for row in cursor.fetchall()]
        cursor.close()
        conn.close()
        
        return tables
        
    except Exception as e:
        st.error(f"❌ Failed to connect to cloud database: {str(e)}")
        return []

def load_data_from_cloud(table_names: list[str]) -> pd.DataFrame:
    """Load data từ cloud database tables"""
    if not table_names:
        return pd.DataFrame()
    
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        dfs = []
        
        for table_name in table_names:
            query = f"SELECT * FROM {table_name}"
            df = pd.read_sql_query(query, conn)
            df["__source_file"] = table_name
            dfs.append(df)
            st.toast(f"📊 Loaded {len(df)} rows from: {table_name}")
        
        conn.close()
        
        if not dfs:
            return pd.DataFrame()
        
        return pd.concat(dfs, ignore_index=True)
        
    except Exception as e:
        st.error(f"❌ Error loading data from cloud: {str(e)}")
        return pd.DataFrame()

def get_available_files(predict_source: str, data_dir: Path, external_dir: Path) -> list[str]:
    def list_csv_files(folder: Path):
        if not folder.exists():
            return []
        return sorted([f.name for f in folder.glob("*.csv")])

    if predict_source == "Predict from Internal Files":
        return list_csv_files(data_dir)
    elif predict_source == "Predict from External Data":
        return list_csv_files(external_dir)
    elif predict_source == "Predict from Database (Cloud)":
        return get_cloud_tables()
    
    return []

def check_files_mapped(selected_files: list[str], mapped_dir: Path) -> tuple[bool, int, int]:
    """
    Check if all selected files have been mapped.
    """
    if not selected_files:
        return False, 0, 0

    mapped_count = 0

    for fname in selected_files:
        mapped_path = mapped_dir / fname
        if mapped_path.exists():
            mapped_count += 1

    total_count = len(selected_files)
    all_mapped = (mapped_count == total_count)

    return all_mapped, mapped_count, total_count


def load_prediction_data(
    predict_source: str,
    selected_files: list[str],
    data_dir: Path,
    external_dir: Path,
    external_mapped_dir: Path,
) -> pd.DataFrame:
    """
    Load data for ML prediction.
    
    IMPORTANT: 
    - Internal Files: Load from data_dir (already clean)
    - External Files: ALWAYS load from external_mapped_dir (mapped columns)
    """

    dfs = []

    if predict_source == "Predict from Internal Files":
        base_dir = data_dir

    elif predict_source == "Predict from External Data":
        base_dir = external_mapped_dir

    else:
        return pd.DataFrame()

    print(f"\n{'='*60}")
    print(f"LOAD PREDICTION DATA")
    print(f"{'='*60}")
    print(f"Source: {predict_source}")
    print(f"Base directory: {base_dir}")
    print(f"Selected files: {selected_files}")
    print(f"{'='*60}\n")

    for fname in selected_files:
        fpath = base_dir / fname

        print(f"Loading file: {fpath}")
        print(f"File exists: {fpath.exists()}")

        if not fpath.exists():
            if predict_source == "Predict from External Data":
                raise FileNotFoundError(
                    f"Mapped file not found: {fpath.name}\n"
                    f"Please run Column Mapping Tool to map '{fname}' first."
                )
            else:
                raise FileNotFoundError(
                    f"Internal file not found: {fpath}\n"
                    f"Please check if the file exists in: {data_dir}"
                )

        df = pd.read_csv(fpath)
        
        print(f"Loaded {len(df)} rows, {len(df.columns)} columns")
        print(f"Columns: {df.columns.tolist()[:10]}...")  # First 10 columns
        
        df["__source_file"] = fname
        dfs.append(df)

    if not dfs:
        return pd.DataFrame()

    result = pd.concat(dfs, ignore_index=True)
    print(f"\nFinal concatenated data: {len(result)} rows\n")
    
    return result


def check_required_columns(df: pd.DataFrame, required_cols: list[str]) -> tuple[bool, list[str]]:
    """
    Check if dataframe has required columns for prediction.
    Returns: (all_present, missing_columns)
    """
    if df.empty:
        return False, required_cols
    
    missing = [col for col in required_cols if col not in df.columns]
    return len(missing) == 0, missing

def run_ml_step(step_key: str, config: dict):
    """
    Run a specific ML step with CHAINED DATAFRAME.
    Step 2 reuses data loaded at Step 1.
    """

    cfg = ML_STEPS[step_key]

    # -------------------------------
    # SET RUNNING STATUS (SAFE)
    # -------------------------------
    st.session_state.ml_steps_state.setdefault(step_key, {})
    st.session_state.ml_steps_state[step_key]["status"] = "running"

    try:
        # ==================================================
        # (A) LOAD BASE DATA (ONLY FOR STEP 1)
        # ==================================================
        if cfg["order"] == 1:
            if config["predict_source"] == "Predict from Database (Cloud)":
                df = load_data_from_cloud(config["selected_files"])
                if df.empty:
                    raise ValueError("No data loaded from cloud database")
            elif config["predict_source"] == "Predict from Internal Files":
                base_dir = DATA_DIR
            elif config["predict_source"] == "Predict from External Data":
                base_dir = EXTERNAL_MAPPED_DIR
            else:
                raise ValueError("Unsupported prediction source")

            dfs = []

            for fname in config["selected_files"]:
                fpath = base_dir / fname

                if not fpath.exists():
                    raise FileNotFoundError(
                        f"Expected mapped file not found: {fpath}\n"
                        f"Please run Column Mapping Tool first."
                    )

                df_part = pd.read_csv(fpath)
                df_part["__source_file"] = fname
                dfs.append(df_part)

            if not dfs:
                raise ValueError("No data loaded from selected files")

            df = pd.concat(dfs, ignore_index=True)

            st.toast(f"📊 Loaded {len(df)} rows, {len(df.columns)} columns")

        # ==================================================
        # (B) DETERMINE INPUT DF
        # ==================================================
        if cfg["order"] == 1:
            input_df = df

        elif cfg["order"] == 2:
            first_step_key = list(ML_STEPS.keys())[0]
            step1_output = st.session_state.ml_step_outputs.get(first_step_key)

            if step1_output is None or step1_output.get("data") is None:
                raise ValueError(
                    "Step 1 data not found. Please run Step 1 before Step 2."
                )

            input_df = step1_output["data"]

        else:
            prev_step_key = list(ML_STEPS.keys())[cfg["order"] - 2]
            prev_output = st.session_state.ml_step_outputs.get(prev_step_key)

            if prev_output is None or prev_output.get("data") is None:
                raise ValueError(
                    f"Previous step ({prev_step_key}) has no data output. "
                    f"Please run steps sequentially."
                )

            input_df = prev_output["data"]

        # ==================================================
        # (C) CHECK REQUIRED COLUMNS
        # ==================================================
        required_cols = cfg.get("required_columns", [])
        if required_cols:
            missing_cols = [c for c in required_cols if c not in input_df.columns]
            if missing_cols:
                raise ValueError(
                    f"Missing required columns: {', '.join(missing_cols)}"
                )

        # ==================================================
        # (D) EXECUTE STEP FUNCTION
        # ==================================================
        step_func = cfg.get("func")
        if step_func is None:
            raise ValueError(f"Step {step_key} has not been implemented yet")

        st.toast(f"🔄 Running {cfg['label']}...")

        if cfg["order"] == 2:
            data_out, model_out, report_out = step_func(
                df=input_df,
                model_type=config.get("model_type"),
                time_granularity=config.get("time_granularity", "monthly"),
            )

        elif cfg["order"] == 5:
            step4_output = st.session_state.ml_step_outputs.get(
                "step_4_forecasting_holt"
            )

            if step4_output is None or "data" not in step4_output:
                raise RuntimeError(
                    "Step 4 output not found. Please run Step 4 before Step 5."
                )

            data_out, model_out, report_out = step_func(
                raw_files=config.get("raw_files"),
                external_trend=step4_output["data"],  # ✅ CHỈ TRUYỀN payload
                time_granularity=config.get("time_granularity", "monthly"),
            )

        else:
            data_out, model_out, report_out = step_func(df=input_df)

        # ==================================================
        # (E) STORE STEP OUTPUT
        # ==================================================
        st.session_state.ml_step_outputs[step_key] = {
            "data": data_out,
            "model": model_out,
            "report": report_out,
        }

        st.session_state.ml_steps_state[step_key] = {
            "status": "done",
            "info": (
                f"Completed with {len(data_out):,} rows"
                if isinstance(data_out, pd.DataFrame)
                else "Completed"
            ),
        }

        # RELEASE FLOW LOCK AFTER STEP FINISHED
        st.session_state.ml_active_step = None

        st.toast("✅ Step completed successfully")

    except Exception as e:
        import traceback

        error_details = traceback.format_exc()

        st.session_state.ml_steps_state[step_key] = {
            "status": "fail",
            "error": str(e),
            "error_details": error_details,
        }

        st.error(f"❌ Step {cfg['order']} failed: {str(e)}")

        with st.expander("🔍 View detailed error", expanded=True):
            st.code(error_details, language="python")

    finally:
        # --------------------------------------------------
        # ENSURE NO STEP IS LEFT IN RUNNING STATE
        # --------------------------------------------------
        if st.session_state.ml_steps_state.get(step_key, {}).get("status") == "running":
            st.session_state.ml_steps_state[step_key]["status"] = "fail"

@st.dialog("Open Column Mapping Tool?", width="small")
def confirm_open_s2_0():
    st.write(
        "You are about to open the Column Mapping Tool.\n\n"
        "This will temporarily replace the current Machine Learning page.\n\n"
        "Your current configuration and data will NOT be lost."
    )

    col1, col2 = st.columns(2)

    with col1:
        if st.button("Cancel"):
            st.session_state.show_step_5_dialog = False
            st.rerun()

    with col2:
        if st.button("Open Mapping Tool", type="primary", key="confirm_mapping"):
            st.session_state.show_s2_0_confirm = False

            st.session_state.mapping_context = {
                "input_dir": str(EXTERNAL_DIR),
                "output_dir": str(EXTERNAL_MAPPED_DIR),
                "selected_files": st.session_state.ml_selected_files,
                "source": "ml",
            }

            st.session_state.open_s2_0 = True
            st.rerun()

@st.dialog("Model Configuration", dismissible=False)
def show_config_dialog():
    st.markdown("### ⚙️ Training Settings")
    
    # Get suggestion from step 1 if available
    suggestion = "No diagnostic data available yet."
    if "step_1_diagnostic" in st.session_state.ml_steps_state:
        output = st.session_state.ml_steps_state["step_1_diagnostic"].get("output")
        if output:
            suggestion = output.get("report", suggestion)
            
    st.info(f"💡 **Suggestion from Step 1:**\n{suggestion}")
    
    # Model list from your new handler
    from analysis.prediction_model.machine_learning_model import MLModelHandler
    model_list = MLModelHandler().get_model_list()
    
    selected_model = st.selectbox("Select Algorithm:", model_list)

    time_granularity = st.selectbox(
        "Time Granularity",
        ["monthly", "quarterly", "yearly"],
    )

    st.divider()
    col1, col2 = st.columns(2)

    if col1.button("🚀 Confirm & Run", use_container_width=True, type="primary"):
        st.session_state.ml_params = {
            "model_type": selected_model,
            "time_granularity": time_granularity,
        }
        st.session_state.show_step_2_dialog = False
        st.session_state.trigger_step_2_run = True
        st.rerun()
        
    if col2.button("Cancel", use_container_width=True):
        st.session_state.show_step_2_dialog = False
        st.rerun()

@st.dialog("Internal Anchoring – Source Selection", dismissible=False, width="medium")
def show_step_5_source_dialog():

    st.markdown("### 📂 Select Source Files for Anchoring")

    ROOT = Path(__file__).resolve().parents[2]
    ANALYSIS_DIR = ROOT / "analysis"
    DATA_DIR = ANALYSIS_DIR / "data"

    INTERNAL_DIR = DATA_DIR
    EXTERNAL_DIR = ANALYSIS_DIR / "external_data" / "external_mapped_data"
    #CLOUD_DIR = DATA_DIR / "cloud"

    # --------------------------------------------------
    # COLLECT SOURCES
    # --------------------------------------------------
    sources = {
        "Internal": INTERNAL_DIR,
        "External": EXTERNAL_DIR,
        #"Cloud": CLOUD_DIR, 
    }

    selected_files = {}

    for label, folder in sources.items():
        if folder.exists():
            files = sorted([f.name for f in folder.glob("*.csv")])
        else:
            files = []

        with st.expander(f"{label}", expanded=(label == "Internal")):
            if files:
                selected = st.multiselect(
                    f"Select files from {label}:",
                    options=files,
                    key=f"step5_{label}_files"
                )
                selected_files[label] = [
                    folder / fname for fname in selected
                ]
            else:
                st.info("No files found.")

    # --------------------------------------------------
    # ACTIONS
    # --------------------------------------------------
    st.divider()
    
    col1, col2 = st.columns(2)

    if col1.button("🚀 Run Anchoring", type="primary", width="stretch"):
        all_files = sum(selected_files.values(), [])
        if not all_files:
            st.error("Please select at least one source file.")
            return

        st.session_state.step_5_selected_raw_files = all_files
        st.session_state.trigger_step_5_run = True
        st.rerun()

    if col2.button("Cancel", width="stretch"):
        st.session_state.show_step_5_dialog = False
        st.rerun()

def render_ml():
    # -------------------------------
    # INIT ML STATES (MUST BE FIRST)
    # -------------------------------
    from_mapping_back = st.session_state.get("_from_mapping_back", False)

    st.session_state.setdefault("show_step_2_dialog", False)
    st.session_state.setdefault("ml_step_2_params", None)
    st.session_state.setdefault("trigger_step_2_run", False)

    st.session_state.setdefault("ml_selected_files", [])
    st.session_state.setdefault("ml_config", None)
    st.session_state.setdefault("config_confirmed", False) 
    st.session_state.setdefault("ml_steps_state", {})
    st.session_state.setdefault("ml_step_outputs", {})
    st.session_state.setdefault("ml_active_step", None)

    st.session_state.setdefault("show_s2_0_confirm", False)
    st.session_state.setdefault("open_s2_0", False)

    if "ml_predict_source" not in st.session_state:
        if st.session_state.get("ml_config") is not None:
            st.session_state.ml_predict_source = st.session_state.ml_config["predict_source"]
        else:
            st.session_state.ml_predict_source = "Predict from Database (Cloud)"

    if from_mapping_back:
        st.session_state._from_mapping_back = False

    if "step_5_dialog_opened" not in st.session_state:
        st.session_state.step_5_dialog_opened = False

    # -------------------------------
    # OPEN STEP 2 MODEL SELECTION DIALOG
    # -------------------------------
    if st.session_state.show_step_2_dialog:
        show_config_dialog()

    # -------------------------------
    # OPEN STEP 5 SOURCE SELECTION DIALOG
    # -------------------------------
    if st.session_state.get("show_step_5_dialog", False):
        show_step_5_source_dialog()

    # -------------------------------
    # TRIGGER STEP 2 AFTER DIALOG
    # -------------------------------
    if st.session_state.trigger_step_2_run:
        st.session_state.trigger_step_2_run = False

        params = st.session_state.get("ml_params")
        if not params:
            st.error("Missing model configuration for Step 2.")
            return

        run_ml_step(
            "step_2_baseline",
            {
                **st.session_state.ml_config,
                **params,
            }
        )

        #st.session_state.ml_active_step = "step_2_baseline"
        st.rerun()

    # -------------------------------
    # TRIGGER STEP 5 AFTER SOURCE SELECTION
    # -------------------------------
    if st.session_state.get("trigger_step_5_run", False):
        st.session_state.trigger_step_5_run = False

        run_ml_step(
            "step_5_internal_anchoring",
            {
                "raw_files": st.session_state.step_5_selected_raw_files
            }
        )

        st.session_state.ml_active_step = None
        st.rerun()

    # -------------------------------
    # OPEN COLUMN MAPPING TOOL
    # -------------------------------
    if st.session_state.show_s2_0_confirm:
        confirm_open_s2_0()

    if st.session_state.open_s2_0:
        with stylable_container(key="s2_0_container", css_styles=stylable_container_mapping_app_css()):
            render_s2_0()
            return

    with stylable_container(key="pipeline_container", css_styles=stylable_container_pipeline_css()):
        
        col1, col2, col3, col4, col5 = st.columns([1, 1, 1, 1, 0.3], vertical_alignment="bottom")

        with col1:
            st.subheader("Prediction From Data")
            st.caption("Select prediction settings to enable steps.")

        with col2:
            OPTIONS = [
                "Predict from Database (Cloud)",
                "Predict from Internal Files",
                "Predict from External Data"
            ]

            predict_source = st.selectbox(
                "Select prediction source",
                OPTIONS,
                index=OPTIONS.index(st.session_state.ml_predict_source),
                key="predict_source_select"
            )

            st.session_state.ml_predict_source = predict_source

        available_files = get_available_files(
            predict_source=predict_source,
            data_dir=DATA_DIR,
            external_dir=EXTERNAL_DIR,
        )

        with col3:
            total_rows = 0

            if st.session_state.ml_selected_files:
                base_dir = (
                    DATA_DIR
                    if predict_source == "Predict from Internal Files"
                    else EXTERNAL_DIR
                )

                for fname in st.session_state.ml_selected_files:
                    fpath = base_dir / fname
                    if fpath.exists():
                        with open(fpath, "r", encoding="utf-8", errors="ignore") as f:
                            total_rows += sum(1 for _ in f) - 1

            cCap, cLen = st.columns([1, 1])
            cCap.caption(f"Total rows: {total_rows:,}")
            cLen.caption(f"Files selected: {len(st.session_state.ml_selected_files)}")

            if from_mapping_back and st.session_state.ml_selected_files:
                st.rerun()

            if available_files:
                with st.popover("Select data files", width="stretch"):
                    st.multiselect(
                        "Files",
                        options=available_files,
                        key="ml_selected_files",
                    )
            else:
                st.button("Select data files", disabled=True)

            if st.session_state.ml_config is not None:
                current_files = set(st.session_state.ml_selected_files)
                config_files = set(st.session_state.ml_config.get("selected_files", []))

                if current_files != config_files:
                    st.toast("Prediction settings changed. Please confirm again.", duration=2)

        with col4:
            enabled_predict_types = [
                v["label"]
                for v in PREDICT_TYPES.values()
                if v["enabled"]
            ]

            predict_type_label = st.selectbox(
                "Prediction type",
                enabled_predict_types,
                key="predict_type_select"
            )

            predict_type = next(
                k for k, v in PREDICT_TYPES.items()
                if v["label"] == predict_type_label
            )

        with col5:
            st.markdown("")
            confirm_disabled = len(st.session_state.ml_selected_files) == 0
            confirmed = st.button("Confirm", key="confirm_config", disabled=confirm_disabled)

        if confirmed:
            st.session_state.ml_config = {
                "predict_source": predict_source,
                "selected_files": st.session_state.ml_selected_files.copy(),
                "predict_type": predict_type,
            }
            st.session_state.ml_predict_source = predict_source

            st.session_state.config_confirmed = True
        
            st.session_state.ml_steps_state = {}
            st.session_state.ml_step_outputs = {}
            st.session_state.ml_active_step = None
            st.toast("✅ Configuration confirmed")

        st.divider()

        # ===============================
        # DEBUG: STEP STATE SNAPSHOT
        # ===============================
        # with st.expander("🐞 DEBUG – ML STEP STATES", expanded=True):
        #     st.write("ml_active_step:", st.session_state.ml_active_step)
        #     st.write("ml_steps_state:")
        #     for k, v in st.session_state.ml_steps_state.items():
        #         st.write(f"  {k}: {v}")

        # -------------------------------
        # RUN PIPELINE
        # -------------------------------
        config = st.session_state.ml_config
        
        mapping_complete = False
        mapped_count = 0
        total_count = 0
        
        if config is not None and config["predict_source"] == "Predict from External Data":
            mapping_complete, mapped_count, total_count = check_files_mapped(
                config["selected_files"],
                EXTERNAL_MAPPED_DIR
            )

        # ROW 1: PREVIEW DATA
        cPreviewTitle, cPreviewBtn = st.columns([1, 8])

        with cPreviewTitle:
            st.markdown("##### Preview Data")
        
        with cPreviewBtn:
        # TẠO GRID CỐ ĐỊNH Số cột
            cols = st.columns(6, gap="small")

            preview_steps = list(PREVIEW_STEPS.items())

            # COLUMN MAPPING TOOL
            with cols[0]:
                if config is not None and config["predict_source"] == "Predict from External Data":
                    if mapping_complete:
                        mapping_icon = "🟢"
                        mapping_label = f"Mapping Tool ({mapped_count}/{total_count})"
                    else:
                        mapping_icon = "🔴"
                        mapping_label = f"Mapping Tool ({mapped_count}/{total_count})"

                    if st.button(f"{mapping_icon} {mapping_label}", key="open_mapping_tool_header", width="stretch"):
                        st.session_state.mapping_context = {
                            "input_dir": str(EXTERNAL_DIR),
                            "output_dir": str(EXTERNAL_MAPPED_DIR),
                            "selected_files": st.session_state.ml_selected_files,
                            "source": "ml",
                        }
                        st.session_state.open_s2_0 = True
                        st.session_state._from_mapping_back = False
                        st.rerun()

                else:
                    st.button("Mapping Tool", disabled=True, key="open_mapping_tool_disabled", width="stretch")

            # STEP 0.1, 0.2
            for i, (step_key, cfg) in enumerate(preview_steps):
                with cols[i+1]:

                    # ---- DISABLE LOGIC ----
                    if step_key == "step_0_1_raw":
                        step_disabled = (
                            config is None
                            or not st.session_state.get("ml_selected_files")
                        )

                    elif step_key == "step_0_2_mapped":
                        step_disabled = not mapping_complete

                    else:
                        step_disabled = False

                    is_active = (st.session_state.ml_active_step == step_key)
                    btn_label = f"👁️ Step {cfg['order']}"
                    btn_type = "primary" if is_active else "secondary"

                    if st.button(btn_label, key=f"preview_{step_key}", disabled=step_disabled, width="stretch", type=btn_type):
                        st.session_state.ml_active_step = step_key
                        st.rerun()

            with cols[5]:
                with st.popover("ℹ️ Describe Steps", width="stretch"):
                    st.markdown("""
                        ### 🔹 Salary Prediction & Analysis Pipeline

                        - **Step 1 (Data Diagnostic & Feasibility Check)**  
                        Define the salary prediction target (`avg_salary`) and evaluate data feasibility  
                        (non-null coverage, variance, minimum sample size).  
                        Provide a preliminary modeling direction based on data characteristics.

                        - **Step 2 (Internal Salary Trend Diagnostic)**  
                        Analyze internal salary data aggregated over time (monthly / quarterly / yearly).  
                        Apply simple time-based models (Linear Regression, Random Forest, XGBoost, etc.)  
                        using a **single time-based feature** to assess whether internal data can learn a stable trend.  
                        This step is **diagnostic only**, not for final prediction.

                        - **Step 3 (External Salary Trend Analysis)**  
                        Analyze external historical salary data to identify long-term market trends.  
                        Evaluate data coverage and suitability as the **primary forecasting signal**.

                        - **Step 4 (External Salary Forecasting)**  
                        Train a time-series forecasting model (e.g. Holt with damped trend)  
                        on external data to learn the long-term salary trend.

                        - **Step 5 (Internal Anchoring & Final Alignment)**  
                        Anchor the external salary forecast to internal salary levels  
                        using recent internal data.  
                        Adjust **forecast level only** (no trend relearning)  
                        to produce the final realistic salary prediction.
                        """)

        # ROW 2: RUN PIPELINE
        cRunTitle, cSteps = st.columns([1, 8])
        
        with cRunTitle:
            st.markdown("##### Run Prediction")

        with cSteps:
            # GRID CỐ ĐỊNH 7 CỘT
            cols = st.columns(6, gap="small")

            # RUN ALL
            with cols[0]:
                if config is None:
                    run_all_disabled = True
                elif config["predict_source"] == "Predict from External Data":
                    run_all_disabled = not mapping_complete
                else:
                    run_all_disabled = False

                with st.popover("Run All Steps", disabled=run_all_disabled, width="stretch"):
                    if st.button("Confirm ☑️", disabled=run_all_disabled, key="run_all_steps", width="stretch"):
                        for step_key in ML_STEPS.keys():
                            run_ml_step(step_key, config)
                        st.session_state.ml_active_step = list(ML_STEPS.keys())[-1]
                        st.rerun()

            # STEP BUTTONS
            all_steps = list(ML_STEPS.items())

            for i, (step_key, cfg) in enumerate(all_steps):
                with cols[i + 1]:
                    step_state = st.session_state.ml_steps_state.get(step_key, {})
                    status = step_state.get("status", "not_started")
                    icon = STATUS_ICON.get(status, "⚪")

                    if config is None:
                        step_disabled = True
                    elif config["predict_source"] == "Predict from External Data" and not mapping_complete:
                        step_disabled = True
                    elif cfg["order"] == 1:
                        step_disabled = False
                    else:
                        prev_step_num = cfg["order"] - 1
                        prev_step_key = list(ML_STEPS.keys())[prev_step_num - 1]
                        prev_status = st.session_state.ml_steps_state.get(prev_step_key, {}).get("status")

                        # # DEBUG
                        # st.write(
                        #     f"[DEBUG] Step {cfg['order']} | prev_step = {prev_step_key} | prev_status = {prev_status}"
                        # )

                        step_disabled = (prev_status != "done")


                    # --- CHIA 2 HÀNG TRONG CÙNG 1 CỘT ---
                    c_run, c_show = st.columns([3, 1], gap=None)

                    # RUN STEP
                    with c_run:
                        is_active = (st.session_state.ml_active_step == step_key)
                        btn_type = "primary" if is_active else "secondary"

                        if st.button(
                            f"{icon} Step {cfg['order']}",
                            key=f"run_{step_key}",
                            disabled=step_disabled,
                            width="stretch",
                            type=btn_type
                        ):
                            # -------------------------------
                            # STEP 2: OPEN MODEL SELECTION DIALOG
                            # -------------------------------
                            if cfg["order"] == 2:
                                st.session_state.show_step_2_dialog = True
                                st.rerun()

                            elif cfg["order"] == 5:
                                if not st.session_state.step_5_dialog_opened:
                                    st.session_state.show_step_5_dialog = True
                                    st.rerun()
                   
                            # -------------------------------
                            # OTHER STEPS: RUN DIRECTLY
                            # -------------------------------
                            else:
                                run_ml_step(step_key, config)
                                st.session_state.ml_active_step = None
                                st.rerun()

                    # VIEW STEP
                    with c_show:
                        step_state = st.session_state.ml_steps_state.get(step_key, {})
                        show_disabled = (
                            step_key not in st.session_state.ml_step_outputs
                            and step_state.get("status") != "fail"
                        )


                        is_viewing = (st.session_state.ml_active_step == step_key)
                        view_btn_type = "primary" if is_viewing else "secondary"

                        if st.button("👁️", key=f"show_{step_key}", disabled=show_disabled, help="View step output", width="stretch", type=view_btn_type):
                            st.session_state.ml_active_step = step_key
                            st.rerun()

        # -------------------------------
        # TABS
        # -------------------------------
        tab_data, tab_model, tab_report = st.tabs(["📊 Data", "🤖 Model", "📄 Report"])
        
        active_step = st.session_state.ml_active_step
        active_output = st.session_state.ml_step_outputs.get(active_step) if active_step else None
        
        with tab_data:
            with stylable_container(key="ml_data_container", css_styles=styable_ml_logs_css()):

                # ===============================
                # STEP 0.1 – RAW DATA PREVIEW
                # ===============================
                if active_step == "step_0_1_raw" and config is not None:
                    dfs = {}

                    base_dir = (
                        DATA_DIR
                        if config["predict_source"] == "Predict from Internal Files"
                        else EXTERNAL_DIR
                    )

                    for fname in config["selected_files"]:
                        fpath = base_dir / fname
                        if fpath.exists():
                            dfs[fname] = pd.read_csv(fpath)

                    tabs = st.tabs(list(dfs.keys()))
                    for tab, (fname, df) in zip(tabs, dfs.items()):
                        with tab:
                            st.caption(f"Raw source: {fname}")
                            st.dataframe(df, width="stretch", height=400)

                # ===============================
                # STEP 0.2 – MAPPED DATA PREVIEW
                # ===============================
                elif active_step == "step_0_2_mapped" and config is not None:
                    dfs = {}

                    for fname in config["selected_files"]:
                        fpath = EXTERNAL_MAPPED_DIR / fname
                        if fpath.exists():
                            dfs[fname] = pd.read_csv(fpath)

                    if not dfs:
                        st.warning("No mapped files found.")
                    else:
                        tabs = st.tabs(list(dfs.keys()))
                        for tab, (fname, df) in zip(tabs, dfs.items()):
                            with tab:
                                st.caption(f"Mapped source: {fname}")
                                st.dataframe(df, width="stretch", height=400)

                # ===============================
                # PIPELINE STEPS (STEP 1+)
                # ===============================
                elif active_output is not None and active_output["data"] is not None:
                    st.success(f"✅ Showing output from: {ML_STEPS[active_step]['label']}")
                    st.dataframe(active_output["data"], width="stretch", height=400)

                elif not st.session_state.config_confirmed:
                    st.info("ℹ️ No data loaded. Please confirm prediction settings above.")

                else:
                    try:
                        preview_df = load_prediction_data(
                            predict_source=config["predict_source"],
                            selected_files=config["selected_files"],
                            data_dir=DATA_DIR,
                            external_dir=EXTERNAL_DIR,
                            external_mapped_dir=EXTERNAL_MAPPED_DIR,
                        )

                        if not preview_df.empty:
                            required_cols = ML_STEPS["step_1_diagnostic"]["required_columns"]
                            missing_cols = [c for c in required_cols if c not in preview_df.columns]

                            if missing_cols:
                                st.error(f"❌ Missing required columns: **{', '.join(missing_cols)}**")
                                st.caption(f"Available columns: {', '.join(preview_df.columns.tolist())}")

                            if config["predict_source"] == "Predict from Internal Files":
                                st.info("👁️ Showing internal data preview")
                            else:
                                st.success("✅ Showing mapped data preview")

                            st.dataframe(preview_df, width="stretch", height=400)

                    except FileNotFoundError as e:
                        st.error(f"❌ {str(e)}")
                        if config["predict_source"] == "Predict from External Data":
                            st.warning(f"⚠️ Column mapping required: {mapped_count}/{total_count} files mapped")
                            st.info("Please click the 'Column Mapping Tool' button above to map your external files.")

        with tab_model:
            with stylable_container(key="ml_model_container", css_styles=styable_ml_logs_css()):
                if active_output is not None and active_output["model"] is not None:
                    st.success(f"✅ Showing output from: {ML_STEPS[active_step]['label']}")
                    model_output = active_output["model"]

                    if isinstance(model_output, dict) and "figure" in model_output:
                        st.pyplot(model_output["figure"])
                    else:
                        st.pyplot(model_output)

                    
                else:
                    failed_steps = [
                        (step_key, state) 
                        for step_key, state in st.session_state.ml_steps_state.items()
                        if state.get("status") == "fail"
                    ]
                    
                    if failed_steps:
                        st.error("❌ Some steps failed. See details below:")
                        
                        for step_key, state in failed_steps:
                            cfg = ML_STEPS[step_key]
                            with st.expander(f"🔴 Step {cfg['order']}: {cfg['label']}", expanded=True):
                                st.write(f"**Error:** {state.get('error', 'Unknown error')}")
                                
                                if "error_details" in state:
                                    st.code(state["error_details"], language="python")
                    else:
                        st.info("ℹ️ No model visualization available. Run a step and click 👁️ to view results.")

        with tab_report:
            with stylable_container(key="ml_report_container", css_styles=styable_ml_logs_css()):
                if active_output is not None and active_output["report"] is not None:
                    st.success(
                        f"✅ Showing output from: {ML_STEPS[active_step]['label']}"
                    )

                    report = active_output["report"]

                    # ==================================================
                    # STEP 2 — SPECIAL TWO-COLUMN REPORT
                    # ==================================================
                    if (
                        active_step == "step_2_baseline"
                        and isinstance(report, dict)
                        and "baseline" in report
                        and "suitability" in report
                    ):
                        col_left, col_right = st.columns(2)

                        with col_left:
                            st.subheader("📊 Baseline Evaluation")
                            st.text(report["baseline"])

                        with col_right:
                            st.subheader("🌍 Data Suitability")
                            st.text(report["suitability"])

                    # ==================================================
                    # OTHER STEPS — NORMAL REPORT
                    # ==================================================
                    else:
                        st.text(report)

                else:
                    st.info(
                        "ℹ️ No report available. Run a step and click 👁️ to view results."
                    )