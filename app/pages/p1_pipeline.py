# -*- coding: utf-8 -*-
"""
S1 PIPELINE — DATA INGESTION & PROCESSING MONITOR

This page monitors and controls the end-to-end data pipeline
from raw data crawling (STEP 1) to structured data processing (STEP 2.7).

Scope:
- STEP 1.1: Crawl job postings from multiple sources
- STEP 2.1 → 2.7: Column mapping, extraction, normalization, enrichment, validation

Execution Model:
- Pipeline is FILE-BASED (each CSV source is tracked independently)
- Each file moves sequentially through the defined steps
- A step is executed globally but status is tracked per file

Rules:
- Steps MUST be executed in order
- If a step fails for a file, all downstream steps are marked as SKIPPED
- Existing DONE steps are not re-executed unless manually rerun

Input:
- data/data_processing/s2.0_data_extracted/*.csv

Output:
- Step-specific folders under data/data_processing/
  (e.g. s2.3_data_values_normalized, s2.7_data_salary_exp_validated)

UI Features:
- File × Step status table
- Per-step execution buttons and range execution
- Real-time terminal log capture

Purpose:
- Ensure data quality, traceability, and reproducibility
- Provide transparent visibility into multi-source processing
"""

import streamlit as st
from streamlit_extras.stylable_container import stylable_container
from assets.styles import stylable_container_pipeline_css, stylable_container_pipeline_monitor_css, stylable_container_logs_css, custom_line_vertical, stylable_container_mapping_app_css

from pathlib import Path
import importlib.util
import io, sys, traceback
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# Phải set ROOT trước mới import được pipeline
from pipeline.tools.s2_0_column_mapper_app import render as render_s2_0

# =====================================================
# PRINT CAPTURE CLASS
# =====================================================
class PrintCapture(io.StringIO):
    def __init__(self, log_placeholder):
        super().__init__()
        self.log_placeholder = log_placeholder
        self.buffer = []
        self.flush_counter = 0
    
    def write(self, text):
        if text.strip():
            if "pipeline_logs" not in st.session_state:
                st.session_state.pipeline_logs = []
            
            st.session_state.pipeline_logs.append(text)
            self.buffer.append(text)
            self.flush_counter += 1
            
            # Flush every 5 writes hoặc nếu có newline
            if self.flush_counter >= 5 or '\n' in text:
                self.flush()
        
        return len(text)
    
    def flush(self):
        """Force update placeholder"""
        if self.buffer:
            self.log_placeholder.code(
                "".join(st.session_state.pipeline_logs[-500:]),
                language="bash"
            )
            self.buffer = []
            self.flush_counter = 0

# =====================================================
# PATH
# =====================================================
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

DATA_DIR = ROOT / "data"
DATA_PROCESSING_DIR = DATA_DIR / "data_processing"
DATA_EXTRACTED_DIR = DATA_PROCESSING_DIR / "s2.0_data_extracted"

# =====================================================
# PIPELINE CONFIG
# =====================================================
# Lấy link để chạy step từng script
PIPELINE_STEPS = {
    "s1.1": ROOT / "pipeline" / "s1_1_run_step1_full_clawlers.py",
    "s2.1": ROOT / "pipeline" / "tools" / "s2_0_column_mapper_app.py",
    "s2.2": ROOT / "pipeline" / "step2_processing" / "s2_2_extracting_description_signals.py",
    "s2.3": ROOT / "pipeline" / "step2_processing" / "s2_3_normalizing_values.py",
    "s2.4": ROOT / "pipeline" / "step2_processing" / "s2_4_enriching_country_from_city.py",
    "s2.5": ROOT / "pipeline" / "step2_processing" / "s2_5_enriching_skill_level_category.py",
    "s2.6": ROOT / "pipeline" / "step2_processing" / "s2_6_standardizing_role_name.py",
    "s2.7": ROOT / "pipeline" / "step2_processing" / "s2_7_validating_salary_exp.py"
}

# Để check trong folder file có tồn tại chưa để check Done
STEP_OUTPUT_DIRS = {
    "s2.1": ROOT / "data" / "data_processing" / "s2.1_data_mapped",
    "s2.2": ROOT / "data" / "data_processing" / "s2.2_data_description_extracted",
    "s2.3": ROOT / "data" / "data_processing" / "s2.3_data_values_normalized",
    "s2.4": ROOT / "data" / "data_processing" / "s2.4_data_country_enriched",
    "s2.5": ROOT / "data" / "data_processing" / "s2.5_data_skill_level_enriched",
    "s2.6": ROOT / "data" / "data_processing" / "s2.6_data_role_name_standardized",
    "s2.7": ROOT / "data" / "data_processing" / "s2.7_data_salary_exp_validated",
}

ML_STEPS = {
    "step_1_diagnostic": {
        "order": 1,
        "label": "Data Diagnostic",
        "action": "step_1_model_family_suggestion",
    },
    "step_2_baseline": {
        "order": 2,
        "label": "Baseline Training",
        "action": "step_2_baseline_training",
    },
    "step_3_enrichment": {
        "order": 3,
        "label": "Feature Enrichment",
        "action": "step_3_enrichment",
    },
    "step_4_final": {
        "order": 4,
        "label": "Final Prediction",
        "action": "step_4_final_prediction",
    },
}

# File name prefix, vì mỗi folder cấu trúc file khác nhau nên phải có công thức nhận dạng file prefix
STEP_FILE_PREFIX = {
    "s2.1": "mapped_",
    "s2.2": "extracted_desc_",
    "s2.3": "normalized_",
    "s2.4": "enriched_",
    "s2.5": "enriched_",
    "s2.6": "standardized_",
    "s2.7": "validated_",
}

STEP_FILE_NAME = {
    "s2.1": "Step 2.1",
    "s2.2": "Step 2.2",
    "s2.3": "Step 2.3",
    "s2.4": "Step 2.4",
    "s2.5": "Step 2.5",
    "s2.6": "Step 2.6",
    "s2.7": "Step 2.7",
}

STATUS_ICON = {
    "done": "🟢",
    "fail": "🔴",
    "not_started": "⚪",
    "skipped": "⬛",
    "running": "🟡", 
}

# Origin icon (ban đầu là dùng icon để hiển thị nên mới có dict này)
ORIGIN_ICON = {
    "CRAWLED": "Crawled",
    "EXTERNAL": "External",
}

# --- TRONG FILE p1_pipeline.py ---

# 1. Khởi tạo State (đảm bảo các biến này có trong st.session_state)
def init_session_state():
    """Initialize all required session state variables"""
    if "pipeline_state" not in st.session_state:
        st.session_state.pipeline_state = init_pipeline_state()
    
    if "pipeline_logs" not in st.session_state:
        st.session_state.pipeline_logs = []
    
    # REALTIME EXECUTION STATE
    if "is_running" not in st.session_state:
        st.session_state.is_running = False
    
    if "current_file_idx" not in st.session_state:
        st.session_state.current_file_idx = 0
    
    if "current_step_idx" not in st.session_state:
        st.session_state.current_step_idx = 0
    
    if "start_step_key" not in st.session_state:
        st.session_state.start_step_key = None
    
    if "end_step_key" not in st.session_state:
        st.session_state.end_step_key = None
    
    # UI FLAGS
    if "open_run_steps_dialog" not in st.session_state:
        st.session_state.open_run_steps_dialog = False
    
    if "open_s2_0" not in st.session_state:
        st.session_state.open_s2_0 = False
    
    if "show_s2_0_confirm" not in st.session_state:
        st.session_state.show_s2_0_confirm = False

# 2. Hàm thực thi chính theo cơ chế "Dừng để render"
def execute_pipeline_realtime():
    """
    CORE REALTIME LOGIC:
    - Execute exactly 1 step for 1 file
    - Update session state
    - Rerun to render UI
    """
    if not st.session_state.is_running:
        return
    
    # Get execution context
    target_files_list = sorted(list(st.session_state.pipeline_state.keys()))
    step_keys = list(PIPELINE_STEPS.keys())
    
    start_idx = step_keys.index(st.session_state.start_step_key)
    end_idx = step_keys.index(st.session_state.end_step_key)
    steps_to_run = step_keys[start_idx:end_idx + 1]
    
    file_idx = st.session_state.current_file_idx
    step_idx = st.session_state.current_step_idx
    
    # Check completion
    if file_idx >= len(target_files_list):
        st.session_state.is_running = False
        st.session_state.current_file_idx = 0
        st.session_state.current_step_idx = 0
        st.toast("🎉 Pipeline completed for all files!")
        return
    
    if step_idx >= len(steps_to_run):
        # Move to next file
        st.session_state.current_file_idx += 1
        st.session_state.current_step_idx = 0
        st.rerun()
        return
    
    # Get current task
    file_name = target_files_list[file_idx]
    step_key = steps_to_run[step_idx]

    step_keys_all = list(PIPELINE_STEPS.keys())
    step_idx_all = step_keys_all.index(step_key)

    # FIX: resolve đúng input file theo step trước
    if step_key == "s2.1":
        file_path = DATA_EXTRACTED_DIR / file_name
    else:
        prev_step = step_keys_all[step_idx_all - 1]
        prev_dir = STEP_OUTPUT_DIRS[prev_step]
        prev_dir.mkdir(parents=True, exist_ok=True)

        stem = Path(file_name).stem
        prefix = STEP_FILE_PREFIX[prev_step]

        matched = list(prev_dir.glob(f"{prefix}*{stem}*.csv"))
        if not matched:
            raise FileNotFoundError(
                f"Input file for {step_key} not found in {prev_dir}"
            )

        file_path = matched[0]

    # ===== FILESYSTEM-BASED DECISION =====
    already_done = detect_step_done(step_key, file_name)

    if already_done and not st.session_state.overwrite_existing:
        st.session_state.pipeline_state[file_name][step_key] = "done"
        st.session_state.current_step_idx += 1
        st.rerun()
        return

    if st.session_state.overwrite_existing:
        out_dir = STEP_OUTPUT_DIRS.get(step_key)
        if out_dir and out_dir.exists():
            stem = Path(file_name).stem
            prefix = STEP_FILE_PREFIX[step_key]
            for f in out_dir.glob(f"{prefix}*{stem}*.csv"):
                f.unlink()

    print(
        f"[PIPELINE] Running {step_key} "
        f"({st.session_state.current_file_idx + 1}/"
        f"{len(target_files_list)}) → {file_name}"
    )

    st.session_state.pipeline_state[file_name][step_key] = "running"
    
    # ===== FIX: DÙNG PLACEHOLDER ĐÃ LƯU TRONG SESSION STATE =====
    old_stdout, old_stderr = sys.stdout, sys.stderr
    pc = PrintCapture(st.session_state.pipeline_log_placeholder)
    sys.stdout = pc
    sys.stderr = pc
    
    try:
        # ===== FIX: STATUS HIỂN THỊ VÀO ĐÚNG PLACEHOLDER =====
        with st.session_state.pipeline_status_placeholder:
            with st.status(f"⏳ {step_key} → {file_name}", expanded=True) as status:
                run_step_for_single_file(step_key, file_path)
                st.session_state.pipeline_state[file_name][step_key] = "done"

            st.session_state.current_step_idx += 1
            st.rerun()
            
    except Exception as e:
        print(traceback.format_exc())
        st.session_state.pipeline_state[file_name][step_key] = "fail"
        
        # Mark downstream steps as skipped
        for s in get_next_steps(step_key):
            st.session_state.pipeline_state[file_name][s] = "skipped"
        
        st.error(f"❌ {step_key} failed: {file_name}")
    
    finally:
        # Force final flush
        if hasattr(pc, 'flush'):
            pc.flush()
        
        sys.stdout, sys.stderr = old_stdout, old_stderr

# =====================================================
# RUN STEPS DIALOG
# =====================================================

# Dialog khi bấm nút Run All Steps
@st.dialog("Run Pipeline Steps", width="medium")
def render_run_steps_dialog(
    pipeline_steps,
    pipeline_state,
    log_placeholder,
    table_placeholder,
    status_placeholder,
):
    render_run_steps_range(
        pipeline_steps=pipeline_steps,
        pipeline_state=pipeline_state,
        log_placeholder=log_placeholder,
        table_placeholder=table_placeholder,
        status_placeholder=status_placeholder,
    )

    # st.divider()
    # _, cClose = st.columns([1, 1])
    # if cClose.button("Close", width="stretch"):
    #     st.session_state.open_run_steps_dialog = False
    #     st.rerun()

# =====================================================
# INIT PIPELINE STATE
# =====================================================
def init_pipeline_state():

    if "open_run_steps_dialog" not in st.session_state:
        st.session_state.open_run_steps_dialog = False

    # Lấy danh sách CRAWLER REGISTRY từ file step 1 để biết được file nào là CRAWLED hoặc EXTERNAL 
    # (check file trong s2.0_data_extracted có tồn tại trong REGISTRY hay không)
    from pipeline.step1_crawlers.s1_1_run_step1_full_clawlers import CRAWLER_REGISTRY
    
    pipeline_state = {}
    crawler_files = set(CRAWLER_REGISTRY.keys())

    for csv_file in DATA_EXTRACTED_DIR.glob("*.csv"):
        file_name = csv_file.name
        origin = "CRAWLED" if file_name in crawler_files else "EXTERNAL"

        pipeline_state[file_name] = {
            "origin": origin,
            "s1.1": "done",
        }

        # =================================================
        # AUTO-DETECT STEP STATUS FROM FILE SYSTEM
        # =================================================

        # s2.1 → s2.7: check file cùng tên trong đúng output dir
        for file_name in pipeline_state.keys():
            stem = Path(file_name).stem

            for step, out_dir in STEP_OUTPUT_DIRS.items():
                prefix = STEP_FILE_PREFIX[step]

                if not out_dir.exists():
                    pipeline_state[file_name][step] = "not_started"
                    continue

                pattern = f"{prefix}*{stem}*.csv"
                matched_files = list(out_dir.glob(pattern))

                pipeline_state[file_name][step] = "done" if matched_files else "not_started"

        # # s2.8: combined step (single global file)
        # combined_file = (
        #     ROOT / "data" / "data_processing" / "s2.8_data_combined" / "combined_all_sources.csv"
        # )

        # s2_8_done = combined_file.exists()

        # for file_name in pipeline_state.keys():
        #     pipeline_state[file_name]["s2.8"] = "done" if s2_8_done else "not_started"

        # # s2.9: final ERD tables
        # processed_dir = ROOT / "data" / "data_processed"

        # s2_9_done = processed_dir.exists() and any(
        #     p.suffix == ".csv" for p in processed_dir.iterdir()
        # )

        # for file_name in pipeline_state.keys():
        #     pipeline_state[file_name]["s2.9"] = "done" if s2_9_done else "not_started"

    return pipeline_state

# =====================================================
# HELPER
# =====================================================
def get_next_steps(step_key):
    step_keys = list(PIPELINE_STEPS.keys())
    if step_key not in step_keys:
        return []
    idx = step_keys.index(step_key)
    return step_keys[idx + 1:]

def detect_step_done(step_key: str, file_name: str) -> bool:
    out_dir = STEP_OUTPUT_DIRS.get(step_key)
    if not out_dir or not out_dir.exists():
        return False

    stem = Path(file_name).stem
    prefix = STEP_FILE_PREFIX[step_key]
    return any(out_dir.glob(f"{prefix}*{stem}*.csv"))

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
    
def run_step_for_single_file(step_key: str, file_path: Path):
    """
    Chạy 1 step cho ĐÚNG 1 file
    Input: file_path là Path object, không phải string
    """
    step_path = PIPELINE_STEPS[step_key]

    spec = importlib.util.spec_from_file_location(step_key, step_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    if not hasattr(module, "run"):
        raise RuntimeError(f"{step_key} has no run() function")

    if not file_path.exists():
        raise FileNotFoundError(f"Input file not found: {file_path}")

    module.run(target_files=[file_path])

def run_single_file_through_pipeline(
    file_path: Path,
    start_step: str,
    end_step: str,
    pipeline_state: dict,
    log_placeholder,
    table_placeholder,
    status_placeholder
):
    step_keys = list(PIPELINE_STEPS.keys())
    start_idx = step_keys.index(start_step)
    end_idx = step_keys.index(end_step)
    
    steps_to_run = step_keys[start_idx:end_idx + 1]
    
    for step_key in steps_to_run:
        pipeline_state[file_path.name][step_key] = "running"
        st.session_state.pipeline_state = pipeline_state
        
        with table_placeholder.container():
            render_file_step_table(pipeline_state, PIPELINE_STEPS)
        
        with status_placeholder:
            with st.status(f"⏳ Running {step_key} for {file_path.name}...", expanded=True) as status:
                try:
                    run_step_for_single_file(step_key, file_path, log_placeholder)
                    pipeline_state[file_path.name][step_key] = "done"
                    status.update(label=f"✅ {step_key} completed", state="complete")
                except Exception as e:
                    print(traceback.format_exc())
                    pipeline_state[file_path.name][step_key] = "fail"
                    
                    for s in get_next_steps(step_key):
                        pipeline_state[file_path.name][s] = "skipped"
                    
                    status.update(label=f"❌ {step_key} failed", state="error")
                    break
        
        st.session_state.pipeline_state = pipeline_state

# =====================================================
# RENDER TABLE
# =====================================================
def render_file_step_table(pipeline_state: dict, step_order: list[str]):

    cStep_desc, cStep_popover = st.columns([1.1, 1], gap=None, vertical_alignment="bottom")   
    with cStep_desc:
        st.markdown("#### Pipeline steps description")

    with cStep_popover.popover("ℹ️ Pipeline steps", width="stretch"):
        st.markdown("""
        - **s1.1 (Crawl data)**: Run all crawlers to collect raw job posting data.
        - **s2.1 (Map columns)**: Map raw source columns to the ERD schema (Use interactive tool).
        - **s2.2 (Extract data)**: Extract salary, experience, location, and remote signals.
        - **s2.3 (Normalize data)**: Normalize values into canonical formats.
        - **s2.4 (Enrich data)**: Enrich country and ISO from city references.
        - **s2.5 (Enrich data)**: Assign skill levels and categories.
        - **s2.6 (Standardize data)**: Standardize job titles into role names.
        - **s2.7 (Validate data)**: Validate salary and experience values.
        """)

    st.divider()

    st.markdown(
        """
        #### Status Legend:
        - 🟢 **Done**: Step completed successfully
        - 🔴 **Fail**: Step failed (next steps skipped)
        - ⚪ **Not started**: Step has not been executed
        - ⬛ **Skipped**: Skipped due to previous failure
        """
    )

    st.divider()

    st.markdown("#### Files x Steps Table")
    st.markdown("<br>", unsafe_allow_html=True)

    # Tile của cột
    header_cols = st.columns([3.2, 2] + [1.2] * len(step_order), gap="small")
    header_cols[0].write("FILE")
    header_cols[1].write("Origin")

    for i, step in enumerate(step_order):
        header_cols[i + 2].write(step)

    for file, info in pipeline_state.items():
        # Từng dòng của cột
        cols = st.columns([3.2, 2] + [1.2] * len(step_order), gap="small")

        cols[0].markdown(
            f"""
            <div style="
                min-height:3rem;
                max-height:3rem;
                padding-bottom:1rem;   /* CHỪA CHỖ CHO SCROLLBAR */
                box-sizing:border-box;
            ">
                <div style="
                    height:100%;
                    overflow-x:scroll;
                    overflow-y:hidden;
                    white-space:nowrap;
                ">
                    {file}
                </div>
            </div>
            """,
            unsafe_allow_html=True
        )

        cols[1].write(ORIGIN_ICON.get(info["origin"], "❓"))

        for i, step in enumerate(step_order):
            state = info.get(step, "not_started")
            cols[i + 2].write(STATUS_ICON.get(state, "⚪"))

# =====================================================
# RUN STEP RANGE CONTROL
# =====================================================
def render_run_steps_range(
    pipeline_steps: dict,
    pipeline_state: dict,
    log_placeholder,
    table_placeholder,
    status_placeholder,
):
    step_keys = [s for s in pipeline_steps if s not in ("s1.1", "s2.1")]

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
                label_visibility="collapsed",
            )

        # ✅ POPOVER = NÚT RUN
        with col_btn.popover("Run", width="stretch"):
            b1, b2, b3 = st.columns([0.8, 1.6, 0.9], gap=None)

            with b1:
                run_skip = st.button(
                    "Skip",
                    use_container_width=True,
                    key="range_run_skip"
                )

            with b2:
                run_overwrite = st.button(
                    "Overwrite",
                    type="primary",
                    use_container_width=True,
                    key="range_run_overwrite"
                )

            with b3:
                cancel = st.button(
                    "Cancel",
                    use_container_width=True,
                    key="range_run_cancel"
                )

            if cancel:
                st.session_state.open_run_steps_dialog = False
                return

            if run_skip or run_overwrite:
                step_keys_all = list(PIPELINE_STEPS.keys())
                start_idx = step_keys_all.index(step_from)
                end_idx = step_keys_all.index(step_to)

                if start_idx > end_idx:
                    st.warning("⚠️ Step FROM must be before TO")
                    return

                st.session_state.overwrite_existing = bool(run_overwrite)

                st.session_state.is_running = True
                st.session_state.current_file_idx = 0
                st.session_state.current_step_idx = 0
                st.session_state.start_step_key = step_from
                st.session_state.end_step_key = step_to

                st.session_state.open_run_steps_dialog = False
                st.rerun()

# =====================================================
# MAIN UI
# =====================================================
def render_pipeline():
    init_session_state() 

    # if st.session_state.is_running:
    #     execute_pipeline_realtime()

    if "open_s2_0" not in st.session_state:
        st.session_state.open_s2_0 = False

    if "show_s2_0_confirm" not in st.session_state:
        st.session_state.show_s2_0_confirm = False

    if "pipeline_state" not in st.session_state:
        st.session_state.pipeline_state = init_pipeline_state()
    
    if "pipeline_logs" not in st.session_state:
        st.session_state.pipeline_logs = []

    # ===== INIT PLACEHOLDERS (GLOBAL FOR THIS RERUN) =====
    if "pipeline_table_placeholder" not in st.session_state:
        st.session_state.pipeline_table_placeholder = None

    if "pipeline_log_placeholder" not in st.session_state:
        st.session_state.pipeline_log_placeholder = None

    if "pipeline_status_placeholder" not in st.session_state:
        st.session_state.pipeline_status_placeholder = None

    if "overwrite_existing" not in st.session_state:
        st.session_state.overwrite_existing = False

    if "show_overwrite_popover" not in st.session_state:
        st.session_state.show_overwrite_popover = False

    with stylable_container(key="pipeline_container", css_styles=stylable_container_pipeline_css()):
        if st.session_state.show_s2_0_confirm:
            
            @st.dialog("Open Column Mapping Tool?", width="small", dismissible=False)
            def confirm_open_s2_0():
                st.write(
                    "This step will open the **Column Mapping Tool** "
                    "in the pipeline view.\n"
                    "No data will be processed automatically.\n"
                )

                c1, c2 = st.columns(2)

                if c1.button("Cancel", use_container_width=True):
                    st.session_state.show_s2_0_confirm = False
                    st.rerun()

                if c2.button("Confirm", type="primary", use_container_width=True):
                    st.session_state.show_s2_0_confirm = False
                    st.session_state.open_s2_0 = True
                    st.rerun()

            confirm_open_s2_0()
        
        if st.session_state.open_s2_0:
            with stylable_container(key="s2_0_container", css_styles=stylable_container_mapping_app_css()):
                render_s2_0()
                return

        else:
            cLeft, cDivider, cRight = st.columns([1.2, 0.01, 1.6])

            with cDivider.container(horizontal=True, horizontal_alignment="center", vertical_alignment="center"):
                #st.write("")
                custom_line_vertical()

            with cRight:
                st.subheader("Output & Source Files")
                st.divider()

                # =====================================================
                # Tabs: Logs / Raw Data / s2.x outputs
                # =====================================================
                tab_labels = (
                    ["Logs", "Step 1.1 (Raw Data)"]
                    + [STEP_FILE_NAME.get(step_key, step_key) for step_key in STEP_OUTPUT_DIRS.keys()]
                )
                tabs = st.tabs(tab_labels)

                # =======================
                # TAB 0 — LOGS
                # =======================
                with tabs[0]:
                    with stylable_container(
                        key="pipeline_logs_container",
                        css_styles=stylable_container_logs_css()
                    ):
                        st.session_state.pipeline_status_placeholder = st.empty()
                        st.session_state.pipeline_log_placeholder = st.empty()

                        st.markdown("""
                            <style>
                            .element-container:has(div[data-testid="stCodeBlock"]) div[data-testid="stCodeBlock"] {
                                max-height: 80vh !important;
                                overflow-y: auto !important;
                                padding-right: 0.5rem !important;
                            }
                            </style>
                        """, unsafe_allow_html=True)

                        if not st.session_state.pipeline_logs:
                            st.session_state.pipeline_log_placeholder.code(
                                "📟 PIPELINE LOGS OUTPUT\n\n"
                                "This panel displays execution messages and logs when the pipeline is running.\n"
                                "Before any step is executed, this area shows an informational note.\n"
                                "Once you run a step, real-time logs will appear here and this note will disappear.\n\n"
                                "PIPELINE STEPS OVERVIEW:\n\n"
                                "s1.1  Crawl data\n"
                                "- Run all crawlers to collect raw job posting data from multiple sources.\n"
                                "- Output raw extracted CSV files for downstream processing.\n\n"
                                "s2.1  Column Mapping (Interactive)\n"
                                "- Manually and automatically map raw source columns to the ERD schema.\n"
                                "- Ensure consistent column structure across all datasets before processing.\n\n"
                                "s2.2  Extract description signals\n"
                                "- Parse job descriptions to extract salary, experience, and remote signals.\n"
                                "- Preserve extracted values without normalization.\n\n"
                                "s2.3  Normalize values\n"
                                "- Standardize city names, countries, currencies, and employment types.\n"
                                "- Convert heterogeneous values into canonical formats.\n\n"
                                "s2.4  Enrich country from city\n"
                                "- Infer country and ISO codes based on city references.\n"
                                "- Fill missing geographic attributes using reference datasets.\n\n"
                                "s2.5  Enrich skill level & category\n"
                                "- Assign skill levels and skill categories using rule-based mappings.\n"
                                "- Improve skill-related consistency across job postings.\n\n"
                                "s2.6  Standardize role names\n"
                                "- Normalize job titles into predefined canonical role names.\n"
                                "- Reduce title variations and improve role-level analysis.\n\n"
                                "s2.7  Validate salary & experience\n"
                                "- Validate salary ranges and experience values for logical consistency.\n"
                                "- Flag or correct invalid and out-of-range data.\n\n"
                                "▶ Select a step to run and monitor execution logs here.\n",
                                language="bash")
                        else:
                            st.session_state.pipeline_log_placeholder.code(
                                "".join(st.session_state.pipeline_logs[-500:]),
                                language="bash"
                            )

                # =======================
                # TAB 1 — RAW DATA (s2.0)
                # =======================
                with tabs[1]:
                    with stylable_container(
                        key="pipeline_files_container_raw",
                        css_styles=stylable_container_logs_css()
                    ):
                        st.markdown("#### Source Files")
                        st.caption("Preview first 200 rows per file")

                        for file_name in sorted(st.session_state.pipeline_state.keys()):
                            file_path = DATA_EXTRACTED_DIR / file_name

                            if not file_path.exists():
                                continue

                            with st.expander(f"📄 {file_name}", expanded=False):
                                try:
                                    df = pd.read_csv(file_path, nrows=200)
                                    st.dataframe(df, width="stretch")
                                    st.caption(f"Showing first {len(df)} rows")
                                except Exception as e:
                                    st.error(f"❌ Cannot preview file: {str(e)}")

                # =======================
                # TAB 2+ — STEP OUTPUTS
                # =======================
                for idx, step_key in enumerate(STEP_OUTPUT_DIRS.keys(), start=2):
                    out_dir = STEP_OUTPUT_DIRS[step_key]

                    with tabs[idx]:
                        with stylable_container(
                            key=f"pipeline_files_container_{step_key}",
                            css_styles=stylable_container_logs_css()
                        ):
                            st.markdown(f"#### Output Files – {STEP_FILE_NAME.get(step_key, step_key)}")

                            if not out_dir.exists():
                                st.info("No output folder found for this step.")
                                continue

                            files = sorted(out_dir.glob("*.csv"))
                            if not files:
                                st.info("No files generated for this step yet.")
                                continue

                            for file_path in files:
                                with st.expander(f"📄 {file_path.name}", expanded=False):
                                    try:
                                        df = pd.read_csv(file_path, nrows=200)
                                        st.dataframe(df, width="stretch")
                                        st.caption(f"Showing first {len(df)} rows")
                                    except Exception as e:
                                        st.error(f"❌ Cannot preview file: {str(e)}")

            with cLeft:
                with st.container(border=False):
                    st.subheader("Tracking Monitor")
                    
                    st.divider()

                with stylable_container(key="pipeline_container_monitor", css_styles=stylable_container_pipeline_monitor_css()):      
                    render_file_step_table(
                        pipeline_state=st.session_state.pipeline_state,
                        step_order=PIPELINE_STEPS
                    )

                    st.divider()
                    
                    cRunsteps, cRunsteps_all = st.columns(2)
                    cRunsteps.markdown("#### ▶ Run Steps")

                    with cRunsteps_all:
                        if st.button("Run Files by Step Range", width="stretch"):
                            st.session_state.open_run_steps_dialog = True

                    st.markdown("<br>", unsafe_allow_html=True)

                    steps = list(PIPELINE_STEPS.keys())
                    BUTTONS_PER_ROW = 4

                    for i in range(0, len(steps), BUTTONS_PER_ROW):
                        row_steps = steps[i:i + BUTTONS_PER_ROW]
                        cols = st.columns(BUTTONS_PER_ROW)

                        for col, step in zip(cols, row_steps):

                            # s2.1 giữ logic cũ
                            if step == "s2.1":
                                if col.button(
                                    "Run s2.1",
                                    key=f"run_btn_{step}",
                                    width="stretch"
                                ):
                                    st.session_state.show_s2_0_confirm = True
                                    st.rerun()
                                continue

                            # ==== POPOVER CHÍNH LÀ NÚT ====
                            with col.popover(
                                f"Run {step}",
                                width="stretch"
                            ):
                                c1, c2 = st.columns([1,1.8], gap=None)

                                with c1:
                                    if st.button(
                                        "Skip",
                                        key=f"run_skip_{step}",
                                        width="stretch"
                                    ):
                                        st.session_state.overwrite_existing = False

                                        st.session_state.is_running = True
                                        st.session_state.current_file_idx = 0
                                        st.session_state.current_step_idx = 0
                                        st.session_state.start_step_key = step
                                        st.session_state.end_step_key = step
                                        st.rerun()

                                with c2:
                                    if st.button(
                                        "Overwrite",
                                        key=f"run_overwrite_{step}",
                                        type="primary",
                                        width="stretch"
                                    ):
                                        st.session_state.overwrite_existing = True

                                        st.session_state.is_running = True
                                        st.session_state.current_file_idx = 0
                                        st.session_state.current_step_idx = 0
                                        st.session_state.start_step_key = step
                                        st.session_state.end_step_key = step
                                        st.rerun()

        # Gọi dialog run steps nếu session state đúng
        if st.session_state.open_run_steps_dialog:
            render_run_steps_dialog(
                pipeline_steps=PIPELINE_STEPS,
                pipeline_state=st.session_state.pipeline_state,
                log_placeholder=st.session_state.pipeline_log_placeholder,
                table_placeholder=st.session_state.pipeline_table_placeholder,
                status_placeholder=st.session_state.pipeline_status_placeholder,
            )

            st.session_state.open_run_steps_dialog = False
    
    # ===== THÊM ĐOẠN NÀY - GỌI EXECUTION SAU KHI UI ĐÃ RENDER =====
    # Execute pipeline if running (MUST be after all UI rendered)
    if st.session_state.is_running:
        execute_pipeline_realtime()