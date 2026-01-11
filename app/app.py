from matplotlib.pylab import f
import streamlit as st
from streamlit_extras.stylable_container import stylable_container
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from assets.styles import set_global_css, stylable_container_menu_css, icon_button, container_title_css
from pages.p0_overview import render_overview
from pages.p1_pipeline import render_pipeline
from pages.p2_database import render_database
from pages.p3_analysis import render_analysis
from pages.p4_dashboard import render_dashboard
from pages.p5_machine_learning import render_ml

# =====================================================
# PAGE CONFIG
# =====================================================
set_global_css()
container_title_css()

# =====================================================
# TOP MENU (HORIZONTAL)
# =====================================================
# Default state
if "current_page" not in st.session_state:
    st.session_state.current_page = "project_overview"

# HEADER
with st.container(key="title_container", border=False, gap="small"):
    if st.session_state.current_page == "project_overview":
        st.subheader("DATA INDUSTRY INSIGHTS   |   Project Overview")
    elif st.session_state.current_page == "pipeline":
        st.subheader("DATA INDUSTRY INSIGHTS   |   Processing Data Pipeline")
    elif st.session_state.current_page == "database":
        st.subheader("DATA INDUSTRY INSIGHTS   |   Database Pipeline")
    elif st.session_state.current_page == "analysis":
        st.subheader("DATA INDUSTRY INSIGHTS   |   Data Analysis")
    elif st.session_state.current_page == "dashboard":
        st.subheader("DATA INDUSTRY INSIGHTS   |   Dashboard")
    elif st.session_state.current_page == "machine_learning":
        st.subheader("DATA INDUSTRY INSIGHTS   |   Machine Learning")

cLeft, cRight = st.columns([1, 17], gap="small")

with cLeft:
    with stylable_container(key="menu_container", css_styles=stylable_container_menu_css()):

        icon_button(
            page_key="overview",
            label="Overview",
            icon_path="app/assets/icons/project-management_white.png",
            current_page=st.session_state.current_page,
        )

        icon_button(
            page_key="pipeline",
            label="Pipeline",
            icon_path="app/assets/icons/data-pipeline-white.png",
            current_page=st.session_state.current_page,
        )

        icon_button(
            page_key="database",
            label="Database",
            icon_path="app/assets/icons/database_white.png",
            current_page=st.session_state.current_page,
        )

        icon_button(
            page_key="analysis",
            label="Analysis",
            icon_path="app/assets/icons/analysis_white.png",
            current_page=st.session_state.current_page,
        )

        icon_button(
            page_key="dashboard",
            label="Dashboard",
            icon_path="app/assets/icons/control-panel_white.png",
            current_page=st.session_state.current_page,
        )

        icon_button(
            page_key="machine_learning",
            label="Prediction",
            icon_path="app/assets/icons/brain_white.png",
            current_page=st.session_state.current_page,
        )

# =====================================================
# MAIN CONTENT CONTAINER
# =====================================================

with cRight:
    # Thụt pages xuống đề ko bị che mất
    #st.markdown("<div style='height: 4.5rem'></div>", unsafe_allow_html=True)

    #custom_line()
    
    # =====================================================
    # GLOBAL SESSION STATE INIT (BẮT BUỘC)
    # =====================================================
    if "lang" not in st.session_state:
        st.session_state.lang = "vi"

    # --------------------------------
    # PAGE CHANGE HANDLER
    # --------------------------------
    if "prev_page" not in st.session_state:
        st.session_state.prev_page = st.session_state.current_page

    if st.session_state.current_page != st.session_state.prev_page:
        # đóng mapping tool khi đổi page
        st.session_state.pop("open_s2_0", None)
        st.session_state.pop("mapping_context", None)

        # ❌ KHÔNG reset ml_selected_files, ml_config
        # ✅ chỉ reset runtime state
        for k in [
            "ml_steps_state",
            "ml_runtime_logs",
            "ml_runtime_state",
        ]:
            st.session_state.pop(k, None)

        st.session_state.prev_page = st.session_state.current_page

    # --------------------------------
    # Setup page display area
    # --------------------------------
    page_slot = st.empty()
    with page_slot.container():
        if st.session_state.current_page == "project_overview":
            render_overview()
        elif st.session_state.current_page == "pipeline":
            render_pipeline()
        elif st.session_state.current_page == "database":
            render_database()
        elif st.session_state.current_page == "analysis":
            render_analysis()
        elif st.session_state.current_page == "dashboard":
            render_dashboard()
        elif st.session_state.current_page == "machine_learning":
            render_ml()