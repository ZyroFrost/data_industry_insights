import streamlit as st
from streamlit_option_menu import option_menu
from streamlit_extras.stylable_container import stylable_container

from assets.styles import set_global_css, option_menu_css, container_sidebar_css
from pages.s1_pipeline import render_pipeline

from pathlib import Path
import subprocess


# =====================================================
# CONFIG
# =====================================================

set_global_css()

# =====================================================
# PAGE RENDER FUNCTIONS
# =====================================================

def render_database():
    st.subheader("🗄 Database")
    st.markdown("""
    Inspect database integrity and table-level statistics
    after pipeline ingestion.
    """)

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("### Tables Overview")
        st.empty()

    with col2:
        st.markdown("### Integrity Checks")
        st.empty()


def render_analysis():
    st.subheader("📊 Analysis (EDA)")
    st.markdown("""
    Exploratory Data Analysis and statistical inspection.
    This layer is used to **understand the data**, not for final reporting.
    """)

    tabs = st.tabs([
        "Overview",
        "Distribution",
        "Correlation",
        "PCA"
    ])

    with tabs[0]:
        st.markdown("### Dataset Overview")
        st.empty()

    with tabs[1]:
        st.markdown("### Distribution Analysis")
        st.empty()

    with tabs[2]:
        st.markdown("### Correlation Analysis")
        st.empty()

    with tabs[3]:
        st.markdown("### PCA / Dimensionality Reduction")
        st.empty()


def render_dashboard():
    st.subheader("📈 Dashboard (BI)")
    st.markdown("""
    Final reporting and business-facing dashboards.
    This section typically links to **external BI tools (Power BI)**.
    """)

    st.link_button(
        "Open Power BI Dashboard",
        url="https://powerbi.microsoft.com/"
    )

    st.caption("Dashboards are maintained outside Streamlit.")


# =====================================================
# TOP MENU (HORIZONTAL)
# =====================================================

current_page = option_menu(
    menu_title="",
    options=["Pipeline", "Database", "Analysis", "Dashboard"],
    icons=["diagram-3", "database", "bezier2", "bar-chart-line"],
    menu_icon="cast",
    default_index=0,
    orientation="horizontal",
    styles=option_menu_css()
)

st.session_state["current_page"] = current_page

# =====================================================
# MAIN CONTENT CONTAINER
# =====================================================

def render_folder_tree(
    base_path: Path,
    active_path: Path | None = None,
    prefix: str = "",
    is_last: bool = True
):
    """
    Render ASCII-style folder tree.
    Only expands the branch that contains active_path.
    """

    if not base_path.exists():
        return

    # xác định ký tự tree
    connector = "└─ " if is_last else "├─ "
    line_prefix = prefix + connector

    # kiểm tra có phải nhánh active không
    is_active_branch = False
    if active_path:
        try:
            is_active_branch = active_path.resolve().is_relative_to(base_path.resolve())
        except AttributeError:
            is_active_branch = str(active_path.resolve()).startswith(str(base_path.resolve()))

    # ---- FOLDER ROOT ----
    st.markdown(f"{line_prefix}📁 {base_path.name}")

    # nếu không phải nhánh active → KHÔNG bung
    if not is_active_branch:
        return

    # chuẩn bị prefix cho level tiếp theo
    child_prefix = prefix + ("   " if is_last else "│  ")

    items = sorted(base_path.iterdir())
    for idx, item in enumerate(items):
        last_item = idx == len(items) - 1

        if item.is_dir():
            render_folder_tree(
                item,
                active_path=active_path,
                prefix=child_prefix,
                is_last=last_item
            )
        else:
            file_connector = "└─ " if last_item else "├─ "
            file_line = child_prefix + file_connector

            # ---- FILE LEAF ----
            if active_path and item.resolve() == active_path.resolve():
                with st.spinner(f"Running {item.name}"):
                    st.markdown(f"{file_line}📄 **{item.name}**")
            else:
                st.markdown(f"{file_line}📄 {item.name}")

def run_pipeline_command(cmd: list[str]):
    output_box = st.empty()
    logs = []

    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1
    )

    for line in process.stdout:
        logs.append(line.rstrip())
        output_box.code("\n".join(logs), language="bash")

    process.wait()

with st.sidebar:
    st.title("📁 Project Tree")
    active_path = Path("data/data_processing/data_extracted/jobs.csv")

    with stylable_container(key="folder_tree", css_styles=container_sidebar_css()):
        render_folder_tree(
            base_path=Path("data"),
            active_path=active_path
        )

with st.container():
    if current_page == "Pipeline":
        render_pipeline()

    elif current_page == "Database":
        render_database()

    elif current_page == "Analysis":
        render_analysis()

    elif current_page == "Dashboard":
        render_dashboard()