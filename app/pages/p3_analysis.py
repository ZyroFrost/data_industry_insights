import streamlit as st
import nbformat
from nbconvert import HTMLExporter
import sys
from pathlib import Path

# =====================================================
# PATH FIX
# =====================================================
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# =====================================================
# NOTEBOOK PATH
# =====================================================
DATA_DIR = ROOT / "analysis"
NB_PATH = DATA_DIR / "s4_3_analysis_EDA_PCA_500k.ipynb"

def render_analysis():

    
    # =====================================================
    # CHECK NOTEBOOK EXISTS
    # =====================================================
    if not NB_PATH.exists():
        st.error(f"Notebook not found: {NB_PATH}")
        return

    # =====================================================
    # LOAD NOTEBOOK
    # =====================================================
    with open(NB_PATH, encoding="utf-8") as f:
        nb = nbformat.read(f, as_version=4)

    # =====================================================
    # EXPORT TO HTML
    # =====================================================
    html_exporter = HTMLExporter()
    html_exporter.exclude_input_prompt = True
    html_exporter.exclude_output_prompt = True

    body, _ = html_exporter.from_notebook_node(nb)

    # =====================================================
    # RENDER IN STREAMLIT
    # =====================================================
    st.components.v1.html(
        body,
        height=900,
        scrolling=True
    )