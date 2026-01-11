import streamlit as st
from streamlit_extras.stylable_container import stylable_container
from assets.styles import stylable_container_overview_css

import base64
from PIL import Image
import io, requests

# =====================================================
# INIT SESSION STATE (BẮT BUỘC)
# =====================================================
if "lang" not in st.session_state:
    st.session_state.lang = "vi"


# =====================================================
# CSS: LANGUAGE SWITCHER (FIXED TOP-LEFT)
# =====================================================
def language_switcher_css():
    st.markdown(
        """
        <style>
        .lang-switcher {
            position: fixed;
            top: 12px;
            left: 12px;
            z-index: 9999;
            display: flex;
            gap: 6px;
        }

        .lang-btn button {
            background-color: #2D7697;
            color: white;
            border: none;
            border-radius: 6px;
            padding: 4px 10px;
            font-size: 12px;
            font-weight: 600;
            cursor: pointer;
        }

        .lang-btn-active button {
            background-color: #165674;
            border: 1px solid white;
        }

        .lang-btn button:hover {
            background-color: #358ff5;
        }
        </style>
        """,
        unsafe_allow_html=True
    )


# =====================================================
# LANGUAGE SWITCHER COMPONENT
# =====================================================
def render_language_switcher():
    st.markdown('<div class="lang-switcher">', unsafe_allow_html=True)

    c1, c2 = st.columns(2, gap="small", width="stretch")

    with c1:
        cls = "lang-btn-active" if st.session_state.lang == "vi" else "lang-btn"
        st.markdown(f'<div class="{cls}">', unsafe_allow_html=True)
        if st.button("VI", key="lang_vi"):
            st.session_state.lang = "vi"
            st.rerun()
        st.markdown("</div>", unsafe_allow_html=True)

    with c2:
        cls = "lang-btn-active" if st.session_state.lang == "en" else "lang-btn"
        st.markdown(f'<div class="{cls}">', unsafe_allow_html=True)
        if st.button("EN", key="lang_en"):
            st.session_state.lang = "en"
            st.rerun()
        st.markdown("</div>", unsafe_allow_html=True)

    st.markdown("</div>", unsafe_allow_html=True)


# =====================================================
# TEXT CONTENT (VI / EN)
# =====================================================
TEXT = {
    "title": {
        "vi": "📘 Tổng quan đồ án",
        "en": "📘 Project Overview",
    },
    "intro": {
        "vi": """
        **Data Industry Insights** là đồ án phân tích dữ liệu tập trung vào việc khảo sát,
        chuẩn hóa và phân tích **thị trường việc làm trong lĩnh vực Data / AI / Machine Learning**
        trên phạm vi toàn cầu giai đoạn **2020–2025**.

        Dự án được xây dựng như một **hệ thống dữ liệu hoàn chỉnh**, bao gồm data pipeline,
        cơ sở dữ liệu quan hệ, phân tích thống kê, trực quan hóa và Machine Learning,
        nhằm khám phá xu hướng tuyển dụng, nhu cầu kỹ năng và mức lương trong ngành Data.
        """,
        "en": """
        **Data Industry Insights** is a data analytics project focused on analyzing,
        normalizing, and exploring the **global Data / AI / Machine Learning job market**
        from **2020 to 2025**.

        The project is designed as a **full data system**, including a data processing pipeline,
        relational database, statistical analysis, visualization, and Machine Learning techniques
        to uncover hiring trends, skill demand, and salary structures in the Data industry.
        """,
    },
    "objectives_title": {
        "vi": "🎯 Mục tiêu đồ án",
        "en": "🎯 Project Objectives",
    },
    "objectives": {
        "vi": """
        - Thu thập và hợp nhất dữ liệu tuyển dụng Data từ nhiều nguồn khác nhau
          (API, public datasets, government sources).
        - Chuẩn hóa dữ liệu theo **Entity–Relationship Diagram (ERD)** thống nhất.
        - Phân tích nhu cầu tuyển dụng, kỹ năng, vai trò và mức lương theo quốc gia và thời gian.
        - Áp dụng các kỹ thuật phân tích nâng cao như **PCA, Clustering và Regression**.
        - Trực quan hóa insight thông qua dashboard tương tác, hỗ trợ ra quyết định.
        """,
        "en": """
        - Collect and integrate Data job postings from multiple sources
          (APIs, public datasets, government sources).
        - Normalize data using a unified **Entity–Relationship Diagram (ERD)**.
        - Analyze job demand, skills, roles, and salaries by country and over time.
        - Apply advanced analytics such as **PCA, Clustering, and Regression**.
        - Visualize insights through interactive dashboards for decision-making.
        """,
    },
    "components_title": {
        "vi": "🧩 Các thành phần chính của hệ thống",
        "en": "🧩 System Components",
    },
    "components": {
        "vi": """
        **Pipeline**  
        Quản lý toàn bộ data pipeline từ raw data đến dữ liệu đã chuẩn hóa,
        đảm bảo reproducible, traceable và kiểm soát lỗi.

        **Database**  
        Pipeline upload và lưu trữ dữ liệu theo mô hình ERD gồm job postings, companies, skills,
        locations và các bảng quan hệ nhiều-nhiều.

        **Analysis**  
        Thực hiện EDA, Correlation, ANOVA, PCA và K-Means để khám phá cấu trúc thị trường.

        **Dashboard**  
        Trực quan hóa các insight chính, hỗ trợ phân tích và ra quyết định.

        **Machine Learning**  
        Đánh giá khả năng dự báo và phân tích giới hạn của các mô hình truyền thống
        trong bối cảnh thị trường dữ liệu phức tạp.
        """,
        "en": """
        **Pipeline**  
        Manages the entire data processing pipeline from raw data to standardized outputs,
        ensuring reproducibility, traceability, and error control.

        **Database**  
        Stores data using an ERD-based relational schema including job postings,
        companies, skills, locations, and many-to-many relationships.

        **Analysis**  
        Performs EDA, Correlation, ANOVA, PCA, and K-Means to explore market structure.

        **Dashboard**  
        Visualizes key insights to support analysis and decision-making.

        **Machine Learning**  
        Evaluates predictive capabilities and highlights the limitations of traditional models
        in a complex global job market.
        """,
    },
    "scope_title": {
        "vi": "📌 Phạm vi & giới hạn",
        "en": "📌 Scope & Limitations",
    },
    "scope": {
        "vi": """
        - Dữ liệu phản ánh nhu cầu tuyển dụng chính thức, không đại diện cho toàn bộ thị trường lao động.
        - Một số trường dữ liệu (lương, kinh nghiệm) có thể bị thiếu do đặc thù nguồn.
        - Phân tích mang tính mô tả và khám phá, không nhằm dự báo chính xác tuyệt đối.
        """,
        "en": """
        - The data reflects formal hiring demand and does not represent the entire labor market.
        - Some attributes (salary, experience) may be missing due to source limitations.
        - The analysis is descriptive and exploratory, not intended for absolute prediction.
        """,
    },
}


# =====================================================
# MAIN RENDER FUNCTION
# =====================================================
def render_project_overview():
    lang = st.session_state.get("lang", "vi")

    cTitle, cBtn = st.columns([9, 1], gap="small", width="stretch", vertical_alignment="bottom")
    cTitle.markdown(f"## {TEXT['title'][lang]}")

    with cBtn:
        cLeft, cRight = st.columns([1, 1], gap="small")

        # ---- VI BUTTON ----
        with cLeft:
            url = "https://flagcdn.com/w40/vn.png"
            img_bytes = requests.get(url).content
            img = Image.open(io.BytesIO(img_bytes))

            buf = io.BytesIO()
            img.save(buf, format="PNG")
            btn_b64 = base64.b64encode(buf.getvalue()).decode()

            st.button(
                f'![icon](data:image/png;base64,{btn_b64})',
                key="nav_vi",
                use_container_width=True,
                on_click=lambda: st.session_state.update({"lang": "vi"})
            )

        # ---- EN BUTTON ----
        with cRight:
            url = "https://flagcdn.com/w40/us.png"
            img_bytes = requests.get(url).content
            img = Image.open(io.BytesIO(img_bytes))

            buf = io.BytesIO()
            img.save(buf, format="PNG")
            btn_b64 = base64.b64encode(buf.getvalue()).decode()

            st.button(
                f'![icon](data:image/png;base64,{btn_b64})',
                key="nav_en",
                use_container_width=True,
                on_click=lambda: st.session_state.update({"lang": "en"})
            )

    st.divider()

    st.markdown(TEXT["intro"][lang])

    st.divider()

    st.markdown(f"### {TEXT['objectives_title'][lang]}")
    st.markdown(TEXT["objectives"][lang])

    st.divider()

    st.markdown(f"### {TEXT['components_title'][lang]}")
    st.markdown(TEXT["components"][lang])

    st.divider()

    st.markdown(f"### {TEXT['scope_title'][lang]}")
    st.markdown(TEXT["scope"][lang])

    st.markdown("---")
    st.markdown(
        "*Data Industry Insights – A data-driven view of the global Data job market.*"
        if lang == "en"
        else
        "*Data Industry Insights – Góc nhìn dữ liệu về thị trường việc làm Data toàn cầu.*"
    )

def render_overview():
    with stylable_container(key="overview_container", css_styles=stylable_container_overview_css()):
        render_project_overview()