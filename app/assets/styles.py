import streamlit as st
import base64
from PIL import Image
import io

# Set global CSS styles
def set_global_css():
    # Set page config phải đặt đầu tiên, nếu nằm sau st nào khác thì sẽ báo lỗi
    st.set_page_config(
        layout="wide",
        page_icon="src/assets/icon.png",
        initial_sidebar_state="expanded")

# Chỉnh màu cho cục bộ toàn app
    bg_color = "#EEF2F6"
    st.markdown(f"""<style>.stApp {{background-color: {bg_color};}}</style>""", unsafe_allow_html=True)

    # chỉnh full màn hình
    st.markdown(
        """
        <style>
        .block-container {
                padding-top: 0rem;
                padding-bottom: 2rem;
                color: black; /* Màu chữ cục bộ container */
            }

        /* Remove default padding of main block (nếu menu nằm main) */
        section[data-testid="stMain"] > div {
            padding: 0 !important;
            max-width: 100% !important;
        }

        /* Loại bỏ margin bottom mặc định của Streamlit */
        [data-testid="stVerticalBlock"] > [style*="flex-direction: column"] > [data-testid="element-container"] {
            margin-bottom: 0 !important;
        }
        </style>
        """,
        unsafe_allow_html=True
    )

    # xóa header, footer
    st.markdown("""
        <style>
            #MainMenu {visibility: hidden;}
            header .stAppHeader {visibility: hidden;}
            footer {visibility: hidden;}
                
        /* Ẩn toàn bộ header trên cùng */
        header[data-testid="stHeader"] {
            display: none;
        }

        /* Đẩy nội dung lên sát trên */
        div[data-testid="stAppViewContainer"] {
            padding-top: 0;
        }
        </style>
    """, unsafe_allow_html=True)

# OPTION MENU CSS
# def option_menu_css():
#     # Tùy chỉnh cho option_menu
#     return {"container": {"padding": "5 !important", "background-color": "#FFFFFF", "border-radius": "15px"},}

def custom_line():
    line_color = "#000000"
    st.markdown(
        f"""
        <hr style="
            margin: 0px 0;
            margin-left: 0.5rem;
            padding-top: 12px;
            border: none;
            border-top: 3px solid {line_color};
            opacity: 0.5;
            width: 99%;
        ">
        """,
        unsafe_allow_html=True
    )

# HORIZONTAL LINE
def custom_line_vertical():
    line_color = "#999696"
    st.markdown(
        f"""
        <div style='height:90vh; border-left:3px solid {line_color}; margin-top:3.4rem;'></div>,
        """,
        unsafe_allow_html=True
    )

# CSS cho container title
def container_title_css():
    bg_color = "#2F4F5F"

    st.html(
        f"""
        <style>
        div.st-key-title_container {{
            background-color: {bg_color};
            border-radius: 0px;
            min-height: 7vh !important;
            width: 100%;

            padding: 0px;
            padding_top: 2px;
            padding-left: 10vh;   /* 👈 THỤT VÀO */
            color: white;           /* 👈 CHỮ TRẮNG */
            font-size: 3.5rem;      /* 👈 SIZE CHÍNH */
            font-weight: 600;

            margin-left: 0rem;
            gap: 0px !important;
        }}
        </style>
        """
    )

# CSS cho container menu
def stylable_container_menu_css():
    bg_color = "#2F4F5F"

    return f"""
        /* CSS cho lớp vỏ container */
        {{
            position: relative;  
            padding: 0rem;
            background-color: {bg_color};
            width: 100%;
            min-height: 93vh;
            display: flex;
            flex-direction: column;
            align-items: stretch;
            margin-top: 0rem;
            padding-top: 0rem;
            margin-bottom: 0rem;
        }}

        /* [QUAN TRỌNG] Nhắm vào lớp layout bên trong để xóa khoảng cách */
        div[data-testid="stVerticalBlock"] {{
            gap: 0px !important;
        }}
        
        /* Đảm bảo các phần tử con không có margin thừa */
        div[data-testid="stVerticalBlock"] > div {{
            margin-bottom: 0px !important;
        }}
    """

def _set_current_page(page_key: str):
    st.session_state.current_page = page_key

# CSS cho icon button (menu nút bên trái)
def icon_button(*, page_key: str, label: str, icon_path: str, current_page: str):
    is_active = page_key == current_page

    # load icon
    img = Image.open(f"{icon_path}")
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    btn_b64 = base64.b64encode(buf.getvalue()).decode()

    icon_size = 37
    text_color = "#e5e7eb"

    bg_idle = "#2F4F5F"
    bg_active = "#2882AC"
    bg_hover = "#c4c19b"
    background_color = bg_active if is_active else bg_idle

    # CSS cho button icon
    st.html(
        f"""
        <style>
        /* 1. ÉP MÀU NỀN CHO NÚT CHÍNH */
        .st-key-nav_{page_key} button {{
            background-color: {background_color} !important;
            border: none !important;
            border-radius: 0 !important;
            height: 100px !important;
            width: 100% !important;
            padding: 4rem 0 !important;
            color: {text_color} !important;
            display: flex !important;
            flex-direction: column !important;
            align-items: center !important;
            justify-content: center !important;
            cursor: pointer;
        }}

         /* 2. Hiệu ứng Hover */
        .st-key-nav_{page_key} button:hover {{
            background-color: {bg_hover} !important;
            color: {text_color} !important;
        }}

         /* 3. Selector chuẩn cho ẢNH bên trong nút */
        /* Phải đi qua thẻ p (paragraph) do Markdown sinh ra */
        .st-key-nav_{page_key} button p img {{
            width: {icon_size}px !important;
            height: {icon_size}px !important;
            background-color: transparent !important;
            object-fit: contain !important;
            margin-bottom: 8px !important; /* Khoảng cách giữa icon và chữ */
        }}

        /* 5. Xử lý viền focus (làm xấu giao diện) */
        .st-key-nav_{page_key} button:focus:not(:active) {{
            border: none !important;
            box-shadow: none !important;
            color: {text_color} !important;
        }}

        .st-key-nav_{page_key} {{
            margin-bottom: 0px !important;
            padding-bottom: 0px !important;
        }}

        /* Đảm bảo div chứa nút không có khoảng cách thừa */
        div[data-testid="stVerticalBlock"] > div:has(.st-key-nav_{page_key}) {{
            gap: 0px !important;
        }}

        /* ICON */
        img[alt="icon"] {{
            max-width: {icon_size}px !important;
            max-height: {icon_size}px !important;
            background-color: {background_color} !important;
            object-fit: contain !important;
            display: block;
            margin: 0 auto;
        }}
        """
    )

    st.button(
        f'![icon](data:image/png;base64,{btn_b64}) {label}',
        key=f"nav_{page_key}",
        width="stretch",
        on_click=_set_current_page,
        args=(page_key,)
    )

# CSS cho container pipeline
def stylable_container_pipeline_css():
    #bg_color = "#CFDDE6"

    """CSS cho logs container với scroll"""
    return """
        {   
            min-height: 90vh !important;
            max-height: 90vh !important;
            overflow-y: hidden !important;
            overflow-x: hidden !important;
            padding: 1rem;
            border-radius: 0px;
        }
        """

def stylable_container_mapping_app_css():
    #bg_color = "#CFDDE6"

    """CSS cho logs container với scroll"""
    return """
        {   
            min-height: 90vh !important;
            max-height: 90vh !important;
            overflow-y: auto !important;
            overflow-x: hidden !important;
            padding: 1rem;
            border-radius: 0px;
        }
        """
    
# CSS cho container pipeline filexstep
def container_pipeline_filexstep_css(sources_list: list[str]):
    bg_color = "#CFDDE6"

    css_rules = []
    for src in sources_list:
        css_rules.append(f"""
        div.st-key-pipeline_container_filexstep_{src} {{
            background-color: {bg_color};
            border: none;
            padding: 0px;
            color: black;
            max-height: 80vh !important;      # ← Giới hạn cao
            overflow-x: auto;
            overflow-y: hidden;
            white-space: nowrap;
        }}
        """)

    st.html(
        f"""
        <style>
        {''.join(css_rules)}
        </style>
        """
    )

# CSS cho container logs với scroll (phần hiển thị terminal)
def stylable_container_logs_css():
    """CSS cho logs container với scroll"""
    return """
        {   
            min-height: 70vh !important;
            max-height: 70vh !important;
            overflow-y: auto !important;
            overflow-x: hidden !important;
            padding: 1rem;
            border-radius: 0px;
        }
        """

def stylable_container_pipeline_monitor_css():
    """CSS cho pipeline container với scroll"""
    return """
        {   
            min-height: 75vh !important;
            max-height: 75vh !important;
            overflow-y: auto !important;
            overflow-x: hidden !important;
            padding: 1rem;
            border-radius: 0px;
        }
        """

def styable_ml_logs_css():
    return """
        {
            min-height: 59vh !important;
            max-height: 59vh !important;
            overflow-y: auto !important;
            overflow-x: hidden !important;
            padding: 0;
            margin: 0;
            border-radius: 0px;
        }
        """

def stylable_ml_container_button_row_css():
    return """
        {
            width: 100%;
            overflow: visible !important;
            padding: 0.5rem 0;
        }
        """

def stylable_container_overview_css():
    #bg_color = "#CFDDE6"

    """CSS cho logs container với scroll"""
    return """
        {   
            min-height: 90vh !important;
            max-height: 90vh !important;
            overflow-y: auto !important;
            overflow-x: hidden !important;
            padding: 1rem;
            border-radius: 0px;
        }
        """