import streamlit as st

def set_global_css():
    # Set page config phải đặt đầu tiên, nếu nằm sau st nào khác thì sẽ báo lỗi
    st.set_page_config(
        layout="wide",
        page_icon="src/assets/icon.png",
        initial_sidebar_state="expanded")

# Chỉnh màu cho cục bộ toàn app (màu xám) #EEEEEE = xám nhẹ
    st.markdown("""<style>.stApp {background-color: white;}</style>""", unsafe_allow_html=True)

    # chỉnh rộng màn hình
    st.markdown("""
        <style>
            .block-container {
                padding-top: 0rem;
                padding-bottom: 2rem;
                padding-left: 2rem;
                padding-right: 2rem;
            }
        </style>
    """, unsafe_allow_html=True)

    # xóa header, footer
    st.markdown("""
        <style>
            #MainMenu {visibility: hidden;}
            header .stAppHeader {visibility: hidden;}
            footer {visibility: hidden;}
        </style>
    """, unsafe_allow_html=True)

def option_menu_css():
    # Tùy chỉnh cho option_menu
    return {"container": {"padding": "5 !important", "background-color": "#FFFFFF", "border-radius": "15px"},}

def container_sidebar_css():
    body_style = """
        {   
            min-height: 69vh !important;
            max-height: 69vh !important;
            overflow-y: auto; /* Nếu nội dung dài thì cuộn bên trong khung */
            overflow-x: auto;
        }

        /* QUAN TRỌNG: chặn wrap của markdown, để overflow-x hoạt động */
        p, span, div { 
            white-space: nowrap;
        }
    """
    return body_style