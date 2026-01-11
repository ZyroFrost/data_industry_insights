import streamlit as st

REPORT_ID = "YOUR_REPORT_ID"
GROUP_ID = "YOUR_GROUP_ID"
TENANT_ID = "456a6442-de9f-4a7a-bbf0-64cb0896f258"  

def render_dashboard():
    embed_url = (
        "https://app.powerbi.com/reportEmbed"
        "?reportId=8f68e805-a62f-49da-9cbb-de332ebdcd46"
        "&groupId=me"
        "&autoAuth=true"
        "&ctid=456a6442-de9f-4a7a-bbf0-64cb0896f258"
    )


    st.components.v1.iframe(
        src=embed_url,
        width="100%",
        height=900,
        scrolling=True
    )