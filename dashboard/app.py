import streamlit as st 
import datetime
from api_client import get_trending, DashboardError

st.set_page_config(
    page_title="DevPulse",
    initial_sidebar_state="expanded",
)

st.title(
    "Welcome to DevPulse"
)

st.sidebar.title("Date Selection")

user_date_input = st.sidebar.date_input(
    "Select a date: ",
    value=datetime.date(2024, 1, 1)
)

user_limit_input = st.sidebar.slider(
    "Limit: ",
    min_value=1, 
    max_value=100,
    value=1
)
try:
    
    rows = get_trending(
        day=user_date_input, 
        limit=user_limit_input
    )
    results = rows["results"]
    if not results:
        st.info(f"No trending data for {user_date_input}")
        
    else:
    
        st.dataframe(results)

        st.bar_chart(
            data=results,
            x="repo_name",
            y="stars"
        )

except DashboardError as e:
    st.error(str(e))
