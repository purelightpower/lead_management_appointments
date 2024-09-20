import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col

st.set_page_config(
    page_title="Appointment Dashboard",
    layout = "wide",
    initial_sidebar_state="collapsed"
)

st.logo("https://i.ibb.co/bbH9pgH/Purelight-Logo.webp")

# Function to create a Snowflake session
def create_snowflake_session():
    connection_parameters = {
        "account": st.secrets["snowflake"]["account"],
        "user": st.secrets["snowflake"]["user"],
        "password": st.secrets["snowflake"]["password"],
        "role": st.secrets["snowflake"]["role"],
        "warehouse": st.secrets["snowflake"]["warehouse"],
        "database": st.secrets["snowflake"]["database"],
        "schema": st.secrets["snowflake"]["schema"],
    }
    return Session.builder.configs(connection_parameters).create()

# Initialize Snowpark session
session = create_snowflake_session()

# Function to execute a SQL query and return a pandas DataFrame
def run_query(query):
    return session.sql(query).to_pandas()

goals_query = """
    SELECT *
    FROM raw.snowflake.lm_appointments
"""

appts_query = """
    SELECT closer, closer_id, COUNT(first_scheduled_close_start_at) APPOINTMENTS
    FROM operational.salesforce.vw_opportunities
    WHERE sales_channel = 'Web To Home' AND WEEK(first_scheduled_close_start_at) = WEEK(CURRENT_DATE) AND YEAR(first_scheduled_close_start_at) = YEAR(CURRENT_DATE)
    GROUP BY closer, closer_id
"""

df_goals = run_query(goals_query)

df_appts = run_query(appts_query)

df = pd.merge(df_goals, df_appts, left_on= 'CLOSER_ID', right_on = 'CLOSER_ID', how = 'left')

df['PROFILE_PICTURE'] = df['PROFILE_PICTURE'].fillna('https://i.ibb.co/ZNK5xmN/pdycc8-1-removebg-preview.png').astype(str)

df['PERCENTAGE_TO_GOAL'] = df['APPOINTMENTS'] / df['GOAL']

# Inject custom CSS for the layout and styling
st.markdown("""
    <style>
    .card {
        background-color: #1e1e1e;
        padding: 20px;
        border-radius: 10px;
        margin-bottom: 10px;
        color: white;
        position: relative;
    }
    .profile-section {
        display: flex;
        align-items: center;
        margin-bottom: 10px;
    }
    .profile-pic {
        border-radius: 50%;
        width: 50px;
        height: 50px;
        margin-right: 15px;
    }
    .name {
        font-size: 22px;
        font-weight: bold;
    }
    .appointments {
        font-size: 24px;
        margin-bottom: 10px;
        color: white;
    }
    .progress-bar {
        background-color: #333;
        border-radius: 25px;
        width: 100%;
        height: 20px;
        position: relative;
        margin-bottom: 10px;
    }
    .progress-bar-fill {
        background-color: #FF6347; /* Tomato color for the progress bar */
        height: 100%;
        border-radius: 25px;
    }
    .goal {
        position: absolute;
        right: 15px;
        top: 50%;
        transform: translateY(-50%);
        font-size: 18px;
        color: white;
        font-weight: bold;
    }
    </style>
""", unsafe_allow_html=True)

# Loop through each row in the DataFrame
for index, row in df.iterrows():
    percentage_to_goal = row['PERCENTAGE_TO_GOAL']
    goal_value = row['GOAL']
    appointments_value = row['APPOINTMENTS']
    
    st.markdown(f"""
        <div class="card">
            <div class="profile-section">
                <img src="{row['PROFILE_PICTURE']}" class="profile-pic" alt="Profile Picture">
                <div class="name">{row['NAME']}</div>
            </div>
            <div class="appointments">{appointments_value}</div>
            <div class="progress-bar">
                <div class="progress-bar-fill" style="width: {percentage_to_goal}%;"></div>
                <div class="goal">{goal_value}</div>
            </div>
        </div>
    """, unsafe_allow_html=True)