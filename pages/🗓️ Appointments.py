import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col

st.set_page_config(
    page_title="Appointment Dashboard",
    layout="wide",
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
    SELECT GOAL, MARKET, TYPE, RANK, ACTIVE, CLOSER_ID, PROFILE_PICTURE, CONCAT(SPLIT_PART(NAME, ' ', 1), ' ', LEFT(SPLIT_PART(NAME, ' ', 2),1), '.') NAME
    FROM raw.snowflake.lm_appointments
"""

appts_query = """
    SELECT closer, closer_id, COUNT(first_scheduled_close_start_at) APPOINTMENTS
    FROM operational.salesforce.vw_opportunities
    WHERE sales_channel = 'Web To Home' 
    AND WEEK(first_scheduled_close_start_at) = WEEK(CURRENT_DATE) 
    AND YEAR(first_scheduled_close_start_at) = YEAR(CURRENT_DATE)
    GROUP BY closer, closer_id
"""

df_goals = run_query(goals_query)
df_appts = run_query(appts_query)

df = pd.merge(df_goals, df_appts, left_on='CLOSER_ID', right_on='CLOSER_ID', how='left')

df["APPOINTMENTS"] = df["APPOINTMENTS"].fillna(0).astype(int)
df['PROFILE_PICTURE'] = df['PROFILE_PICTURE'].fillna('https://i.ibb.co/ZNK5xmN/pdycc8-1-removebg-preview.png').astype(str)

# Calculate PERCENTAGE_TO_GOAL, handling division by zero
df['PERCENTAGE_TO_GOAL'] = np.where(
    df['GOAL'] == 0, 100,  # If GOAL is 0, set percentage to 100
    np.minimum((df['APPOINTMENTS'] / df['GOAL']) * 100, 100)  # Otherwise, calculate the percentage and cap it at 100
)

# Inject custom CSS for the layout and remove the space at the top
st.markdown("""
    <style>
    .css-18e3th9 {
        padding-top: 0 !important;  /* Remove the space at the top */
    }
    .card {
        background-color: #1e1e1e;
        padding: 10px;
        border-radius: 10px;
        margin-bottom: 5px;
        color: white;
        position: relative;
    }
    .profile-section {
        display: flex;
        align-items: center;
        margin-bottom: 8px;
    }
    .profile-pic {
        border-radius: 50%;
        width: 28px;
        height: 28px;
        margin-right: 15px;
    }
    .name {
        font-size: 18px;
        font-weight: bold;
    }
    .appointments {
        font-size: 16px;
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
        right: 5px;
        top: 50%;
        transform: translateY(-50%);
        font-size: 16px;
        color: white;
        font-weight: bold;
    }
    </style>
""", unsafe_allow_html=True)


st.sidebar.title("Filters")

# Add the Market multiselect filter below the "Filters" title
if 'selected_markets' not in st.session_state:
    st.session_state['selected_markets'] = ['All Markets']  # Default value

selected_markets = st.sidebar.multiselect(
    'Market', 
    ['All Markets'] + sorted(df['MARKET'].unique()),
    default=st.session_state['selected_markets'],
    key='market_multiselect'
)

# Save the selected market filters to session state
st.session_state['selected_markets'] = selected_markets

# Apply the market filter to the DataFrame
if 'All Markets' not in selected_markets:
    df = df[df['MARKET'].isin(selected_markets)]

# Define the number of cards per row (e.g., 3, 4, 6)
cards_per_row = 6

# Group by MARKET and loop over each group
for idx, (market, group_df) in enumerate(df.groupby('MARKET')):
    # Add a header for each market group
    if idx > 0:  # Only add divider after the first market group
        st.divider()
    st.header(market)
    
    # Break the group into chunks (rows of cards)
    for i in range(0, len(group_df), cards_per_row):
        row_df = group_df.iloc[i:i + cards_per_row]  # Get a chunk of cards (one row)

        # Create columns for this row
        cols = st.columns(cards_per_row)

        # Loop through each card in the row and assign it to a column
        for col, (_, row) in zip(cols, row_df.iterrows()):
            percentage_to_goal = row['PERCENTAGE_TO_GOAL']
            goal_value = row['GOAL']
            appointments_value = row['APPOINTMENTS']

            with col:
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