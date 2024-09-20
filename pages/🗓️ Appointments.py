import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col

st.set_page_config(
    page_title="Appointment Dashboard",
    #page_icon="",
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

df['PERCENTAGE_TO_GOAL'] = df['APPOINTMENTS'] / df['GOAL']

#for index, row in df.iterrows().container(border=True):
   cols1, cols2 = st.columns(1,3)
   with cols1:
        st.image()

    with cols2:
        st.write(f"{row['CLOSER']}")
        st.progress(float(row['PERCENTAGE_TO_GOAL']))