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
df_goals = run_query(goals_query)

st.dataframe(df_goals)