import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col

st.set_page_config(
    page_title="Appointment Dashboard",
    # page_icon="",
    initial_sidebar_state="collapsed"
)

st.sidebar.image("https://i.ibb.co/bbH9pgH/Purelight-Logo.webp", use_column_width=True)

hide_streamlit_style = """
    <style>
    #MainMenu {visibility: hidden;}  /* Hides the hamburger menu */
    footer {visibility: hidden;}  /* Hides the footer */
    header {visibility: hidden;}  /* Hides the header where Fork/GitHub options might be */
    
    /* Reduces padding from the main content container */
    .css-10trblm {padding-top: 0px; padding-bottom: 0px;}  /* Content padding */
    
    /* Reduces top padding for the main app layout */
    .css-1d391kg {padding-top: 0px !important;}  /* Top padding for content */
    
    </style>
"""
st.markdown(hide_streamlit_style, unsafe_allow_html=True)


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

# Query to get full names of 'Closer' users
users_query = """
    SELECT DISTINCT FULL_NAME, SALESFORCE_ID
    FROM operational.airtable.vw_users 
    WHERE role_type = 'Closer' AND term_date IS NULL
"""
df_users = run_query(users_query)

# Get current targets
current_targets_query = """
    SELECT * FROM analytics.ad_hoc.lm_appts_test
"""
current_targets = run_query(current_targets_query)

# Get profile pictures
profile_picture_query = """
    SELECT FULL_NAME, PROFILE_PICTURE
    FROM operational.airtable.vw_users
"""
profile_picture = run_query(profile_picture_query)

# Get appointments
appointments_query = """
    SELECT * FROM raw.snowflake.lm_appointments
"""
appointments = run_query(appointments_query)

# Merge the dataframes on the full name
merged_df = df_users.merge(
    current_targets, left_on='FULL_NAME', right_on='CLOSER', how='left'
).merge(
    appointments, left_on='FULL_NAME', right_on='NAME', how='left'
).merge(
    profile_picture, on='FULL_NAME', how='left'
)

# Continue with your existing data processing...

# Example continuation:
# (Ensure column names match your DataFrame)
merged_df = merged_df.rename(columns={
    'FULL_NAME': 'FULL_NAME',
    'PROFILE_PICTURE': 'PROFILE_PICTURE'
})

# Drop redundant columns
if 'CLOSER' in merged_df.columns:
    merged_df = merged_df.drop(columns=['CLOSER'])

# Fill NaN values and ensure correct data types
merged_df['TARGET'] = merged_df['TARGET'].fillna(0).astype(int)
merged_df['MARKET'] = merged_df['MARKET'].fillna('No Market').astype(str)
merged_df['GOAL'] = merged_df['GOAL'].fillna(0).astype(int)
merged_df['RANK'] = merged_df['RANK'].fillna(100).astype(int)
merged_df['TYPE'] = merged_df['TYPE'].fillna('None').astype(str)
merged_df['PROFILE_PICTURE'] = merged_df['PROFILE_PICTURE'].fillna('https://i.ibb.co/ZNK5xmN/pdycc8-1-removebg-preview.png').astype(str)

# Convert 'ACTIVE' column to boolean
merged_df['ACTIVE'] = merged_df['ACTIVE'].fillna('No').astype(str)
merged_df['ACTIVE'] = merged_df['ACTIVE'].str.strip().str.lower().map({'yes': True, 'no': False})
merged_df['ACTIVE'] = merged_df['ACTIVE'].fillna(False)  # Default to False if any other value

# Ensure 'TYPE' column has valid options
valid_types = ['None', '🏃 Field Marketing', '🏠 Web To Home']
merged_df['TYPE'] = merged_df['TYPE'].apply(lambda x: x if x in valid_types else 'None')

# Prepare the dataframe for editing
edit_df = merged_df[['PROFILE_PICTURE', 'FULL_NAME', 'MARKET', 'TYPE', 'ACTIVE', 'GOAL', 'RANK', 'SALESFORCE_ID']].copy()

# Continue with your Streamlit app...

# Display the editable dataframe
st.write("## 🎯 Edit Closer Targets")

# Modify option lists to include 'All'
def get_market_options(filtered_df):
    return ['All Markets'] + sorted(filtered_df['MARKET'].unique())

def get_closer_options(filtered_df):
    return ['All Closers'] + sorted(filtered_df['FULL_NAME'].unique())

def get_type_options(filtered_df):
    return ['All Channels'] + sorted(filtered_df['TYPE'].unique())

# Initialize with the full dataframe
filtered_edit_df = edit_df.copy()

# Create columns for the filters
cols1, cols2, cols3 = st.columns(3)

# First filter: Market
with cols1:
    market_input = st.selectbox('Select Market', get_market_options(filtered_edit_df), index=0, key='market_select')

# Filter by market if not 'All Markets'
if market_input != 'All Markets':
    filtered_edit_df = filtered_edit_df[filtered_edit_df['MARKET'] == market_input]

# Second filter: Closer
with cols2:
    closer_input = st.selectbox('Select Closer', get_closer_options(filtered_edit_df), index=0, key='closer_select')

# Filter by closer if not 'All Closers'
if closer_input != 'All Closers':
    filtered_edit_df = filtered_edit_df[filtered_edit_df['FULL_NAME'] == closer_input]

# Third filter: Type
with cols3:
    type_input = st.selectbox('Select Type', get_type_options(filtered_edit_df), index=0, key='type_select')

# Filter by type if not 'All Channels'
if type_input != 'All Channels':
    filtered_edit_df = filtered_edit_df[filtered_edit_df['TYPE'] == type_input]

# Sort the filtered DataFrame by 'FULL_NAME'
filtered_edit_df = filtered_edit_df.sort_values(by='FULL_NAME')

# Wrap the data editor and save button in a form
with st.form('editor_form'):
    # Configure the data editor with column configurations
    edited_df = st.data_editor(
        filtered_edit_df,
        column_order=['PROFILE_PICTURE', 'FULL_NAME', 'MARKET', 'TYPE', 'ACTIVE', 'GOAL', 'RANK'],
        disabled={'FULL_NAME': True, 'PROFILE_PICTURE': True},
        hide_index=True,
        use_container_width=True,
        column_config={
            'PROFILE_PICTURE': st.column_config.ImageColumn(
                label=' '
            ),
            'ACTIVE': st.column_config.CheckboxColumn(
                'Active',
                help="Check if the closer is active",
                default=False
            ),
            'FULL_NAME': st.column_config.TextColumn(
                'Name'
            ),
            'MARKET': st.column_config.TextColumn(
                'Market'
            ),
            'GOAL': st.column_config.NumberColumn(
                'Goal'
            ),
            'RANK': st.column_config.NumberColumn(
                'Rank'
            ),
            'TYPE': st.column_config.SelectboxColumn(
                'Type',
                options=valid_types,
                help="Select the type of marketing",
                required=True
            ),
        }
    )
    
    # Add a submit button within the form
    submitted = st.form_submit_button('Save changes')

# Process the form submission
if submitted:
    # Get the original data for the filtered rows
    original_filtered_df = edit_df.loc[edited_df.index]
    
    # Find the rows where any columns have changed
    changes = edited_df.compare(original_filtered_df)
    
    if changes.empty:
        st.info("No changes detected.")
    else:
        # Iterate over the changed rows
        for idx in changes.index.unique():
            row = edited_df.loc[idx]
            full_name = row['FULL_NAME']
            closer_id = row['SALESFORCE_ID']
            new_goal = int(row['GOAL'])
            new_rank = int(row['RANK'])
            new_active = row['ACTIVE']
            new_type = row['TYPE']
            new_market = row['MARKET']

            # Convert boolean to 'Yes'/'No' for storage if needed
            active_str = 'Yes' if new_active else 'No'

            # Get current timestamp
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            # Build the UPSERT SQL query (update or insert)
            query = f"""
            MERGE INTO raw.snowflake.lm_appointments AS target
            USING (SELECT '{full_name}' AS NAME) AS source
            ON target.NAME = source.NAME
            WHEN MATCHED THEN
                UPDATE SET
                    GOAL = {new_goal},
                    RANK = {new_rank},
                    ACTIVE = '{active_str}',
                    TYPE = '{new_type}',
                    MARKET = '{new_market}',
                    TIMESTAMP = '{timestamp}'
            WHEN NOT MATCHED THEN
                INSERT (CLOSER_ID, NAME, GOAL, RANK, ACTIVE, TYPE, MARKET, TIMESTAMP)
                VALUES ('{closer_id}', '{full_name}', {new_goal}, {new_rank}, '{active_str}', '{new_type}', '{new_market}', '{timestamp}');
            """
            try:
                session.sql(query).collect()
                st.success(f"Saved changes for {full_name}")
            except Exception as e:
                st.error(f"Error saving changes for {full_name}: {str(e)}")