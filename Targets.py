import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col

st.set_page_config(
    page_title="Appointment Dashboard",
    initial_sidebar_state="collapsed"
)

st.logo("https://i.ibb.co/bbH9pgH/Purelight-Logo.webp")

hide_streamlit_style = """
    <style>
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header {visibility: hidden;}
    .css-10trblm {padding-top: 0px; padding-bottom: 0px;}
    .css-1d391kg {padding-top: 0px !important;}
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

# Cache functions to avoid redundant queries
def get_users():
    users_query = """
        SELECT DISTINCT FULL_NAME, SALESFORCE_ID
        FROM operational.airtable.vw_users 
        WHERE role_type = 'Closer' AND term_date IS NULL
    """
    return session.sql(users_query).to_pandas()

def get_profile_pictures():
    profile_picture_query = """
        SELECT FULL_NAME, PROFILE_PICTURE
        FROM operational.airtable.vw_users
    """
    return session.sql(profile_picture_query).to_pandas()

def get_appointments():
    appointments_query = """
        SELECT * FROM raw.snowflake.lm_appointments
    """
    return session.sql(appointments_query).to_pandas()

def get_current_targets():
    current_targets_query = """
        SELECT * FROM analytics.ad_hoc.lm_appts_test
    """
    return session.sql(current_targets_query).to_pandas()

# Load data
df_users = get_users()
profile_picture = get_profile_pictures()
appointments = get_appointments()
current_targets = get_current_targets()

# Merge the dataframes on the full name
merged_df = df_users.merge(
    current_targets, left_on='FULL_NAME', right_on='CLOSER', how='left'
).merge(
    appointments, left_on='FULL_NAME', right_on='NAME', how='left'
).merge(
    profile_picture, on='FULL_NAME', how='left'
)

# Rename and drop columns as needed
merged_df = merged_df.rename(columns={
    'FULL_NAME': 'FULL_NAME',
    'PROFILE_PICTURE_y': 'PROFILE_PICTURE'
})

if 'CLOSER' in merged_df.columns:
    merged_df = merged_df.drop(columns=['CLOSER'])

# Fill NaN values and ensure correct data types
merged_df['TARGET'] = merged_df['TARGET'].fillna(0).astype(int)
merged_df['MARKET'] = merged_df['MARKET'].fillna('No Market').astype(str)
merged_df['GOAL'] = merged_df['GOAL'].fillna(0).astype(int)
merged_df['RANK'] = merged_df['RANK'].fillna(100).astype(int)
merged_df['FM_GOAL'] = merged_df['FM_GOAL'].fillna(0).astype(int)
merged_df['FM_RANK'] = merged_df['FM_RANK'].fillna(100).astype(int)
merged_df['TYPE'] = merged_df['TYPE'].fillna('🏠🏃 Hybrid').astype(str)
merged_df['PROFILE_PICTURE'] = merged_df['PROFILE_PICTURE'].fillna('https://i.ibb.co/ZNK5xmN/pdycc8-1-removebg-preview.png').astype(str)

# Convert 'ACTIVE' column to boolean
merged_df['ACTIVE'] = merged_df['ACTIVE'].fillna('No').astype(str)
merged_df['ACTIVE'] = merged_df['ACTIVE'].str.strip().str.lower().map({'yes': True, 'no': False})
merged_df['ACTIVE'] = merged_df['ACTIVE'].fillna(False)

# Ensure 'TYPE' column has valid options
valid_types = ['🏠🏃 Hybrid', '🏃 Field Marketing', '🏠 Web To Home']
merged_df['TYPE'] = merged_df['TYPE'].apply(lambda x: x if x in valid_types else 'None')

# Prepare the dataframe for editing
edit_df = merged_df[['PROFILE_PICTURE', 'FULL_NAME', 'MARKET', 'TYPE', 'ACTIVE', 'GOAL', 'RANK', 'FM_GOAL', 'FM_RANK', 'SALESFORCE_ID']].copy()

# Initialize session state
if 'filtered_edit_df' not in st.session_state:
    st.session_state['filtered_edit_df'] = edit_df.copy()

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
filtered_edit_df = st.session_state['filtered_edit_df'].copy()

# Create columns for the filters
cols1, cols2, cols3 = st.columns(3)

# First filter: Market
with cols1:
    market_input = st.selectbox('', get_market_options(filtered_edit_df), index=0, key='market_select')

# Filter by market if not 'All Markets'
if market_input != 'All Markets':
    filtered_edit_df = filtered_edit_df[filtered_edit_df['MARKET'] == market_input]

# Second filter: Closer
with cols2:
    closer_input = st.selectbox('', get_closer_options(filtered_edit_df), index=0, key='closer_select')

# Filter by closer if not 'All Closers'
if closer_input != 'All Closers':
    filtered_edit_df = filtered_edit_df[filtered_edit_df['FULL_NAME'] == closer_input]

# Third filter: Type
with cols3:
    type_input = st.selectbox('', get_type_options(filtered_edit_df), index=0, key='type_select')

# Filter by type if not 'All Channels'
if type_input != 'All Channels':
    filtered_edit_df = filtered_edit_df[filtered_edit_df['TYPE'] == type_input]

# Sort the filtered DataFrame by 'FULL_NAME'
filtered_edit_df = filtered_edit_df.sort_values(by='FULL_NAME')

# Wrap the data editor and save button in a form
with st.form('editor_form'):
    # Reset indices for comparison
    original_filtered_df = filtered_edit_df.copy().reset_index(drop=True)
    
    # Configure the data editor with column configurations
    edited_df = st.data_editor(
        filtered_edit_df.reset_index(drop=True),
        column_order=['PROFILE_PICTURE', 'FULL_NAME', 'MARKET', 'TYPE', 'ACTIVE', 'GOAL', 'RANK', 'FM_GOAL', 'FM_RANK'],
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
                'W2H Goal'
            ),
            'RANK': st.column_config.NumberColumn(
                'W2H Rank'
            ),
            'FM_GOAL': st.column_config.NumberColumn(
                'FM Goal'
            ),
            'FM_RANK': st.column_config.NumberColumn(
                'FM Rank'
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
    # Normalize data types before comparison
    edited_df = edited_df.astype(str)
    original_filtered_df = original_filtered_df.astype(str)

    # Strip whitespaces
    edited_df = edited_df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    original_filtered_df = original_filtered_df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

    # Compare the edited data with the original data
    changes = edited_df.compare(original_filtered_df)

    if changes.empty:
        st.info("No changes detected.")
    else:
        # Update session state with new edited data
        st.session_state['filtered_edit_df'].update(edited_df)

        # Accumulate queries for batch execution
        queries = []
        for idx in changes.index.unique():
            row = edited_df.loc[idx]
            full_name = row['FULL_NAME'].replace("'", "''")
            new_goal = int(row['GOAL'])
            new_rank = int(row['RANK'])
            fm_goal = int(row['FM_GOAL'])
            fm_rank = int(row['FM_RANK'])
            new_active = row['ACTIVE']
            new_type = row['TYPE']
            new_market = row['MARKET']
            profile_picture = row['PROFILE_PICTURE']
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            active_str = 'Yes' if new_active == 'True' else 'No'

            query = f"""
            MERGE INTO raw.snowflake.lm_appointments AS target
            USING (SELECT '{full_name}' AS NAME) AS source
            ON target.NAME = source.NAME
            WHEN MATCHED THEN
                UPDATE SET
                    GOAL = {new_goal},
                    RANK = {new_rank},
                    FM_GOAL = {fm_goal},
                    FM_RANK = {fm_rank},
                    ACTIVE = '{active_str}',
                    TYPE = '{new_type}',
                    MARKET = '{new_market}',
                    TIMESTAMP = '{timestamp}',
                    PROFILE_PICTURE = '{profile_picture}'
            WHEN NOT MATCHED THEN
                INSERT (CLOSER_ID, NAME, GOAL, RANK, FM_GOAL, FM_RANK, ACTIVE, TYPE, MARKET, TIMESTAMP, PROFILE_PICTURE)
                VALUES ('{row['SALESFORCE_ID']}', '{full_name}', {new_goal}, {new_rank}, {fm_goal}, {fm_rank}, '{active_str}', '{new_type}', '{new_market}', '{timestamp}', '{profile_picture}');
            """
            queries.append(query)

        # Execute all queries in a batch
        with st.spinner('Saving changes...'):
            for query in queries:
                try:
                    session.sql(query).collect()
                    st.success(f"Saved changes for {row['FULL_NAME']}")
                except Exception as e:
                    st.error(f"Error saving changes for {row['FULL_NAME']}: {str(e)}")