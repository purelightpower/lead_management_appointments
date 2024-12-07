import streamlit as st
import pandas as pd
from datetime import datetime
from snowflake.snowpark import Session

# Configure the Streamlit page settings
st.set_page_config(
    page_title="Appointment Dashboard",          # Set the title of the page
    initial_sidebar_state="collapsed",           # Collapse the sidebar by default
    layout="wide"                                # Use the full width of the page
)

# Display the company logo at the top of the page
st.logo("https://i.ibb.co/bbH9pgH/Purelight-Logo.webp")

# Apply custom CSS styles
custom_css = """
    <style>
    /* Hide Streamlit's default elements */
    #MainMenu {visibility: hidden;}              /* Hide the hamburger menu */
    footer {visibility: hidden;}                 /* Hide the footer */
    header {visibility: hidden;}                 /* Hide the header */
    .css-10trblm {padding-top: 0px; padding-bottom: 0px;}  /* Adjust padding */
    .css-1d391kg {padding-top: 0px !important;}  /* Adjust padding */
    </style>
"""
st.markdown(custom_css, unsafe_allow_html=True)

# Function to create a Snowflake session using the connection parameters from Streamlit secrets
def create_snowflake_session():
    connection_parameters = {
        "account": st.secrets["snowflake"]["account"],     # Snowflake account name
        "user": st.secrets["snowflake"]["user"],           # Username
        "password": st.secrets["snowflake"]["password"],   # Password
        "role": st.secrets["snowflake"]["role"],           # User role
        "warehouse": st.secrets["snowflake"]["warehouse"], # Compute warehouse
        "database": st.secrets["snowflake"]["database"],   # Database name
        "schema": st.secrets["snowflake"]["schema"],       # Schema name
    }
    return Session.builder.configs(connection_parameters).create()

# Initialize the Snowflake session
session = create_snowflake_session()

# Cache the function to get appointments data to avoid redundant queries
@st.cache_data(show_spinner=False)
def get_appointments():
    appointments_query = """
        SELECT a.NAME, a.MARKET, a.TYPE, a.ACTIVE, a.GOAL, a.RANK, a.FM_GOAL, a.FM_RANK, a.CLOSER_ID, a.TIMESTAMP, a.PROFILE_PICTURE
        FROM raw.snowflake.lm_appointments a
    """
    return session.sql(appointments_query).to_pandas()

# Cache the function to get all closers from the users table
@st.cache_data(show_spinner=False)
def get_all_closers():
    closers_query = """
        SELECT DISTINCT FULL_NAME NAME, SALESFORCE_ID CLOSER_ID, PROFILE_PICTURE
        FROM operational.airtable.vw_users
        WHERE ROLE = 'Closer'  -- Adjust this condition based on your data
    """
    return session.sql(closers_query).to_pandas()

# Load the appointments data using the cached function
appointments = get_appointments()

# Load all closers data
all_closers_df = get_all_closers()

# Now, include all closers
available_closers_df = all_closers_df
available_closers = available_closers_df['NAME'].tolist()

# Data cleaning and preprocessing steps
appointments['ACTIVE'] = appointments['ACTIVE'].fillna('No').astype(str).str.strip().str.lower().map({'yes': True, 'no': False})
appointments['ACTIVE'] = appointments['ACTIVE'].fillna(False)

valid_types = ['🏠🏃 Hybrid', '🏃 Field Marketing', '🏠 Web To Home']
appointments['TYPE'] = appointments['TYPE'].apply(lambda x: x if x in valid_types else '🏠🏃 Hybrid')

appointments['MARKET'] = appointments['MARKET'].fillna('No Market').astype(str)

appointments['GOAL'] = appointments['GOAL'].fillna(0).astype(int)
appointments['RANK'] = appointments['RANK'].fillna(100).astype(int)
appointments['FM_GOAL'] = appointments['FM_GOAL'].fillna(0).astype(int)
appointments['FM_RANK'] = appointments['FM_RANK'].fillna(100).astype(int)

default_profile_picture = 'https://i.ibb.co/ZNK5xmN/pdycc8-1-removebg-preview.png'
appointments['PROFILE_PICTURE'] = appointments['PROFILE_PICTURE'].fillna(default_profile_picture)

# Prepare the dataframe for editing
edit_df = appointments[['PROFILE_PICTURE', 'NAME', 'MARKET', 'TYPE', 'ACTIVE', 'GOAL', 'RANK', 'FM_GOAL', 'FM_RANK', 'CLOSER_ID']].copy()

unique_closers = edit_df['NAME'].unique().tolist()

# Initialize session state
if 'filtered_edit_df' not in st.session_state:
    st.session_state['filtered_edit_df'] = edit_df.copy()

# Display a warning message
st.warning("ⓘ This page is for managers only. If you're not a manager or responsible for updating closer targets, please use the appointments page only.")

# Display the page title
st.write("## 🎯 Edit Closer Targets")

# Separator
st.write("---")

# Container for the "Add or Update Closer" form
add_closer_container = st.container()

with add_closer_container:
    st.write("### Add or Update Closer")
    with st.form('add_closer_form', clear_on_submit=True):
        # Selectbox for the closer's name from all closers
        selected_name = st.selectbox('Select Closer Name', options=available_closers)
        
        # Fetch CLOSER_ID and PROFILE_PICTURE
        closer_data = available_closers_df[available_closers_df['NAME'] == selected_name].iloc[0]
        closer_id = closer_data['CLOSER_ID'] or ''
        profile_picture = closer_data['PROFILE_PICTURE'] or default_profile_picture
        
        # Check if the closer already exists
        exists_in_appointments = selected_name in appointments['NAME'].values
        
        # If exists, fetch existing data to pre-fill the form
        if exists_in_appointments:
            existing_record = appointments[appointments['NAME'] == selected_name].iloc[0]
            existing_market = existing_record['MARKET']
            existing_type = existing_record['TYPE']
            existing_active = existing_record['ACTIVE']
            existing_goal = existing_record['GOAL']
            existing_rank = existing_record['RANK']
            existing_fm_goal = existing_record['FM_GOAL']
            existing_fm_rank = existing_record['FM_RANK']
        else:
            existing_market = ''
            existing_type = valid_types[0]
            existing_active = True
            existing_goal = 0
            existing_rank = 100
            existing_fm_goal = 0
            existing_fm_rank = 100
        
        new_market = st.selectbox('Market', options=sorted(edit_df['MARKET'].unique()), index=sorted(edit_df['MARKET'].unique()).index(existing_market) if existing_market in edit_df['MARKET'].unique() else 0)
        new_type = st.selectbox('Type', options=valid_types, index=valid_types.index(existing_type) if existing_type in valid_types else 0)
        new_active = st.checkbox('Active', value=existing_active)
        new_goal = st.number_input('W2H Goal', min_value=0, value=int(existing_goal))
        new_rank = st.number_input('W2H Rank', min_value=0, value=int(existing_rank))
        fm_goal = st.number_input('FM Goal', min_value=0, value=int(existing_fm_goal))
        fm_rank = st.number_input('FM Rank', min_value=0, value=int(existing_fm_rank))
        
        # Submit button to add or update the closer
        submit_label = 'Update Closer' if exists_in_appointments else 'Add New Closer'
        add_submitted = st.form_submit_button(submit_label)
    
    # Process the form submission
    if add_submitted:
        if not selected_name:
            st.error("Please select a closer name.")
        else:
            # Prepare variables for the SQL query
            full_name = selected_name.replace("'", "''")
            new_market = new_market.replace("'", "''")
            new_type = new_type.replace("'", "''")
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            active_str = 'Yes' if new_active else 'No'
            
            # Prepare the data dictionary
            data = {
                'CLOSER_ID': closer_id,
                'NAME': full_name,
                'GOAL': int(new_goal),
                'RANK': int(new_rank),
                'FM_GOAL': int(fm_goal),
                'FM_RANK': int(fm_rank),
                'ACTIVE': active_str,
                'TYPE': new_type,
                'MARKET': new_market,
                'PROFILE_PICTURE': profile_picture,
                'TIMESTAMP': timestamp
            }
            
            if exists_in_appointments:
                # Prepare the UPDATE query
                set_clause = ",\n    ".join([f"{key} = '{value}'" if isinstance(value, str) else f"{key} = {value}" for key, value in data.items() if key != 'NAME'])
                query = f"""
                UPDATE raw.snowflake.lm_appointments
                SET {set_clause}
                WHERE NAME = '{full_name}';
                """
                action = 'updated'
            else:
                # Prepare the INSERT query
                columns = ", ".join(data.keys())
                values = ", ".join([f"'{value}'" if isinstance(value, str) else f"{value}" for value in data.values()])
                query = f"""
                INSERT INTO raw.snowflake.lm_appointments ({columns})
                VALUES ({values});
                """
                action = 'added'
            
            # Execute the query
            try:
                session.sql(query).collect()
                st.success(f"Successfully {action} closer: {selected_name}")
                
                # Clear cached data functions
                get_appointments.clear()
                # Reload the appointments data
                appointments = get_appointments()
                
                # Update the session state
                st.session_state['filtered_edit_df'] = appointments[['PROFILE_PICTURE', 'NAME', 'MARKET', 'TYPE', 'ACTIVE', 'GOAL', 'RANK', 'FM_GOAL', 'FM_RANK', 'CLOSER_ID']].copy()
                # Clear the cached data
                st.cache_data.clear()
                
            except Exception as e:
                st.error(f"Error processing closer: {str(e)}")

# Separator
st.write("---")

# Define functions to get options for filters
def get_market_options(filtered_df):
    return ['All Markets'] + sorted(filtered_df['MARKET'].unique())

def get_closer_options(filtered_df):
    return ['All Closers'] + sorted(filtered_df['NAME'].unique())

def get_type_options(filtered_df):
    return ['All Channels'] + sorted(filtered_df['TYPE'].unique())

# Use the filtered dataframe from session state
filtered_edit_df = st.session_state['filtered_edit_df'].copy()

# Create three columns for the filters
cols1, cols2, cols3 = st.columns(3)

# First filter: Market
with cols1:
    market_input = st.selectbox('Select Market', get_market_options(filtered_edit_df), index=0, key='market_select')

# Filter the dataframe based on the selected market
if market_input != 'All Markets':
    filtered_edit_df = filtered_edit_df[filtered_edit_df['MARKET'] == market_input]

# Second filter: Closer
with cols2:
    closer_input = st.selectbox('Select Closer', get_closer_options(filtered_edit_df), index=0, key='closer_select')

# Filter the dataframe based on the selected closer
if closer_input != 'All Closers':
    filtered_edit_df = filtered_edit_df[filtered_edit_df['NAME'] == closer_input]

# Third filter: Type
with cols3:
    type_input = st.selectbox('Select Type', get_type_options(filtered_edit_df), index=0, key='type_select')

# Filter the dataframe based on the selected type
if type_input != 'All Channels':
    filtered_edit_df = filtered_edit_df[filtered_edit_df['TYPE'] == type_input]

# Sort the filtered dataframe
filtered_edit_df = filtered_edit_df.sort_values(by='NAME').reset_index(drop=True)

# Display the data editor for existing closers
st.write("### Existing Closers")
with st.form('editor_form'):
    # Store the original filtered dataframe
    original_filtered_df = filtered_edit_df.copy()

    # Configure the data editor
    edited_df = st.data_editor(
        filtered_edit_df,
        column_order=['PROFILE_PICTURE', 'NAME', 'MARKET', 'TYPE', 'ACTIVE', 'GOAL', 'RANK', 'FM_GOAL', 'FM_RANK'],
        hide_index=True,
        use_container_width=True,
        disabled={
            'NAME': True  # Disable editing of the 'NAME' column entirely
        },
        column_config={
            'PROFILE_PICTURE': st.column_config.ImageColumn(
                label=' ',
                help="Profile Picture"
            ),
            'ACTIVE': st.column_config.CheckboxColumn(
                'Active',
                help="Check if the closer is active",
                default=False
            ),
            'MARKET': st.column_config.SelectboxColumn(
                'Market',
                options=get_market_options(filtered_edit_df)[1:],  # Exclude 'All Markets'
                help="Select the market",
                required=True
            ),
            'GOAL': st.column_config.NumberColumn(
                'W2H Goal',
                help="Set the Web To Home goal"
            ),
            'RANK': st.column_config.NumberColumn(
                'W2H Rank',
                help="Set the Web To Home rank"
            ),
            'FM_GOAL': st.column_config.NumberColumn(
                'FM Goal',
                help="Set the Field Marketing goal"
            ),
            'FM_RANK': st.column_config.NumberColumn(
                'FM Rank',
                help="Set the Field Marketing rank"
            ),
            'TYPE': st.column_config.SelectboxColumn(
                'Type',
                options=valid_types,
                help="Select the type of channel",
                required=True
            ),
        }
    )

    # Submit button to save changes
    submitted = st.form_submit_button('Save Changes to Existing Closers')

# Process the form submission
if submitted:
    # Align indices
    edited_df = edited_df.reset_index(drop=True)
    original_filtered_df = original_filtered_df.reset_index(drop=True)

    # Compare the edited dataframe with the original
    changes = edited_df.compare(original_filtered_df)

    if changes.empty:
        st.info("No changes detected.")
    else:
        # Prepare to handle updates
        queries = []
        for idx in changes.index.unique():
            row = edited_df.loc[idx]
            full_name = row['NAME'].replace("'", "''")
            new_goal = int(row['GOAL'])
            new_rank = int(row['RANK'])
            fm_goal = int(row['FM_GOAL'])
            fm_rank = int(row['FM_RANK'])
            new_active = row['ACTIVE']
            new_type = row['TYPE']
            new_market = row['MARKET']
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            active_str = 'Yes' if new_active else 'No'

            query = f"""
            UPDATE raw.snowflake.lm_appointments
            SET GOAL = {new_goal},
                RANK = {new_rank},
                FM_GOAL = {fm_goal},
                FM_RANK = {fm_rank},
                ACTIVE = '{active_str}',
                TYPE = '{new_type}',
                MARKET = '{new_market}',
                TIMESTAMP = '{timestamp}'
            WHERE NAME = '{full_name}';
            """
            queries.append(query)

        # Execute all queries
        with st.spinner('Saving changes...'):
            for query in queries:
                try:
                    session.sql(query).collect()
                    st.success(f"Saved changes for {row['NAME']}")
                except Exception as e:
                    st.error(f"Error saving changes for {row['NAME']}: {str(e)}")

        # Clear cached data
        get_appointments.clear()
        get_all_closers.clear()

        # Reload the appointments data
        appointments = get_appointments()

        # Update the session state
        st.session_state['filtered_edit_df'] = appointments[['PROFILE_PICTURE', 'NAME', 'MARKET', 'TYPE', 'ACTIVE', 'GOAL', 'RANK', 'FM_GOAL', 'FM_RANK', 'CLOSER_ID']].copy()

        # Clear the cached data
        st.cache_data.clear()