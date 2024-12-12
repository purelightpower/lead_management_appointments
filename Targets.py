import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime
from snowflake.snowpark import Session

st.set_page_config(
    page_title="Appointment Dashboard",
    initial_sidebar_state="collapsed",
    layout="wide"
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

session = create_snowflake_session()

if st.session_state.get('data_updated', False):
    st.cache_data.clear()
    st.session_state['data_updated'] = False

@st.cache_data(show_spinner=False)
def get_users():
    users_query = """
        SELECT DISTINCT FULL_NAME, SALESFORCE_ID
        FROM operational.airtable.vw_users 
        WHERE role_type IN ('Closer', 'Manager') AND term_date IS NULL
    """
    return session.sql(users_query).to_pandas()

@st.cache_data(show_spinner=False)
def get_market():
    market_query = """
        SELECT MARKET, MARKET_GROUP, RANK, NOTES
        FROM raw.snowflake.lm_markets 
    """
    return session.sql(market_query).to_pandas()

@st.cache_data(show_spinner=False)
def get_profile_pictures():
    profile_picture_query = """
        SELECT FULL_NAME, PROFILE_PICTURE
        FROM operational.airtable.vw_users
    """
    return session.sql(profile_picture_query).to_pandas()

@st.cache_data(show_spinner=False)
def get_appointments():
    appointments_query = """
        SELECT * FROM raw.snowflake.lm_appointments
    """
    return session.sql(appointments_query).to_pandas()

df_users = get_users()
df_markets = get_market()
df_profile_pictures = get_profile_pictures()  # Reintroduced to fetch profile picture
appointments = get_appointments()

st.warning("ⓘ This page is for managers only. If you're not a manager or responsible for updating closer targets, please use the appointments page only.")

if 'NAME' in appointments.columns:
    appointments = appointments.rename(columns={'NAME': 'FULL_NAME'})

if 'SALESFORCE_ID' not in appointments.columns:
    appointments['SALESFORCE_ID'] = ''

if 'PROFILE_PICTURE' not in appointments.columns:
    appointments['PROFILE_PICTURE'] = 'https://i.ibb.co/ZNK5xmN/pdycc8-1-removebg-preview.png'

merged_df = appointments.copy()

if 'CLOSER' in merged_df.columns:
    merged_df = merged_df.drop(columns=['CLOSER'])

merged_df['MARKET'] = merged_df['MARKET'].fillna('No Market').astype(str)
merged_df['GOAL'] = merged_df['GOAL'].fillna(0).astype(int)
merged_df['RANK'] = merged_df['RANK'].fillna(100).astype(int)
merged_df['FM_GOAL'] = merged_df['FM_GOAL'].fillna(0).astype(int)
merged_df['FM_RANK'] = merged_df['FM_RANK'].fillna(100).astype(int)
merged_df['TYPE'] = merged_df['TYPE'].fillna('🏠🏃 Hybrid').astype(str)
merged_df['TYPE'] = merged_df['TYPE'].fillna('Empty').astype(str)
merged_df['PROFILE_PICTURE'] = merged_df['PROFILE_PICTURE'].fillna('https://i.ibb.co/ZNK5xmN/pdycc8-1-removebg-preview.png').astype(str)
merged_df['ACTIVE'] = merged_df['ACTIVE'].fillna('No').astype(str)
merged_df['ACTIVE'] = merged_df['ACTIVE'].str.strip().str.lower().map({'yes': True, 'no': False})
merged_df['ACTIVE'] = merged_df['ACTIVE'].fillna(False)

valid_types = ['🏠🏃 Hybrid', '🏃 Field Marketing', '🏠 Web To Home']
merged_df['TYPE'] = merged_df['TYPE'].apply(lambda x: x if x in valid_types else '🏠🏃 Hybrid')

valid_market_types = df_markets['MARKET'].unique()
merged_df['MARKET'] = merged_df['MARKET'].apply(lambda x: x if x in valid_market_types else 'No Market')

edit_df = merged_df[['PROFILE_PICTURE', 'FULL_NAME', 'MARKET', 'TYPE', 'ACTIVE', 'GOAL', 'RANK', 'FM_GOAL', 'FM_RANK', 'SALESFORCE_ID', 'CLOSER_NOTES']].copy()

# UPDATED SECTION: Always update the session state with the latest edit_df
st.session_state['filtered_edit_df'] = edit_df.copy()

def get_market_options(filtered_df):
    return ['All Markets'] + sorted(filtered_df['MARKET'].unique())

def get_closer_options(filtered_df):
    return ['All Closers'] + sorted(filtered_df['FULL_NAME'].unique())

def get_type_options(filtered_df):
    return ['All Channels'] + sorted(filtered_df['TYPE'].unique())

st.write("## 🎯 Edit Closer Targets")

filtered_edit_df = st.session_state['filtered_edit_df'].copy()

cols1, cols2, cols3 = st.columns(3)

with cols1:
    market_input = st.selectbox('', get_market_options(filtered_edit_df), index=0, key='market_select')

if market_input != 'All Markets':
    filtered_edit_df = filtered_edit_df[filtered_edit_df['MARKET'] == market_input]

with cols2:
    closer_input = st.selectbox('', get_closer_options(filtered_edit_df), index=0, key='closer_select')

if closer_input != 'All Closers':
    filtered_edit_df = filtered_edit_df[filtered_edit_df['FULL_NAME'] == closer_input]

with cols3:
    type_input = st.selectbox('', get_type_options(filtered_edit_df), index=0, key='type_select')

if type_input != 'All Channels':
    filtered_edit_df = filtered_edit_df[filtered_edit_df['TYPE'] == type_input]

filtered_edit_df = filtered_edit_df.sort_values(by='FULL_NAME')

# Generate closer list from df_users
closer_list = sorted(df_users['FULL_NAME'].dropna().unique())

if hasattr(st, 'popover'):
    with st.popover("Add Closer  + ", disabled=False):
        with st.form(clear_on_submit=True, key='add_closer_form', border=False):
            closer_selection = st.selectbox("Closer Name", options=closer_list)
            market_selection = st.selectbox("Market", options=valid_market_types)
            type_selection = st.selectbox("Type", options=valid_types)
            w2h_goal = st.number_input("Web Goal", min_value=0, max_value=60, value=12, step=1)
            w2h_rank = st.number_input("Web Rank", min_value=0, max_value=60, value=1, step=1)
            fm_goal = st.number_input("FM Goal", min_value=0, max_value=60, value=12, step=1)
            fm_rank = st.number_input("FM Rank", min_value=0, max_value=60, value=1, step=1)
            is_active = st.checkbox("Active?", value=True)
            closer_notes = st.text_area("Notes")

            submit_button = st.form_submit_button("Submit")
            if submit_button:
                if closer_selection.strip() == '':
                    st.error("Closer name cannot be empty.")
                else:
                    full_name = closer_selection.strip().replace("'", "''")
                    active_str = 'Yes' if is_active else 'No'
                    
                    # Fetch profile picture from df_profile_pictures if available
                    profile_pic = 'https://i.ibb.co/ZNK5xmN/pdycc8-1-removebg-preview.png'
                    if closer_selection in df_profile_pictures['FULL_NAME'].values:
                        profile_pic = df_profile_pictures.loc[df_profile_pictures['FULL_NAME'] == closer_selection, 'PROFILE_PICTURE'].iloc[0]
                        if pd.isna(profile_pic) or profile_pic.strip() == '':
                            profile_pic = 'https://i.ibb.co/ZNK5xmN/pdycc8-1-removebg-preview.png'

                    salesforce_id = ''
                    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

                    insert_query = f"""
                    INSERT INTO raw.snowflake.lm_appointments (CLOSER_ID, NAME, GOAL, RANK, FM_GOAL, FM_RANK, ACTIVE, TYPE, MARKET, TIMESTAMP, PROFILE_PICTURE, CLOSER_NOTES)
                    VALUES ('{salesforce_id}', '{full_name}', '{w2h_goal}', '{w2h_rank}', '{fm_goal}', '{fm_rank}', '{active_str}', '{type_selection}', '{market_selection}', '{timestamp}', '{profile_pic}', '{closer_notes}');
                    """

                    try:
                        session.sql(insert_query).collect()
                        st.success(f"You successfully added {closer_selection}")
                        get_appointments.clear()
                        st.session_state['data_updated'] = True
                        st.cache_data.clear()
                        st.rerun()  # Force app to rerun to show new data immediately
                    except Exception as e:
                        st.error(f"Error adding {closer_selection}: {str(e)}")
else:
    st.info("Popover feature not available. Please upgrade Streamlit or use an alternative component.")

with st.form('editor_form'):
    original_filtered_df = filtered_edit_df.copy().reset_index(drop=True)
    
    edited_df = st.data_editor(
        filtered_edit_df.reset_index(drop=True),
        column_order=['PROFILE_PICTURE', 'FULL_NAME', 'MARKET', 'TYPE', 'ACTIVE', 'GOAL', 'RANK', 'FM_GOAL', 'FM_RANK', 'CLOSER_NOTES'],
        disabled={'FULL_NAME': True, 'PROFILE_PICTURE': True},
        hide_index=True,
        use_container_width=True,
        column_config={
            'PROFILE_PICTURE': st.column_config.ImageColumn(label=' '),
            'ACTIVE': st.column_config.CheckboxColumn('Active', help="Check if the closer is active", default=False),
            'FULL_NAME': st.column_config.TextColumn('Name'),
            'MARKET': st.column_config.SelectboxColumn('Market', options=valid_market_types, help="Select the market", required=True),
            'GOAL': st.column_config.NumberColumn('W2H Goal'),
            'RANK': st.column_config.NumberColumn('W2H Rank'),
            'FM_GOAL': st.column_config.NumberColumn('FM Goal'),
            'FM_RANK': st.column_config.NumberColumn('FM Rank'),
            'CLOSER_NOTES': st.column_config.TextColumn('Notes'),
            'TYPE': st.column_config.SelectboxColumn('Type', options=valid_types, help="Select the type of channel", required=True),
        }
    )
    
    submitted = st.form_submit_button('Save changes')

if submitted:
    edited_df = edited_df.astype(str)
    original_filtered_df = original_filtered_df.astype(str)
    edited_df = edited_df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    original_filtered_df = original_filtered_df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    changes = edited_df.compare(original_filtered_df)

    if changes.empty:
        st.info("No changes detected.")
    else:
        st.session_state['filtered_edit_df'].update(edited_df)
        
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
            closer_notes = row['CLOSER_NOTES']
            salesforce_id = row['SALESFORCE_ID'].replace("'", "''")
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            active_str = 'Yes' if new_active.lower() == 'true' else 'No'

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
                    PROFILE_PICTURE = '{profile_picture}',
                    CLOSER_NOTES = '{closer_notes}'
            WHEN NOT MATCHED THEN
                INSERT (CLOSER_ID, NAME, GOAL, RANK, FM_GOAL, FM_RANK, ACTIVE, TYPE, MARKET, TIMESTAMP, PROFILE_PICTURE, CLOSER_NOTES)
                VALUES ('{salesforce_id}', '{full_name}', {new_goal}, {new_rank}, {fm_goal}, {fm_rank}, '{active_str}', '{new_type}', '{new_market}', '{timestamp}', '{profile_picture}', '{closer_notes}');
            """
            queries.append(query)

        with st.spinner('Saving changes...'):
            for query in queries:
                try:
                    session.sql(query).collect()
                    st.success(f"Saved changes for {row['FULL_NAME']}")
                except Exception as e:
                    st.error(f"Error saving changes for {row['FULL_NAME']}: {str(e)}")

        get_appointments.clear()
        st.session_state['data_updated'] = True
        st.cache_data.clear()

st.divider()
st.write("## 🏙️ Edit Markets")

with st.form('market_editor_form'):
    original_market_df = df_markets.copy().reset_index(drop=True)
    edited_market_df = st.data_editor(
        df_markets[['MARKET', 'MARKET_GROUP', 'RANK', 'NOTES']].reset_index(drop=True),
        num_rows="dynamic",
        hide_index=True,
        use_container_width=True,
        column_config={
            'MARKET': st.column_config.TextColumn('Market'),
            'MARKET_GROUP': st.column_config.TextColumn('Market Group'),
            'RANK': st.column_config.NumberColumn('Rank'),
            'NOTES': st.column_config.TextColumn('Notes'),
        }
    )

    submitted_market = st.form_submit_button('Save Changes')

if submitted_market:
    edited_market_df = edited_market_df.reset_index(drop=True)
    original_market_df = original_market_df.reset_index(drop=True)

    original_markets = set(original_market_df['MARKET'])
    edited_markets = set(edited_market_df['MARKET'])

    new_markets = edited_markets - original_markets
    deleted_markets = original_markets - edited_markets
    common_markets = original_markets & edited_markets

    queries = []

    for market in deleted_markets:
        market_safe = market.replace("'", "''")
        query = f"DELETE FROM raw.snowflake.lm_markets WHERE MARKET = '{market_safe}';"
        queries.append((query, f"Deleted market '{market}'"))

    new_markets_df = edited_market_df[edited_market_df['MARKET'].isin(new_markets)]
    for idx, row in new_markets_df.iterrows():
        market = row['MARKET']
        if pd.isna(market) or market == '':
            st.error("Market name cannot be empty.")
            continue
        market_safe = market.replace("'", "''")

        market_group = row.get('MARKET_GROUP', '')
        if pd.isna(market_group):
            market_group = ''
        market_group = market_group.replace("'", "''")

        rank = row.get('RANK', '')
        if pd.isna(rank) or rank == '':
            rank_value = 'NULL'
        else:
            try:
                rank_value = int(rank)
            except (ValueError, TypeError):
                st.error(f"Invalid rank value for market '{market}'. Rank must be an integer.")
                continue

        notes = row.get('NOTES', '')
        if pd.isna(notes):
            notes = ''
        notes = notes.replace("'", "''")

        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        query = f"""
        INSERT INTO raw.snowflake.lm_markets (MARKET, MARKET_GROUP, RANK, NOTES, TIMESTAMP)
        VALUES ('{market_safe}', '{market_group}', {rank_value}, '{notes}', '{timestamp}');
        """
        queries.append((query, f"Inserted new market '{market}'"))

    for market in common_markets:
        edited_row = edited_market_df[edited_market_df['MARKET'] == market].iloc[0]
        original_row = original_market_df[original_market_df['MARKET'] == market].iloc[0]
        columns_to_compare = ['MARKET_GROUP', 'RANK', 'NOTES']
        if not edited_row[columns_to_compare].equals(original_row[columns_to_compare]):
            market_safe = market.replace("'", "''")

            market_group = edited_row.get('MARKET_GROUP', '')
            if pd.isna(market_group):
                market_group = ''
            market_group = market_group.replace("'", "''")

            rank = edited_row.get('RANK', '')
            if pd.isna(rank) or rank == '':
                rank_value = 'NULL'
            else:
                try:
                    rank_value = int(rank)
                except (ValueError, TypeError):
                    st.error(f"Invalid rank value for market '{market}'. Rank must be an integer.")
                    continue

            notes = edited_row.get('NOTES', '')
            if pd.isna(notes):
                notes = ''
            notes = notes.replace("'", "''")

            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            query = f"""
            UPDATE raw.snowflake.lm_markets
            SET MARKET_GROUP = '{market_group}', RANK = {rank_value}, NOTES = '{notes}', TIMESTAMP = '{timestamp}'
            WHERE MARKET = '{market_safe}';
            """
            queries.append((query, f"Updated market '{market}'"))

    if queries:
        with st.spinner('Saving changes...'):
            for query, message in queries:
                try:
                    session.sql(query).collect()
                    st.success(message)
                except Exception as e:
                    st.error(f"Error processing {message}: {str(e)}")
        get_market.clear()
        get_appointments.clear()
        df_markets = get_market()
    else:
        st.info("No changes detected.")