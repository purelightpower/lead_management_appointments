import streamlit as st

def render_sidebar(df_markets, filtered_edit_df):
    # Sidebar title
    st.sidebar.title("Filters")

    # First filter: Market
    market_input = st.sidebar.selectbox('Select Market', ['All Markets'] + sorted(filtered_edit_df['MARKET'].unique()), index=0)

    # Filter by market if not 'All Markets'
    if market_input != 'All Markets':
        filtered_edit_df = filtered_edit_df[filtered_edit_df['MARKET'] == market_input]

    # Second filter: Closer
    closer_input = st.sidebar.selectbox('Select Closer', ['All Closers'] + sorted(filtered_edit_df['FULL_NAME'].unique()), index=0)

    # Filter by closer if not 'All Closers'
    if closer_input != 'All Closers':
        filtered_edit_df = filtered_edit_df[filtered_edit_df['FULL_NAME'] == closer_input]

    # Third filter: Type
    type_input = st.sidebar.selectbox('Select Type', ['All Channels'] + sorted(filtered_edit_df['TYPE'].unique()), index=0)

    # Filter by type if not 'All Channels'
    if type_input != 'All Channels':
        filtered_edit_df = filtered_edit_df[filtered_edit_df['TYPE'] == type_input]

    return filtered_edit_df