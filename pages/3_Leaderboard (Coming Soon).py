import streamlit as st
import pandas as pd
import sys
import os

# Get the directory of the current script
current_dir = os.path.dirname(os.path.abspath(__file__))

# Get the parent directory
parent_dir = os.path.abspath(os.path.join(current_dir, '..'))

# Add the parent directory to sys.path
sys.path.append(parent_dir)

from components.leaderboard import display_styled_table

st.set_page_config(layout="wide")

# Sample data with image URLs
data = {
    'Name': ['Lawson B.', 'Cris C.', 'Sean L.', 'Sydney R.'],
    'Score': [1, 2, 3, 4],
    'Sets': [16, 14, 13, 11],
    'Image': [
        'https://solgen.file.force.com/profilephoto/729PL0000005F4r/F',  # Placeholder image URL for Alice
        'https://solgen.file.force.com/profilephoto/729PL000000gFQr/F',  # Placeholder image URL for Bob
        'https://solgen.file.force.com/profilephoto/7294u00000117hD/F',  # Placeholder image URL for Charlie
        'https://solgen.file.force.com/profilephoto/7294u00000117hc/F'   # Placeholder image URL for David
    ]
}
df = pd.DataFrame(data)

data = {
    'Name': ['Lawson B.', 'Cris C.', 'Sean L.', 'Sydney R.'],
    'Score': [1, 2, 3, 4],
    'Pulls': [135, 122, 119, 111],
    'Image': [
        'https://solgen.file.force.com/profilephoto/729PL0000005F4r/F',  # Placeholder image URL for Alice
        'https://solgen.file.force.com/profilephoto/729PL000000gFQr/F',  # Placeholder image URL for Bob
        'https://solgen.file.force.com/profilephoto/7294u00000117hD/F',  # Placeholder image URL for Charlie
        'https://solgen.file.force.com/profilephoto/7294u00000117hc/F'   # Placeholder image URL for David
    ]
}
df_2 = pd.DataFrame(data)

st.info("ⓘ Leaderboards are coming soon, where you'll be able to see how you measure up against coworkers")

cols1,cols2 = st.columns([1,1])
# Call the function with your DataFrame and parameters
with cols1:
    st.title('Sets', help="Sets are measure on the day you called the customer and scheduled the appointment for the sales rep. This is the day the Opportunity was created in Salesforce.")
    display_styled_table(
        df,
        image_column='Image',
        name_column='Name',
        score_column='Sets',
        image_size=35,
        table_width='90%',
        badge_size=18
    )

with cols2:
    st.title('Pulls')
    display_styled_table(
        df_2,
        image_column='Image',
        name_column='Name',
        score_column='Pulls',
        image_size=35,
        table_width='100%',
        badge_size=18
    )