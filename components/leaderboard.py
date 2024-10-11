import streamlit as st
import pandas as pd

def display_styled_table(
    df,
    image_column: str,
    name_column: str,
    score_column: str,
    rank_column: str = None,
    alignment_dict: dict = None,
    image_size: int = 70,
    table_width: str = '80%',
    badge_size: int = 25,
    max_height: str = None  # New parameter for scroll height (e.g., '400px')
):
    """
    Displays a styled DataFrame in Streamlit with images, badges, and custom styling.

    Parameters:
    - df: The DataFrame to display.
    - image_column: The name of the column containing image URLs.
    - name_column: The name of the column containing names.
    - score_column: The name of the column containing scores.
    - rank_column: The name of the column to use for ranking (optional).
    - alignment_dict: A dictionary specifying text alignment per column.
    - image_size: Size of the images in pixels.
    - table_width: Width of the table (e.g., '80%').
    - badge_size: Size of the badges in pixels.
    - max_height: Maximum height of the table container for scrolling (e.g., '400px')
    """

    # If rank_column is not provided, create it based on the score_column
    if rank_column is None:
        df['Rank'] = df[score_column].rank(method='max', ascending=False).astype(int)
        rank_column = 'Rank'
    else:
        df[rank_column] = df[rank_column].astype(int)

    # Function to add rank overlay (gold, silver, bronze, or empty for others)
    def rank_overlay(rank):
        if rank == 1:
            return '<span class="badge gold">1</span>'
        elif rank == 2:
            return '<span class="badge silver">2</span>'
        elif rank == 3:
            return '<span class="badge bronze">3</span>'
        else:
            return '<span class="badge" style="visibility: hidden;"></span>'  # Invisible overlay for other ranks

    # Function to display name and image with border and overlay badge
    def name_image_html(image_path, rank, name):
        border_color = ''
        if rank == 1:
            border_color = 'gold'
        elif rank == 2:
            border_color = 'silver'
        elif rank == 3:
            border_color = '#cd7f32'  # Bronze
        else:
            border_color = 'transparent'  # No border for ranks beyond 3

        overlay = rank_overlay(rank)

        return f'''
        <div style="position: relative; display: inline-block;">
            <img src="{image_path}" style="border-radius: 50%; border: 2px solid {border_color}; margin-right: 10px; width:{image_size}px; height:{image_size}px;">
            {overlay}
        </div>
        <span style="vertical-align: middle;">{name}</span>
        '''

    # Apply the name and image HTML to the DataFrame
    df['Name_Image'] = df.apply(lambda row: name_image_html(row[image_column], row[rank_column], row[name_column]), axis=1)

    # Define default text alignment if not provided
    if alignment_dict is None:
        alignment_dict = {
            rank_column: 'center',
            'Name_Image': 'left',
            score_column: 'center'
        }

    # Function to style the DataFrame
    def style_table(df, alignment_dict):
        styler = df.style.set_properties(**{
            'background-color': '#1A1E23',
            'color': 'white',
            'padding': '10px 20px'
        })

        # Set text alignment per column
        for col, alignment in alignment_dict.items():
            styler = styler.set_properties(subset=[col], **{'text-align': alignment})

        # Apply background color to even rows
        styler = styler.apply(lambda x: ['background-color: #272D33' if x.name % 2 == 0 else '' for _ in x], axis=1)

        return styler

    # Prepare the DataFrame for display
    display_df = df[[rank_column, 'Name_Image', score_column]]

    # Style the DataFrame
    styled_df = style_table(display_df, alignment_dict)

    # Build CSS for header alignment
    header_css = ''
    column_list = display_df.columns.tolist()
    for idx, col in enumerate(column_list):
        alignment = alignment_dict.get(col, 'center')
        header_css += f'''
        th:nth-child({idx + 1}) {{
            text-align: {alignment};
        }}
        '''

    # Add custom CSS for badges and table styling
    st.markdown(f"""
        <style>
        .badge {{
            position: absolute;
            top: -10px;
            left: -10px;
            background-color: #ccc;
            color: white;
            border-radius: 50%;
            padding: 5px;
            width: {badge_size}px;
            height: {badge_size}px;
            display: flex;
            justify-content: center;
            align-items: center;
            font-weight: bold;
        }}
        .gold {{
            background-color: gold;
        }}
        .silver {{
            background-color: silver;
        }}
        .bronze {{
            background-color: #cd7f32;
        }}
        /* Center the table */
        table {{
            margin-left: auto;
            margin-right: auto;
            width: {table_width};
        }}
        th {{
            font-weight: bold;
        }}
        {header_css}
        td {{
            vertical-align: middle;
        }}
        </style>
        """, unsafe_allow_html=True)

    # Generate the table HTML
    table_html = styled_df.hide(axis='index').to_html(escape=False)

    # Wrap the table in a div with optional scroll
    if max_height:
        table_container_style = f'overflow-y: auto; max-height: {max_height};'
    else:
        table_container_style = ''

    st.markdown(f'''
        <div style="{table_container_style}">
            {table_html}
    ''', unsafe_allow_html=True)