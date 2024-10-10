import streamlit as st
from geopy.geocoders import Nominatim
import requests
import folium
from streamlit_folium import st_folium
import openrouteservice


def get_coordinates(address):
    try:
        geolocator = Nominatim(user_agent="geo_app")
        location = geolocator.geocode(address)
        if location:
            return (location.latitude, location.longitude)
    except:
        return None
    return None


def main():
    st.title("Address Distance Calculator")

    # Input fields for starting and ending addresses on the same line
    col1, col2, col3 = st.columns([3, 3, 1])
    with col1:
        starting_address = st.text_input("Starting", key="start")
    with col2:
        ending_address = st.text_input("Ending", key="end")
    with col3:
        calculate_button = st.button("Calculate", key="calculate")

    # Calculate distance when button is pressed
    if calculate_button and starting_address and ending_address:
        try:
            start_coords = get_coordinates(starting_address)
            end_coords = get_coordinates(ending_address)

            if start_coords and end_coords:
                # Use OpenRouteService to get driving distance
                client = openrouteservice.Client(key=st.secrets["open_route"]["token"])
                try:
                    route = client.directions(
                        coordinates=[(start_coords[1], start_coords[0]), (end_coords[1], end_coords[0])],
                        profile='driving-car',
                        format='geojson'
                    )
                except openrouteservice.exceptions.ApiError as e:
                    st.write("Error with OpenRouteService API. Please check your API key or try again later.")
                    return

                # Extract distance in meters and convert to miles and kilometers
                distance_meters = route['features'][0]['properties']['segments'][0]['distance']
                distance_km = distance_meters / 1000
                distance_miles = distance_km * 0.621371

                st.write(f"Driving distance between the addresses: {distance_miles:.2f} miles ({distance_km:.2f} km)")

                # Update map with folium
                map_center = [(start_coords[0] + end_coords[0]) / 2, (start_coords[1] + end_coords[1]) / 2]
                distance_map = folium.Map(location=map_center, zoom_start=10)

                # Add markers for starting and ending locations
                folium.Marker(start_coords, popup="Starting Address", icon=folium.Icon(color='green')).add_to(distance_map)
                folium.Marker(end_coords, popup="Ending Address", icon=folium.Icon(color='red')).add_to(distance_map)

                # Add the route line to the map
                folium.PolyLine([tuple(reversed(coord)) for coord in route['features'][0]['geometry']['coordinates']], color='blue').add_to(distance_map)

                # Display updated map
                st.subheader("Map Showing the Driving Route")
                st_folium(distance_map, width=700, height=500)
            else:
                st.write("Could not find one or both of the addresses. Please try again.")
        except requests.exceptions.RequestException as e:
            st.write("Network error: Please check your connection and try again.")
        except ModuleNotFoundError as e:
            st.write("Required module not found. Please ensure all dependencies are installed.")


if __name__ == "__main__":
    main()
