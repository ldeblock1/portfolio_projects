import pandas as pd
import googlemaps
from time import sleep
from dotenv import load_dotenv
import os

load_dotenv()


# Initialize Google Maps Geocoding API
api_key = os.getenv("API_KEY")
gmaps = googlemaps.Client(key=api_key)

# Load CSV file with a different encoding
file_path = '/Users/lucasdeblock/Library/CloudStorage/OneDrive-Personal/Red Cross Projects/Master_Database/redcross_outreach_strategy_database_2.csv'
df = pd.read_csv(file_path, encoding='ISO-8859-1')

# Concatenate the address columns into a single address string where Full Address is missing
df['Full Address'] = df.apply(
    lambda row: row['Full Address'] if pd.notnull(row['Full Address']) else f"{row['Site Addr1']}, {row['Site City']}, {row['Site State']} {row['Site Zip']}",
    axis=1
)

# Function to get latitude, longitude, phone number, and website from Google Maps
def get_location_info(address):
    try:
        geocode_result = gmaps.geocode(address)
        if geocode_result:
            location = geocode_result[0]['geometry']['location']
            lat = location['lat']
            lng = location['lng']
            place_id = geocode_result[0]['place_id']
            
            # Get place details using the place_id
            place_details = gmaps.place(place_id)
            phone_number = place_details['result'].get('formatted_phone_number')
            website = place_details['result'].get('website')
            
            return lat, lng, phone_number, website
        else:
            print(f"Address not found: {address}")
            return None, None, None, None
    except Exception as e:
        print(f"Error fetching details for address: {address}. Error: {e}")
        return None, None, None, None

# Add 'Website' column if it doesn't exist in the DataFrame
if 'Website' not in df.columns:
    df['Website'] = None

# Load intermediate progress, if exists
output_file = '/Users/lucasdeblock/Library/CloudStorage/OneDrive-Personal/Red Cross Projects/Master_Database/Redcross_Master_Database_with_websites.xlsx'
try:
    processed_df = pd.read_excel(output_file)
    processed_rows = processed_df.shape[0]
    print(f"Resuming from row {processed_rows}")
except FileNotFoundError:
    processed_rows = 0
    print("No progress file found, starting from the beginning.")

# Loop through the DataFrame and get missing data, starting from the last processed row
for i, row in df.iterrows():
    if i < processed_rows:
        continue  # Skip rows that have already been processed

    if pd.isnull(row['Phone']) or pd.isnull(row['Website']):
        print(f"Processing row {i+1}/{len(df)}: {row['Full Address']}")
        
        # If latitude and longitude are missing, get everything including coordinates
        if pd.isnull(row['Latitude']) or pd.isnull(row['Longitude']):
            lat, lng, phone, website = get_location_info(row['Full Address'])
            df.at[i, 'Latitude'] = lat
            df.at[i, 'Longitude'] = lng
            if pd.isnull(row['Phone']):
                df.at[i, 'Phone'] = phone
            if pd.isnull(df.at[i, 'Website']):
                df.at[i, 'Website'] = website
            print(f"Processed (with lat/long): {row['Full Address']} -> Latitude: {lat}, Longitude: {lng}, Phone: {phone}, Website: {website}")
        
        # If latitude and longitude exist, only find phone and website
        else:
            _, _, phone, website = get_location_info(row['Full Address'])
            if pd.isnull(row['Phone']):
                df.at[i, 'Phone'] = phone
            if pd.isnull(df.at[i, 'Website']):
                df.at[i, 'Website'] = website
            print(f"Processed (phone/website only): {row['Full Address']} -> Phone: {phone}, Website: {website}")
        
        # Save progress every 50 rows to avoid data loss
        if (i + 1) % 50 == 0:
            df.to_excel(output_file, index=False)
            print(f"Progress saved at row {i+1}")
        
        # Sleep for 2 seconds to comply with API rate limits
        sleep(2)

# Final save of the updated DataFrame
df.to_excel(output_file, index=False)
print("Geocoding, phone, and website collection complete. Final output saved to:", output_file)
