"""
FILE: extract_crossing_inventory_data.py
PURPOSE: Fetches official crossing metadata directly from the U.S. Department 
         of Transportation SODA API using a Crossing ID.
"""

# This page https://datahub.transportation.gov/stories/s/bk9e-kthi links to this other page:
# https://data.transportation.gov/Railroads/Crossing-Inventory-Data-Form-71-Current/m2f8-22s6/about_data
# And from there the "Actions" button at upper right gives an API option among others. The resulting pop-up box gives
# some info including a link to API documentation and a toggle to select SODA2 or SODA3. The API documentation is at this link:
# https://dev.socrata.com/foundry/data.transportation.gov/m2f8-22s6
# The below starter snippet code uses the SODA2 endpoint, but we can assess whether it might make more sense to use the SODA3 one instead, which would involve
# everyone applying for an app token and putting it in the .env file at repo root.


import requests

# Define the SODA2 resource endpoint
soda2_url = "https://data.transportation.gov/resource/m2f8-22s6.json"

# Define the filters to look for our target crossing ID
params = {
    "crossingid": "839211T"
}

try:
    # Make the HTTP GET request
    response = requests.get(soda2_url, params=params)
    
    # Verify the request was successful
    if response.status_code == 200:
        data = response.json()
        
        # Socrata returns a list of records matching the filter
        if data:
            record = data[0]
            
            # Using .get() prevents KeyError if a field happens to be blank/null
            street = record.get("street", "Not Available")
            city = record.get("cityname", "Not Available")
            state = record.get("statename", "Not Available")
            
            # Display results
            print("=== SODA2 Extraction Results ===")
            print(f"Crossing ID: 839211T")
            print(f"Street:      {street}")
            print(f"City:        {city}")
            print(f"State:       {state}")
        else:
            print("No records found matching that Crossing ID.")
    else:
        print(f"API Request Failed. HTTP Status Code: {response.status_code}")
        print(response.text)

except Exception as e:
    print(f"An unexpected error occurred: {e}")