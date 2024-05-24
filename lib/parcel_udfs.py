from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import requests
import re


# Define the UDF to map property use codes to titles
property_use_mapping = {
    (0, 99): 'Vacant',
    (100, 199): 'SFR',
    (200, 299): 'Rec Centers',
    (300, 349): '2-4Plex',
    (350, 359): 'Small Apartments (<25)',
    (360, 379): 'Large Apartments (25+)',
    (380, 399): 'Condos',  # apt co-ops
    (400, 499): 'Hotels',
    (500, 599): 'Motels',
    (600, 699): 'Resorts',
    (700, 799): 'Condos',
    (800, 899): 'Mobile Home',
    (900, 999): 'Salvage',
    (1000, 1099): 'Commercial (Misc)',
    (1100, 1199): 'Retail',
    (1200, 1299): 'Retail/Office Combo',
    (1300, 1399): 'Retail (Dept Stores)',
    (1400, 1499): 'Retail (Shopping Centers)',
    (1500, 1599): 'Office ',
    (1600, 1699): 'Office (Banks)',
    (1700, 1799): 'Gas Station',
    (1800, 1899): 'Car Dealers',
    (1900, 1999): 'Nursing Homes',
    (2000, 2099): 'Retail (Restaurants/Bars)',
    (2100, 2199): 'Medical',
    (2200, 2299): 'Race Tracks',
    (2300, 2399): 'Cemeteries',
    (2400, 2499): 'Golf Courses',
    (2500, 2599): 'Amusement/Recreation',
    (2600, 2699): 'Parking Facilities',
    (2700, 2799): 'Clubs/Lodges',
    (2800, 2899): 'Partial Complete',
    (2900, 2999): 'Private Schools',
    (3000, 3099): 'Industrial Park',
    (3700, 3799): 'Warehouses',
    (4000, 4099): 'Nurseries',
    (4100, 4199): 'Field Crops',
    (4200, 4299): 'Vineyards',
    (4300, 4399): 'Mature Crop Trees',
    (4400, 4499): 'Mature Citrus Trees',
    (4500, 4599): 'High Density Agriculture',
    (4600, 4699): 'Jojoba',
    (4700, 4799): 'Ranch',
    (4800, 4899): 'Pasture',
    (4900, 4999): 'Fallow Land',
    (5000, 6099): 'Utilities',
    (6100, 6999): 'Natural Resources',
    (7000, 8499): 'Personal Property',
    (8500, 8599): 'Townhomes',
    (8600, 8699): 'Multile SF Dwellings',
    (8700, 8799): 'Residence >= 5 acres',
    (8800, 8899): 'Limited Use (?)',
    (8900, 8999): 'Converted Use Property',
    (9000, 9099): 'Tax-Exempt; Private Property',
    (9100, 9199): 'Tax-Exempt; Private Owner',
    (9200, 9299): 'Religious',
    (9300, 9399): 'Exempt',
    (9400, 9499): 'Federal-Owned',
    (9500, 9599): 'State-Owned',
    (9600, 9699): 'County-Owned',
    (9700, 9799): 'Municipally-Owned',
    (9800, 9899): 'Tribal Lands',
    (9900, 9999): 'Special Districts Exempt'
}



# Define property use code ranges and their corresponding categories
code_ranges = {
    (0, 99): 'Vacant',
    (100, 199): 'SFR',
    (200, 299): 'Government',
    (300, 349): 'SFR',
    (800, 899): 'SFR',
    (350, 399): 'Multi-Family',
    (700, 899): 'Multi-Family',
    (400, 699): 'Hotel/Motel/Resort',
    (900, 999): 'Industrial',
    (1000, 1899): 'Commercial',
    (1900, 1999): 'Other',
    (2000, 2099): 'Commercial',
    (2100, 2199): 'Medical',
    (2200, 2599): 'Other',
    (2700, 2999): 'Other',
    (2600, 2699): 'Parking',
    (3000, 3799): 'Industrial',
    (4000, 4999): 'Agricultural',
    (5000, 6999): 'Government',
    (7000, 8499): 'Other',
    (8500, 8599): 'Multi-Family',
    (8600, 8799): 'SFR',
    (8800, 8899): 'Other',
    (8900, 8999): 'TBD',
    (9000, 9299): 'Other',
    (9300, 9999): 'Government',
}

# Create a function to map property use codes to categories
def map_category(property_use_code):
    for code_range, category in code_ranges.items():
        if code_range[0] <= int(property_use_code) <= code_range[1]:
            return category
    return None


def map_property_use_code(property_use_code):
    for code_range, title in property_use_mapping.items():
        if code_range[0] <= int(property_use_code) <= code_range[1]:
            return title
    return 'N/A'

# Function to make HTTP GET request
def make_request(parcel_no):
    url = f'https://treasurer.maricopa.gov/parcel/default.aspx?Parcel={parcel_no}'
    print(url)
    try:
        response = requests.get(url)
        x=response.text
        match = re.search(r'\$\d+(,\d+)*\.\d+', x)
        dollar_amount = match.group(0)
        cleaned_string = re.sub(r'[^0-9.]', '', dollar_amount)
        return cleaned_string
    except Exception as e:
        return str(e)
        