from Secured_Master.lib.parcels_lib import *
from pyogrio import read_dataframe




df = read_func('tableau_tax_data_2023_20240403184829')
vacant_updated_puc = read_func('updated_vacant_pucs_2023')

updated_df = df.join(vacant_updated_puc, on='parcel_no', how='left')
updated_df = (
    updated_df
    .withColumn(
        'property_use_code',
        when(col('updated_puc').isNotNull() & (col('updated_puc') != col('property_use_code')),
            col('updated_puc')).otherwise(col('property_use_code'))
    )
    .drop('updated_puc')
)

property_use_mapping = {
    (0, 99): 'Vacant',
    (100, 199): 'SFR',
    (200, 299): 'Rec Centers',
    (300, 349): '2-4Plex',
    (350, 399): 'Apartments',
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
    (5000, 8499): 'Other',
    (8500, 8599): 'Multi-Family',
    (8600, 8799): 'SFR',
    (8800, 8899): 'Other',
    (8900, 8999): 'TBD',
    (9000, 9399): 'Other',
    (9400, 9999): 'Government',
}



# Register the UDF; # Apply the UDF to create the new column
map_property_use_udf = udf(map_property_use_code, StringType())
updated_df = updated_df.withColumn('property_use_code_title', map_property_use_udf(col('property_use_code')))

# Register the UDF; # Apply the mapping function to create the major_category column
map_category_udf = udf(map_category, StringType())
updated_df = updated_df.withColumn('major_category', map_category_udf(col('property_use_code')))

updated_df = (
    updated_df
    .filter(~col('major_category').isin(['Government', 'Agricultural']))
    .withColumn(
        'major_category',
        when(col('major_category').isin(['TBD', 'Other']), lit('Other'))
        .otherwise(col('major_category'))
    )
    # .filter(col('is_valid_zip_city')=='Y')
    # .drop(col('is_valid_zip_city'))
)


current_time = datetime.now().strftime("%Y%m%d%H%M%S")
write_func(updated_df, f'tableau_tax_data_2023_{current_time}')
           