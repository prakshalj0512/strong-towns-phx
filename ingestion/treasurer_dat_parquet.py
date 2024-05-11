################################
# Convert dat file provided by the treasurer office to usable parquet files
# break treasurer parquet into 3 separate ones, for 2M, 2S, 2P records
################################
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from lib.parcels_lib import *
import yaml


data = read_func('source/tax_ext_2023-003.dat', format='text')

# Define the expression to split the text into records after every 550 characters
split_expression = expr("split(regexp_replace(value, '\\n', ''), '(?<=\\G.{550})') as records")
split_data = data.select(split_expression)
exploded_data = split_data.selectExpr('explode(records) as record')

write_func(exploded_data, 'bronze/tax_ext_2023_parq', format='parquet')

################################
# split into 3 separate ones: 2M, 2S, 2P records
################################

exploded_data = read_func('bronze/tax_ext_2023_parq', format='parquet')
letters = ['M', 'S', 'P']

# Construct the path to the YAML file relative to the script's directory
script_dir = os.path.dirname(__file__)
yaml_path = os.path.join(script_dir, '../config/treasurer.yaml')

for letter in letters:
    data = exploded_data.filter(substring('record', 12, 2) == f'2{letter}')
    
    with open(yaml_path, 'r') as file:
        field_lengths = yaml.safe_load(file)[f'two_{letter}_field_lengths']

    # Calculate the starting position for each field
    field_start_positions = {}
    start_position = 1
    for field, length in field_lengths.items():
        field_start_positions[field] = start_position
        start_position += length
    # Convert single column to multiple columns based on field lengths
    for field, start_pos in field_start_positions.items():
        data = data.withColumn(field, substring(col('record'), start_pos, field_lengths[field]))
    
    # Set the last 2 digits as decimal for specified columns
    two_decimal_cols = [
        'state_tax', 'county_tax', 'jr_college_tax', 'school_tax', 'city_tax', 'tot_primary_half_tax',
        'flood_tax', 'cawcd_tax', 'bond_tax', 'other_tax', 'fire_dist_tax', 'library_tax', 'health_tax',
        'elem_sec_tax', 'hs_sec_tax', 'tot_sec_half_tax', 'elem_eaf', 'hs_eaf', 
        'spec_dist_tax', 'spec_dist_half_tax', 'spec_dist_state_aid', 'fund_x_eaf',
    ]
    for col_name in two_decimal_cols:
        if col_name in data.columns:
            data = data.withColumn(col_name, (col(col_name).cast('double') / 100).cast('decimal(18,2)'))
    
    four_decimal_cols = [
        'state_rate', 'county_rate', 'school_rate', 'jr_college_rate', 'city_rate', 'area_code_tax_rate', 'flood_rate', 'elem_state_aid_amt', 'hs_state_aid_amt',
        'addnl_elem_state_aid_amt', 'addnl_hs_state_aid_amt',
        'cawcd_rate', 'bond_rate', 'other_rate', 'fire_dist_rate', 'library_rate', 'health_rate', 'elem_sec_rate', 'hs_sec_rate', 'spec_dist_tax_rate',
    ]
    for col_name in four_decimal_cols:
        if col_name in data.columns:
            data = data.withColumn(col_name, (col(col_name).cast('double') / 10000).cast('decimal(18,4)'))
    
    write_func(data, f'bronze/treasurer_two_{letter}_parq', format='parquet')
    
