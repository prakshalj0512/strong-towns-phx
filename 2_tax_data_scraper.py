import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from lib.parcels_lib import *
from lib.parcel_udfs import *

# Initialize SparkSession
spark = SparkSession.builder \
    .appName('tax_data') \
    .getOrCreate()


df = read_func('bronze/raw_100')

# Group by situs_zip and count occurrences & extract list of ZIP codes
zip_counts_df = df.groupBy('situs_zip').count().orderBy(col('count').desc())
zips_list = zip_counts_df.select('situs_zip').rdd.flatMap(lambda x: x).collect()

########################### 
# exclusion logic
###########################

# Get the current directory & # list of all directories in the current directory
current_directory = f'{os.getcwd()}/data/bronze/tax_data_2023/'
directories = [directory for directory in os.listdir(current_directory) if os.path.isdir(os.path.join(current_directory, directory))]

# Print the list of directories
print('Directories in the current directory:')
# zips_to_exclude = [None]
zips_to_exclude = []
for dir in directories:
    if ('results') in dir:
        zips_to_exclude.append(str(dir.split('=')[1]))
    
zips_to_calcuate = list(set(zips_list) - set(zips_to_exclude))
print(zips_to_calcuate)

# OVERRIDE ON APR 21, 2024
zips_to_calcuate = ['N/A', None]

for zip in zips_to_calcuate:
    print(f'-------------------------{zip}--------------------')
    # Apply the function to each row in the DataFrame
    if zip is None:
        df2 = df.filter(col('situs_zip').isNull())
    else:
        df2 = df.filter(col('situs_zip')==zip)
    
    print(df2.count())

    df2 = df2.repartition(24)

    # Call the scraping method & save result under response_code col
    result_df = df2.rdd.map(lambda row: (row['parcel_no'], make_request(row['parcel_no']))).toDF(['parcel_no', 'response_code'])
    result_df.cache()

    ans = df2.join(result_df,on='parcel_no')

    tax_data_df = (
        ans
        .select(
            'parcel_no',
            'situs_zip',
            'owners_name',
            'property_use_code_major',
            'property_use_code',
            'acres_clean',
            col('response_code').alias('tax_2023')
        )
    )
     
    write_func(tax_data_df, f'bronze/tax_data_2023/results={zip}')