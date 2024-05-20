################################
# Use treasurer office tax data to validate scraped info
# If discrepancy between sources, re-scrape & identify parcels
# with bad parcels, TBD??
################################
from pyspark.sql.functions import *
from pyspark.sql.types import *
from lib.parcels_lib import *
from lib.parcel_udfs import *


backfilled_tax_data_2023 = read_func('bronze/backfilled_tax_data_2023')

treasurer_2m = read_func('bronze/treasurer_two_M_parq', 'parquet')
treasurer_2s = read_func('bronze/treasurer_two_S_parq', 'parquet')

# Define columns to aggregate for treasurer_2m and treasurer_2s separately
cols_to_aggregate_2m = [
    'state_tax', 'county_tax', 'jr_college_tax', 'school_tax', 'city_tax',
    'flood_tax', 'cawcd_tax', 'bond_tax', 'other_tax',
    'fire_dist_tax', 'library_tax', 'health_tax', 'elem_sec_tax', 'hs_sec_tax',
    'elem_state_aid_amt', 'hs_state_aid_amt', 'addnl_elem_state_aid_amt', 'addnl_hs_state_aid_amt'
]

cols_to_aggregate_2s = [
    'spec_dist_tax', 'spec_dist_state_aid'
]

# Aggregate treasurer data for treasurer_2m and treasurer_2s separately
def aggregate_treasurer(df, cols_to_aggregate):
    return (
        df.groupBy('tax_ext_parcel_book_map_item')
          .agg(*[sum(col(c)).alias(c) for c in cols_to_aggregate])
    )

agg_treasurer_2m = aggregate_treasurer(treasurer_2m, cols_to_aggregate_2m)
agg_treasurer_2s = aggregate_treasurer(treasurer_2s, cols_to_aggregate_2s)

# Define columns to add for treasurer_tax calculation
cols_to_add = [
    'state_tax', 'county_tax', 'jr_college_tax', 'school_tax', 'city_tax',
    'flood_tax', 'cawcd_tax', 'bond_tax', 'other_tax',
    'fire_dist_tax', 'library_tax', 'health_tax', 'elem_sec_tax', 'hs_sec_tax', 'spec_dist_tax'
]

# Define columns to subtract as subsidies for treasurer_tax calculation
cols_to_subtract = [
    'elem_state_aid_amt', 'hs_state_aid_amt', 'addnl_elem_state_aid_amt',
    'addnl_hs_state_aid_amt', 'spec_dist_state_aid'
]

treasurer_taxes_parcel_data = (
    backfilled_tax_data_2023.alias('a')
    .join(agg_treasurer_2m.alias('b'), col('a.parcel_no') == col('b.tax_ext_parcel_book_map_item'), 'left')
    .join(agg_treasurer_2s.alias('c'), col('a.parcel_no') == col('c.tax_ext_parcel_book_map_item'), 'left')
    # .select('a.*', 'b.*', 'c.*')
    .select('a.*', *cols_to_add, *cols_to_subtract)
)


# Calculate treasurer_tax by summing columns to add and subtracting columns as subsidies
treasurer_taxes_parcel_data = (
    treasurer_taxes_parcel_data
    .withColumn('tax_additions', expr(" + ".join(cols_to_add)))
    .withColumn('tax_subtractions', expr(" + ".join(cols_to_subtract)))
    .withColumn('treasurer_tax', col('tax_additions') - col('tax_subtractions'))
    .withColumn('diff_percentage', abs((col('treasurer_tax') - col('tax_2023')) / col('tax_2023')) * 100)
    .withColumn('treasurer_verified', 
                when(col('diff_percentage') > 10, lit('N')).otherwise(lit('Y'))
    )
)

unverified_tax_data = (
    treasurer_taxes_parcel_data
    .filter(col('treasurer_verified')=='N')
    # .limit(100)
)

rescraped_taxes = (
    unverified_tax_data
    .rdd.map(lambda row: (row['parcel_no'], make_request(row['parcel_no']))).toDF(['parcel_no', 'response_code'])
    .withColumnRenamed('response_code', 'rescraped_treasurer_tax')
)

rescraped_treasurer_taxes = unverified_tax_data.join(rescraped_taxes, on='parcel_no')
write_func(rescraped_treasurer_taxes, 'bronze/rescraped_treasurer_taxes')

rescraped_treasurer_taxes = read_func('bronze/rescraped_treasurer_taxes').select('parcel_no', 'rescraped_treasurer_tax')

final_taxes_dataset = (
    treasurer_taxes_parcel_data
    .join(rescraped_treasurer_taxes, on='parcel_no', how='left')
    .withColumn('tax_2023', coalesce(col('rescraped_treasurer_tax'), col('tax_2023')))
    .withColumn('new_diff_percentage', abs((col('treasurer_tax') - col('tax_2023')) / col('tax_2023')) * 100)
    .withColumn('treasurer_verified', when(col('new_diff_percentage') > 10, lit('N')).otherwise(lit('Y'))
    )
)

write_func(
    (
        final_taxes_dataset
        # .filter(col('treasurer_verified')=='Y')
        .drop('diff_percentage', 'rescraped_treasurer_tax', 'new_diff_percentage')
    ),
    'bronze/treasurer_verified_tax_data_2023'
)
