from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import time
from pyspark.sql.functions import avg
from pyspark.sql.types import StructType, StructField, FloatType



S3_DATA_SOURCE_PATH = "s3://airline-delay-project/input/DelayedFlights-updated.csv"
S3_DATA_OUTPUT_PATH_SELECTED_DATA = "s3://airline-delay-project/output/selected-data"
S3_DATA_OUTPUT_PATH_TIME_LIST = "s3://airline-delay-project/output/time-list"
S3_DATA_OUTPUT_PATH_TIME_BY_DELAY_TYPE = "s3://airline-delay-project/output/time_by_delay_type"

delayTypes = ["CarrierDelay", "NASDelay", "WeatherDelay", "LateAircraftDelay", "SecurityDelay"]

def get_delay_by_delay_type(airlines_delay_df, delay_types):
    time_by_delay_type = []
    results_all = []
    for item in delay_types:
        start_time = time.time()
        result = (airlines_delay_df
            .where((col("Year") >= 2003) & (col("Year") <= 2010) & (col(item) > 0))
            .groupBy("Year")
            .agg(round(avg(item), 3).alias(f"Average_{item}"))
            .orderBy("Year")
        )
        end_time = time.time()
        results_all.append(result)
        time_by_delay_type.append({f"{item}":end_time - start_time})
    return results_all, time_by_delay_type
    
def get_average_value(values_list):
    schema_list = StructType([StructField("value", FloatType(), True)])
    values_df = spark.createDataFrame([(value,) for value in values_list], schema_list)
    avg_value = values_df.agg(avg("value")).collect()[0][0]
    return avg_value
    
def main():
    spark = SparkSession.builder.appName('AirlineData').getOrCreate()
    airlines_df = spark.read.csv(S3_DATA_SOURCE_PATH, inferSchema=True, header=True)
    print("Total number of recods in the data set: %s" % airlines_df.count())
    airlines_df.write.mode('overwrite').parquet(S3_DATA_OUTPUT_PATH_SELECTED_DATA)
    print('Selected data was successfully saved to s3: %s' % S3_DATA_OUTPUT_PATH_SELECTED_DATA)
    delay_by_type, delayTime = get_delay_by_delay_type(airlines_df, delayTypes)
    time_list = []
    start_time = time.time()
    for run in range(1, 6):
        time_per_run,time_by_type  = get_delay_by_delay_type(airlines_df, delayTypes)   
        end_time = time.time()
        time_list.append({f"time_for_run#{run}":end_time - start_time, "by_type":time_by_type})
    time_list.write.mode('overwrite').parquet(S3_DATA_OUTPUT_PATH_TIME_LIST)
    print('Time List data was successfully saved to s3: %s' % S3_DATA_OUTPUT_PATH_TIME_LIST)
    CarrierDelay =[]
    NASDelay = []
    WeatherDelay =[]
    LateAircraftDelay =[]
    SecurityDelay =[]
    for index, item in enumerate(time_list):
        CarrierDelay+=item['by_type'][0].values()
        NASDelay+=item['by_type'][1].values()
        WeatherDelay+=item['by_type'][2].values()
        LateAircraftDelay+=item['by_type'][3].values()
        SecurityDelay+=item['by_type'][4].values()
        
    CarrierDelay_avg =get_average_value(CarrierDelay)
    NASDelay_avg = get_average_value(NASDelay)
    WeatherDelay_avg =get_average_value(WeatherDelay)
    LateAircraftDelay_avg =get_average_value(LateAircraftDelay)
    SecurityDelay_avg =get_average_value(SecurityDelay)
    average_time_by_delay_type_values = [CarrierDelay_avg, NASDelay_avg, WeatherDelay_avg, LateAircraftDelay_avg, SecurityDelay_avg]
    average_time_by_delay_type_values.write.mode('overwrite').parquet(S3_DATA_OUTPUT_PATH_TIME_BY_DELAY_TYPE)
    
if __name__=="__main__":
    start_time = time.time()
    main()
    end_time = time.time()
    print("Query Execution time: ", end_time - start_time, " seconds")
