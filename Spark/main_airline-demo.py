from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import time
from pyspark.sql.functions import avg
from pyspark.sql.types import StructType, StructField, FloatType



S3_DATA_SOURCE_PATH = "s3://airline-delay-project/input/DelayedFlights-updated.csv"
S3_DATA_OUTPUT_PATH_SELECTED_DATA = "s3://airline-delay-project/output/selected-data"

    
def main():
    spark = SparkSession.builder.appName('AirlineData').getOrCreate()
    airlines_df = spark.read.csv(S3_DATA_SOURCE_PATH, inferSchema=True, header=True)
    print("Total number of recods in the data set: %s" % airlines_df.count())
    selected_data = airlines_df.where((col('Dest')=='JFK') & (col('DepDelay') > 100))
    selected_data.write.mode('overwrite').parquet(S3_DATA_OUTPUT_PATH_SELECTED_DATA)
    print('Selected data was successfully saved to s3: %s' % S3_DATA_OUTPUT_PATH_SELECTED_DATA)

    
if __name__=="__main__":
    start_time = time.time()
    main()
    end_time = time.time()
    print("Query Execution time: ", end_time - start_time, " seconds")
