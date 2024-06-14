import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

# Parse job arguments
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'db_url', 'db_table', 'db_user', 'db_password', 's3_output_path'])

# Initialize Spark and Glue contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Database connection parameters
db_url = args['db_url']
db_table = args['db_table']
db_user = args['db_user']
db_password = args['db_password']
s3_output_path = args['s3_output_path']

# Function to read data based on partition number and write to S3
def process_partition(partition_number):
    query = f"(SELECT * FROM {db_table} WHERE numero_partição = {partition_number}) as subquery"
    jdbcDF = spark.read.format("jdbc") \
        .option("url", db_url) \
        .option("dbtable", query) \
        .option("user", db_user) \
        .option("password", db_password) \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .load()
    
    output_path = f"{s3_output_path}/partition={partition_number}"
    jdbcDF.write.mode("overwrite").parquet(output_path)

# List of partitions
partitions = list(range(1, 17))

# Create a DataFrame to parallelize the process using GlueContext
partitions_df = spark.createDataFrame(partitions, "int").toDF("partition_number")

# Use foreachPartition to process each partition in parallel
partitions_df.foreachPartition(lambda partition: [process_partition(row.partition_number) for row in partition])

# Stop the job
job.commit()
