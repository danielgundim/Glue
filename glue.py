import sys
import pytz
import logging
from datetime import datetime
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def log_info(message):
    tz = pytz.timezone('America/Sao_Paulo')
    now = datetime.now(tz)
    logger.info(f"{now.strftime('%Y-%m-%d %H:%M:%S')} - {message}")

# Parse job arguments
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'DB_HOSTNAME', 'DB_NAME', 'DB_USERNAME', 'DB_PASSWORD', 'data_agendamento'])

# Initialize Spark and Glue contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Database connection parameters
db_url = f"jdbc:mysql://{args['DB_HOSTNAME']}/{args['DB_NAME']}"
db_user = args['DB_USERNAME']
db_password = args['DB_PASSWORD']
data_agendamento = args['data_agendamento']
s3_output_path = "s3://your-bucket/path"  # Defina o caminho do S3 aqui

log_info("Iniciando o job de Glue")

# Function to read data based on subparticao and data_agendamento and write to S3
def process_partition(subparticao):
    log_info(f"Iniciando o processamento da subpartição {subparticao}")
    query = f"(SELECT * FROM {args['DB_NAME']} WHERE subparticao = {subparticao} AND data_agendamento = '{data_agendamento}') as subquery"
    
    dyf = glueContext.create_dynamic_frame.from_options(
        connection_type="mysql",
        connection_options={
            "url": db_url,
            "dbtable": query,
            "user": db_user,
            "password": db_password,
            "customJdbcDriverS3Path": "s3://path-to-your-jdbc-driver/mysql-connector-java-8.0.23.jar",
            "customJdbcDriverClassName": "com.mysql.cj.jdbc.Driver"
        },
        transformation_ctx=f"subparticao_{subparticao}"
    )
    
    output_path = f"{s3_output_path}/subparticao={subparticao}"
    glueContext.write_dynamic_frame.from_options(
        frame=dyf,
        connection_type="s3",
        connection_options={"path": output_path},
        format="parquet"
    )
    log_info(f"Finalizando o processamento da subpartição {subparticao}")

# List of subparticoes
subparticoes = list(range(1, 17))

# Create a DataFrame to parallelize the process using Spark
subparticoes_df = spark.createDataFrame(subparticoes, "int").toDF("subparticao")

log_info("Iniciando o processamento paralelo das subpartições")

# Apply process_partition function to each subparticao in parallel
subparticoes_df.rdd.foreach(lambda row: process_partition(row.subparticao))

log_info("Finalizando o processamento paralelo das subpartições")

# Commit job
job.commit()
log_info("Job de Glue finalizado")
