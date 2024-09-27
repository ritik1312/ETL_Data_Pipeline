from pyspark.sql import SparkSession
from pyspark import SparkConf
from utils.exception import SparkSessionException
from config.logging_config import logger


def createSparkSession(pipeline_name):

    jar_path = "./local_jar"

    all_jars = [
            f"{jar_path}/mysql-connector-j-9.0.0.jar",
        ]
    
    conf = SparkConf()
    conf.set('spark.jars', ','.join(all_jars))   # Added configuration to connect with MySQL
    conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")   # Added configuration to handle legacy time parser

    try:
        spark = SparkSession.builder \
                .appName(pipeline_name) \
                .master("local[*]") \
                .config(conf=conf) \
                .getOrCreate()
        
        logger.info(f"Created spark session successfully : {pipeline_name}")

    except Exception as error:

        logger.error(f"Failed to create spark session: {pipeline_name}. Error details: {str(error)}")
        raise SparkSessionException(f"Failed to create spark session: {pipeline_name}")
    
    return spark