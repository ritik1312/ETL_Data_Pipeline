from pyspark.sql import DataFrame
from utils.database import MySQLdb
from utils.exception import FileHandlingException, DBHandlingException      # Custom exceptions
from config.logging_config import logger
import utils.const_messages as message


class DataExtracter:
    
    
    def __init__(self, sparkSession):
        self.spark = sparkSession
        self.raw_data_path = "./data/raw"
        logger.info(f"{message.OBJECT_INITIALIZATION} {self.__class__.__name__}.")


    def read_csv(self, file_name, header, schema) -> DataFrame:
        file_path = f"{self.raw_data_path}/{file_name}"
        
        try:
            if schema:
                spark_df = self.spark.read.csv(file_path, schema=schema)
            else:
                spark_df = self.spark.read.csv(file_path, header=header, inferSchema=True)

            logger.info(f"Successfully read csv file: {file_name} as Spark Dataframe")
            return spark_df
        
        except Exception as error:

            logger.error(f"Failed to read csv file: {file_name}. Error details: {str(error)}")
            raise FileHandlingException(f"Failed to read file : {file_name}")
    

    
    def read_db(self, table_name) -> DataFrame:

        try:
            db = MySQLdb(self.spark)
            spark_df = db.readDF(table_name)

            logger.info(f"Successfully read from DB table: {table_name} as Spark Dataframe")
            return spark_df
        
        except Exception as error:

            logger.error(f"Failed to read from DB table: {table_name}. Error details: {str(error)}")
            raise DBHandlingException(f"Failed to read table : {table_name} from DB")
        


    # Single function to handle all data formats
    def extract_data(self, data_source: str, type: str, header: bool, schema) -> DataFrame:
        if type.lower() == "db":
            return self.read_db(table_name=data_source)
        else:
            return self.read_csv(file_name=data_source, header=header, schema=schema)