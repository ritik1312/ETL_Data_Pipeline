from pyspark.sql import DataFrame
import pyspark.sql.functions as f
from utils.database import MySQLdb
from utils.exception import FileHandlingException, DBHandlingException      # Custom exceptions
from config.logging_config import logger
import utils.const_messages as message
import os, glob


class DataLoader:

    def __init__(self, sparkSession):
        self.spark = sparkSession
        self.processed_data_path = "./data/processed"
        logger.info(f"{message.OBJECT_INITIALIZATION} {self.__class__.__name__}.")


    def write_csv(self, spark_df, file_name, mode):

        file_path = f"{self.processed_data_path}/{file_name}"

        try:

            spark_df.write.csv(file_path, header=True, mode=mode)

            part_files = glob.glob(f"./{file_path}/part-*.csv")

            for file_num, part_file in enumerate(part_files):
                os.rename(part_file, f"{file_path}/{file_name}{file_num + 1}.csv")

            logger.info(f"Successfully saved data: {file_name} locally at: {file_path}")

        except Exception as error:

            logger.error(f"Failed to save data: {file_name} locally. Error details: {str(error)}")
            raise FileHandlingException(f"Failed to save {file_name} locally :(")



    def write_db(self, spark_df:DataFrame, table_name, mode, primary_key):
        
        spark_df = spark_df.withColumn("BD_CREATE_DT_TM", f.current_timestamp())\
                            .withColumn("BD_UPDATE_DT_TM", f.current_timestamp())
        
        try:
            db = MySQLdb(self.spark)

            try:
                historical_data = db.readDF(table_name)
                table_exists = True

            except Exception as error:
                logger.info(f"Table '{table_name}' does not exist. Creating one.")
                table_exists = False
                
            if not table_exists:
                db.writeDF(spark_df, table_name, mode=mode)
                logger.info(f"Table '{table_name}' created successfully.")
            
            else:
                joined_df = spark_df.alias("new").join(
                    historical_data.alias("old"),
                    on=primary_key,
                    how="left"
                )

                # New records
                new_records = joined_df.filter(f.col("old." + primary_key).isNull()).select("new.*")

                if(new_records.count() != 0):
                    db.writeDF(new_records, table_name, mode="append")
                    logger.info(f"Added new records in table {table_name}")
                else:
                    logger.info(f"No new records to load into {table_name}")


                # Updated records
                updated_records = joined_df.filter(
                    (f.col("old." + primary_key).isNotNull()) &
                    f.expr(" OR ".join([f"new.{col} != old.{col}" for col in spark_df.columns if col not in ["BD_CREATE_DT_TM", "BD_UPDATE_DT_TM"]]))
                )


                if(updated_records.count() != 0):

                    db.writeDF(updated_records, "temp", mode="overwrite")

                    # Updating historical table with new data in temp table
                    columns_to_compare = [col for col in spark_df.columns if col != primary_key]
                    db.update_table_with_temp(table_name, primary_key, columns_to_compare)
                    logger.info(f"Updated records in table {table_name}.")
                else:
                    logger.info(f"No updated records to load into {table_name}")


        except Exception as error:

            logger.error(f"Failed to load {table_name} into DB. Error details: {str(error)}")
            raise DBHandlingException(f"Failed to load {table_name} into DB :(")

    

    # Single function to handle all data formats
    def load_data(self, spark_df: DataFrame, data_name: str, type: str, mode: str, primary_key:str):
        if type == "file":
            self.write_csv(spark_df, file_name=data_name, mode=mode)
        elif type == "db":
            self.write_db(spark_df, table_name=data_name, mode=mode, primary_key=primary_key)
        else:
            return ValueError(f'Unsupported type: {type}. Can save as local file or in db only!')