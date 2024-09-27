import mysql.connector

class MySQLdb:
    
    def __init__(self, spark_session):
        self.spark = spark_session
        self.jdbc_url = "jdbc:mysql://localhost:3306/transformer"
        self.user = "ritik"
        self.password = "rootroot"
        self.driver = "com.mysql.cj.jdbc.Driver"

    
    def readDF(self, table_name):
        df = self.spark.read \
            .format("jdbc") \
            .option("driver", self.driver) \
            .option("url", self.jdbc_url) \
            .option("dbtable", table_name) \
            .option("user", self.user) \
            .option("password", self.password) \
            .load()
        return df
    
    
    def writeDF(self, df, new_table_name, mode):
        df.write \
            .format("jdbc") \
            .mode(mode)\
            .option("driver", self.driver) \
            .option("url", self.jdbc_url) \
            .option("dbtable", new_table_name) \
            .option("user", self.user) \
            .option("password", self.password) \
            .save()
        
    
    def update_table_with_temp(self, table_name, primary_key, columns_to_compare):

        connection = mysql.connector.connect(
            host = "localhost",
            user = self.user,
            password = self.password,
            database = "transformer"
        )
        cursor = connection.cursor()
        
        temp_table_name = "temp"
        set_clause = ", ".join([f"ht.{col} = tt.{col}" for col in columns_to_compare])

        update_query = f"""
            UPDATE {table_name} AS ht
            JOIN {temp_table_name} AS tt
            ON ht.{primary_key} = tt.{primary_key}
            SET {set_clause},
                ht.BD_UPDATE_DT_TM = tt.BD_UPDATE_DT_TM
        """
        
        drop_temp_query = f"DROP TABLE {temp_table_name}"

        cursor.execute(update_query)
        cursor.execute(drop_temp_query)
        connection.commit()

        cursor.close()
        connection.close()