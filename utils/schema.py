from pyspark.sql.types import StructType, StructField, StringType, IntegerType


sdn_schema = StructType([
    StructField("Ent_num", IntegerType(), nullable=False),
    StructField("SDN_Name", StringType()),
    StructField("SDN_Type", StringType()),
    StructField("Program", StringType()),
    StructField("Title", StringType()),
    StructField("Call Sign", StringType()),
    StructField("Vess Type", StringType()),
    StructField("Tonnage", StringType()),
    StructField("GRT", StringType()),
    StructField("Vess_flag", StringType()),
    StructField("Vess_owner", StringType()),
    StructField("Remarks", StringType())
])

add_schema = StructType([
    StructField("Ent_num", IntegerType(), nullable=False),
    StructField("Add_num", IntegerType()),
    StructField("Address", StringType()),
    StructField("City/State/Province/Postal Code", StringType()),
    StructField("Country", StringType()),
    StructField("Add_remarks", StringType())
])

alt_schema = StructType([
    StructField("ent_num", IntegerType(), nullable=False),
    StructField("alt_num", IntegerType()),
    StructField("alt_type", StringType()),
    StructField("alt_name", StringType()),
    StructField("alt_remarks", StringType())
])

customer_schema = StructType([
    StructField("id", IntegerType(), nullable=False),
    StructField("first_name", StringType()),
    StructField("last_name", StringType()),
    StructField("Country_code", StringType()),
    StructField("Date_of_birth", StringType())
])