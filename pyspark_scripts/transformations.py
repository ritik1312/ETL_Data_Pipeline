from pyspark.sql import DataFrame
from config.logging_config import logger
from utils.exception import TransformationFailureException
import pyspark.sql.functions as f
import utils.const_messages as message



class DataTransformer:

    def __init__(self, sparkSession):
        self.spark = sparkSession
        logger.info(f"{message.OBJECT_INITIALIZATION} {self.__class__.__name__}.")



    def merge_customer_names(self, customers_df:DataFrame) -> DataFrame:

        try:
            result_df = customers_df.withColumn(
                                    "Customer_name", 
                                    f.concat_ws(' ', f.col('first_name'), f.col('last_name'))
                                )

            logger.info("Merged customer names in customers data")

        except Exception as error:

            logger.error(f"{message.TRANSFORMATION_FAILED} {self.merge_customer_names.__name__} on customer data. Error details: {str(error)}")
            raise TransformationFailureException("Failed to merge customer names in customers data !")
            
        return result_df



    def change_date_format(self, customers_df:DataFrame) -> DataFrame:

        dob = "Date_of_birth"

        try:
            result_df = customers_df.withColumn(
                                                dob,
                                                f.coalesce(
                                                    f.to_date(f.col(dob), 'M/d/yyyy'),
                                                    f.to_date(f.col(dob), 'M-d-yyyy'),
                                                    f.to_date(f.col(dob), 'd/M/yyyy'),
                                                    f.to_date(f.col(dob), 'd-M-yyyy'),
                                                    f.to_date(f.col(dob), 'yyyy/M/d'),
                                                    f.to_date(f.col(dob), 'yyyy-m-d')
                                                )
                                            )

            logger.info("Corrected Date_of_birth formats in customers data")

        except Exception as error:

            logger.error(f"{message.TRANSFORMATION_FAILED} {self.change_date_format.__name__} on customers data. Error details: {str(error)}")
            raise TransformationFailureException("Failed to correct date formats in customers data !")

        return result_df
    

    def get_cust_data_with_country(self, customers_df:DataFrame, codes_df:DataFrame) -> DataFrame:

        try:
            #Optimization
            result_df = customers_df.join(
                                            f.broadcast(codes_df), 
                                            customers_df["Country_code"] == codes_df["Country_code"],
                                            "left_outer"
                                    ).select(
                                        customers_df["id"].alias("CUSTOMER_ID"),
                                        customers_df["Customer_name"].alias("CUSTOMER_NAME"),
                                        codes_df["Country"].alias("CUSTOMER_COUNTRY"),
                                        customers_df["Date_of_birth"].alias("CUSTOMER_DATE_OF_BIRTH")
                                    )
            
            logger.info("Retrieved customer data with country names")

        except Exception as error:

            logger.error(f"{message.TRANSFORMATION_FAILED} {self.get_cust_data_with_country.__name__} on customer data. Error details: {str(error)}")
            raise TransformationFailureException("Failed to get customer data with country names !")

        return result_df



    def format_names(self, spark_df:DataFrame, col_name):

        try:
            formatted_names = spark_df.withColumn(col_name, f.regexp_replace(f.col(col_name), ",", ""))

        except Exception as error:

            logger.error(f"{message.TRANSFORMATION_FAILED} {self.format_names.__name__} on sdn data. Error details: {str(error)}")
            raise TransformationFailureException("Failed to format SDN Name in Sdn data !")
        
        return formatted_names


    def extract_all_akas_from_remarks(self, sdn_df:DataFrame) -> DataFrame:

        # aka names are followed by word aka in Remarks
        # Some aka names are enclosed in '' while some in "".
        # Some aka names itself contain ',' in it.

        aka_pattern = r"a\.k\.a\.\s['\"]([^'\";.]+)['\"][;.]"

        try:
            remarks_col = f.col("Remarks")
            akas_col = f.regexp_extract_all(remarks_col, f.lit(aka_pattern), 1)
            result_df = sdn_df.withColumn('AKA',
                                            f.when(remarks_col.isNull() | (f.size(akas_col) == 0), None)\
                                            .otherwise(f.concat_ws(";", akas_col))
                                        )
            logger.info("Extracted all akas from Remarks in Sdn data")
        
        except Exception as error:

            logger.error(f"{message.TRANSFORMATION_FAILED} {self.extract_all_akas_from_remarks.__name__} on sdn data. Error details: {str(error)}")
            raise TransformationFailureException("Failed to extract all akas from Remarks in Sdn data !")
        
        return result_df
    

    def extract_dob_from_remarks(self, sdn_df:DataFrame) -> DataFrame:
        
        dob_pattern = r"DOB\s([^;.]+)[;.]"

        try:
            dob_or_year = f.trim(f.regexp_extract(f.col("Remarks"), dob_pattern, 1))
            length = f.length(dob_or_year)

            result_df = sdn_df.withColumn(
                                            "DOB",
                                            f.when(
                                                length > 4,
                                                f.to_date(dob_or_year, "d MMM yyyy")
                                            ).when(
                                                length == 4,
                                                f.to_date(f.concat(dob_or_year, f.lit("01-01")), "yyyy-MM-dd")
                                            ).otherwise(None)
                                        )

            logger.info("Extracted DOB from Remarks in Sdn data")

        except Exception as error:

            logger.error(f"{message.TRANSFORMATION_FAILED} {self.extract_dob_from_remarks.__name__} on sdn data. Error details: {str(error)}")
            raise TransformationFailureException("Failed to extract DOB from Remarks in Sdn data !")
        
        return result_df
    

    
    def extract_countries_from_remarks(self, sdn_df:DataFrame) -> DataFrame:
        
        pob_country_pattern = r"POB\s(?:[^;.]*,\s)?([^;,.]+)[;.]"
        nat_pattern = r"nationality\s(?:possibly\s)?([^;.]+)[;.]"
        passport_country_pattern = r"Passport.*?\(([^)]+)\)[;.]"


        try:
            remarks_col = f.col("Remarks")
            nationalities = f.regexp_extract_all(remarks_col, f.lit(nat_pattern), 1)
            passport_countries = f.regexp_extract_all(remarks_col, f.lit(passport_country_pattern), 1)
            pob_country_col = f.regexp_extract(f.col("Remarks"), pob_country_pattern, 1)

            nat_col = f.array_union(nationalities, passport_countries)
            
            result_df = sdn_df.withColumn(
                                            "Nationality",
                                            f.when((f.col("Remarks").isNull()) | (f.size(nat_col) == 0), None)\
                                            .otherwise(f.concat_ws(";", nat_col))
                                )\
                                .withColumn(
                                            "POB_country",
                                            f.when(pob_country_col == "", None)\
                                            .otherwise(pob_country_col)
                                )

            logger.info("Extracted Nationality and POB country from Remarks in Sdn data")
        
        except Exception as error:

            logger.error(f"{message.TRANSFORMATION_FAILED} {self.extract_nationality_from_remarks.__name__} on sdn data. Error details: {str(error)}")
            raise TransformationFailureException("Failed to extract nationality and POB country from Remarks in Sdn data !")
        
        return result_df
    



    def extract_gender_from_remarks(self, sdn_df:DataFrame) -> DataFrame:

        gender_pattern = r"Gender\s([^;.]+)[;.]"

        try:
            gender_col = f.regexp_extract(f.col("Remarks"), gender_pattern, 1)

            result_df = sdn_df.withColumn(
                                            "Gender", 
                                            f.when(gender_col == "", None)\
                                            .otherwise(gender_col)
                                        )

            logger.info("Extracted Gender from Remarks in Sdn data")
        
        except Exception as error:

            logger.error(f"{message.TRANSFORMATION_FAILED} {self.extract_gender_from_remarks.__name__} on sdn data. Error details: {str(error)}")
            raise TransformationFailureException("Failed to extract gender from Remarks in Sdn data !")
        
        return result_df



    def filter_aka_types(self, alt_df:DataFrame) -> DataFrame:

        try:
            result_df = alt_df.filter("alt_type = 'aka'")\
                    .select('ent_num', 'alt_name')
            
            logger.info("Filtered aka type Alternate names in alt data")
        
        except Exception as error:

            logger.error(f"{message.TRANSFORMATION_FAILED} {self.filter_aka_types.__name__} on alt data. Error details: {str(error)}")
            raise TransformationFailureException("Failed to get aka type Alternate names in alt data !")
        
        return result_df



    def merge_alt_names_for_same_ent(self, alt_df:DataFrame) -> DataFrame:

        try:
            # Optimization
            result_df = alt_df.repartition("ent_num").groupBy("ent_num").agg(
                f.concat_ws(';', f.collect_list("alt_name")).alias('Alt_names')
            )

            logger.info("Merged alt names for same entity in Alt data")
        
        except Exception as error:

            logger.error(f"{message.TRANSFORMATION_FAILED} {self.merge_alt_names_for_same_ent.__name__} on alt data. Error details: {str(error)}")
            raise TransformationFailureException("Failed to merge alt names for same entity in Alt data !")
        
        return result_df
    

    def get_residential_address(self, add_df:DataFrame) -> DataFrame:

        try:
            res_addr = f.concat_ws(
                                    ", ", 
                                    f.col("Address"), 
                                    f.col("City/State/Province/Postal Code"), 
                                    f.col("Country")
                                )
            result_df = add_df.withColumn(
                                            "Residential_address", 
                                            f.when(res_addr == "", None).otherwise(res_addr)
                                        ).drop("Address", "City/State/Province/Postal Code")
            
            logger.info("Extracted residential address in Add data")

        except Exception as error:

            logger.error(f"{message.TRANSFORMATION_FAILED} {self.get_residential_address.__name__} on add data. Error details: {str(error)}")
            raise TransformationFailureException("Failed to extract residential address in Add data !")
        
        return result_df



    def merge_multiple_countries_for_same_ent(self, add_df:DataFrame) -> DataFrame:

        try:
            # Optimization 
            result_df = add_df.repartition("Ent_num").groupBy("Ent_num").agg(
                f.when(
                    f.size(f.collect_list("Country")) > 0,
                    f.concat_ws(';', f.collect_list("Country"))
                ).otherwise(None).alias('Country'), 
                f.first("Residential_address").alias("Residential_address")
            )

            logger.info("Merged multiple countries for same ent in Add data")

        except Exception as error:

            logger.error(f"{message.TRANSFORMATION_FAILED} {self.get_single_country_for_same_ent.__name__} on add data. Error details: {str(error)}")
            raise TransformationFailureException("Failed to merge multiple countries for same entity in Add data !")
        
        return result_df
    



    def gather_watchlist_data(self, sdn_df:DataFrame, alt_df:DataFrame, add_df:DataFrame) -> DataFrame:
        
        try:
            result_df = sdn_df\
                        .join(alt_df, sdn_df["Ent_num"] == alt_df["ent_num"], "left_outer")\
                        .join(add_df, sdn_df["Ent_num"] == add_df["Ent_num"], "left_outer")\
                        .drop(sdn_df["Remarks"], alt_df["ent_num"], add_df["Ent_num"])\
                        .withColumnRenamed("Ent_num", "WATCHLIST_ID")\
                        .withColumnRenamed("DOB", "WATCHLIST_DATE_OF_BIRTH")\
                        .withColumnRenamed("Gender", "WATCHLIST_GENDER")
            
            logger.info("Collected Watchlist data from Sdn, Alt and Add data")

        except Exception as error:

            logger.error(f"{message.TRANSFORMATION_FAILED} {self.gather_watchlist_data.__name__}. Error details: {str(error)}")
            raise TransformationFailureException("Failed to gather watchlist data from Sdn, Alt and Add data !")
        
        return result_df



    def extract_watchlist_name(self, watchlist_df:DataFrame) -> DataFrame:

        sdn_name_col = watchlist_df["SDN_Name"]
        aka_col = watchlist_df["AKA"]
        alt_names_col = watchlist_df["Alt_names"]

        try:

            result_df = watchlist_df.withColumn(
                                                "WATCHLIST_NAME",
                                                f.when(
                                                    aka_col.isNotNull() & alt_names_col.isNotNull(), 
                                                    f.concat_ws(";", sdn_name_col, aka_col, alt_names_col)
                                                ).when(
                                                    aka_col.isNotNull(), 
                                                    f.concat_ws(";", sdn_name_col, aka_col)
                                                ).when(
                                                    alt_names_col.isNotNull(), 
                                                    f.concat_ws(";", sdn_name_col, alt_names_col)
                                                ).otherwise(sdn_name_col)
                                            ).drop(sdn_name_col, aka_col, alt_names_col)
            logger.info("Created watchlist name in watchlist data")

        except Exception as error:

            logger.error(f"{message.TRANSFORMATION_FAILED} {self.extract_watchlist_name.__name__}. Error details: {str(error)}")
            raise TransformationFailureException("Failed to create watchlist name in watchlist data!")
        
        return result_df
    


    def extract_watchlist_address(self, watchlist_df:DataFrame) -> DataFrame:
        
        nationality_col = f.col("Nationality")
        pob_country_col = f.col("POB_country")
        add_country_col = f.col("Country")


        try:
            nationality_array = f.split(nationality_col, ";")
            
            add_country_array = f.split(add_country_col, ";")

            combined_array = f.array_union(nationality_array, add_country_array)

            wl_nationality_col = f.when(combined_array.isNull(), None)\
                                    .otherwise(
                                        f.concat_ws(
                                                ";", 
                                                f.when(pob_country_col.isNull(), combined_array)\
                                                .otherwise(f.array_except(combined_array, f.array(pob_country_col)))
                                        )
                                    )
        
        
            result_df = watchlist_df.withColumn(
                                                "WATCHLIST_NATIONALITY",
                                                f.when(wl_nationality_col == "", None)\
                                                .otherwise(wl_nationality_col)
                                            )\
                                    .withColumnRenamed("POB_country", "WATCHLIST_COUNTRY_OF_BIRTH")\
                                    .withColumnRenamed("Residential_address", "WATCHPERSON_RESIDENTIAL_ADDRESS")\
                                    .drop("Country", "nationality")

            logger.info("Created watchlist nationality in watchlist data")

        except Exception as error:

            logger.error(f"{message.TRANSFORMATION_FAILED} {self.extract_watchlist_address.__name__}. Error details: {str(error)}")
            raise TransformationFailureException("Failed to create watchlist nationality in watchlist data !")
        
        return result_df

    

    def get_watchlist_names_array(self, watchlist_df:DataFrame) -> DataFrame:

        try:
            watchlist_array = watchlist_df.withColumn("WATCHLIST_NAMES_ARRAY", f.split(f.lower(f.col("WATCHLIST_NAME")), ";"))
            logger.info("Created watchlist names array in Watchlist data.")
        
        except Exception as error:

            logger.error(f"{message.TRANSFORMATION_FAILED} {self.get_watchlist_names_array.__name__} in Watchlist data. Error details: {str(error)}")
            raise TransformationFailureException("Failed to create watchlist names array in Watchlist data !")
        
        return watchlist_array
    

    
    def generate_compliance_match(self, customers_df:DataFrame, watchlist_df:DataFrame) -> DataFrame:

        cust_id_col = customers_df["CUSTOMER_ID"]
        cust_name_col = customers_df["CUSTOMER_NAME"]
        cust_country_col = customers_df["CUSTOMER_COUNTRY"]
        cust_dob_col = customers_df["CUSTOMER_DATE_OF_BIRTH"]
        wl_name_col = watchlist_df["WATCHLIST_NAME"]
        wl_nationality_col = watchlist_df["WATCHLIST_NATIONALITY"]
        wl_pob_col = watchlist_df["WATCHLIST_COUNTRY_OF_BIRTH"]
        wl_dob_col = watchlist_df["WATCHLIST_DATE_OF_BIRTH"]
        wl_names_array_col = watchlist_df["WATCHLIST_NAMES_ARRAY"]

        try:
            # Optimization
            compliance_match_df = f.broadcast(customers_df).join(
                                                    watchlist_df, 
                                                    f.array_contains(wl_names_array_col, f.lower(cust_name_col)),
                                                    "left_outer"
                                                ).select(
                                                    cust_id_col,
                                                    cust_name_col,
                                                    cust_country_col,
                                                    cust_dob_col,
                                                    wl_name_col,
                                                    wl_nationality_col,
                                                    wl_pob_col,
                                                    wl_dob_col,
                                                    f.when(
                                                        wl_name_col.isNotNull(),
                                                        f.when(
                                                            (
                                                                f.array_contains(
                                                                    f.split(f.lower(wl_nationality_col), ";"), 
                                                                    f.lower(cust_country_col)
                                                                )
                                                            ) | (wl_pob_col == cust_country_col),
                                                            f.when(
                                                                wl_dob_col == cust_dob_col, 
                                                                f.lit("HIGH")
                                                                ).otherwise(
                                                                    f.lit("MEDIUM")
                                                                )
                                                        ).otherwise(None)
                                                    ).otherwise(None)
                                                    .alias("ALERT")
                                                )

            logger.info("Successfully generated compliance alerts.")
        
        except Exception as error:

            logger.error(f"{message.TRANSFORMATION_FAILED} {self.generate_compliance_match.__name__}. Error details: {str(error)}")
            raise TransformationFailureException("Failed to generate compliance match alert !")
        
        return compliance_match_df
    



    def extract_age_grps_from_dob(self, date_of_birth_df:DataFrame, dob_col_name, age_grp_col_name) -> DataFrame:

        try:
            age_df = date_of_birth_df.withColumn("Age", (f.date_diff(f.current_date(), f.col(dob_col_name)) / 365.25).cast('int'))

            age_col = f.col("Age")

            age_group_df = age_df.withColumn(
                            age_grp_col_name,
                            f.when(age_col <= 10, "0-10")
                            .when((age_col > 10) & (age_col <= 20), "11-20")
                            .when((age_col > 20) & (age_col <= 30), "21-30")
                            .when((age_col > 30) & (age_col <= 40), "31-40")
                            .when((age_col > 40) & (age_col <= 50), "41-50")
                            .when((age_col > 50) & (age_col <= 60), "51-60")
                            .when((age_col > 60) & (age_col <= 70), "61-70")
                            .when((age_col > 70) & (age_col <= 80), "71-80")
                            .when((age_col > 80) & (age_col <= 90), "71-80")
                            .otherwise(None)
                        )
            
            logger.info("Successfully Extracted age groups from date of birth.")

        except Exception as error:

            logger.error(f"{message.TRANSFORMATION_FAILED} {self.extract_age_grps_from_dob.__name__}. Error details: {str(error)}")
            raise TransformationFailureException("Failed to extract age groups from date of birth !")
        
        return age_group_df
    

