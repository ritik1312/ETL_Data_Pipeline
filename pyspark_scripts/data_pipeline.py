from config.spark_config import createSparkSession
from config.logging_config import logger
from pyspark.sql import DataFrame
from pyspark_scripts.extract_data import DataExtracter
from pyspark_scripts.load_data import DataLoader
from pyspark_scripts.transformations import DataTransformer
from pyspark_scripts.visualizations import Visualizer
import utils.const_messages as message
import numpy as np
import os


class DataPipeline:


    # SPARK SESSION CREATION
    def __init__(self, pipeline_name):
        self.spark = createSparkSession(pipeline_name)
        logger.info(f"{message.OBJECT_INITIALIZATION} {self.__class__.__name__}.")



    # DATA EXTRACTION
    def ingest_data(self, data_source):

        extracter = DataExtracter(self.spark)

        spark_df = extracter.extract_data(data_source)

        return spark_df


    # TRANSFORMATIONS

    # CUSTOMER DATA TRANSFORMATIONS
    def transform_customer_data(self, customers_df:DataFrame, country_codes_df:DataFrame):

        transformer = DataTransformer(self.spark)

        merged_cust_names = transformer.merge_customer_names(customers_df)
        merged_cust_names.show(10, truncate=False)

        cust_correct_dates = transformer.change_date_format(merged_cust_names)
        cust_correct_dates.show(10, truncate=False)

        distinct_codes = country_codes_df.dropDuplicates()
        cust_with_country = transformer.get_cust_data_with_country(cust_correct_dates, distinct_codes)
        cust_with_country.show(10, truncate=False)

        logger.info(f"{message.TRANSFORMATION_SUCCESS} customers data.")
        return cust_with_country



    # SDN DATA TRANSFORMATIONS
    def transform_sdn_data(self, sdn_df:DataFrame):

        transformer = DataTransformer(self.spark)

        sdn_cleaned = transformer.format_names(
                        sdn_df.na.replace("-0- ", None).select(['Ent_num', 'SDN_Name', 'Remarks']),
                        "SDN_Name"
                    ) 
        sdn_cleaned.show(10, truncate=False)

        sdn_aka_extract = transformer.extract_all_akas_from_remarks(sdn_cleaned)
        sdn_aka_extract.show(10, truncate=False)
        
        sdn_dob_extract= transformer.extract_dob_from_remarks(sdn_aka_extract)
        sdn_dob_extract.show(10, truncate=False)

        sdn_countries_extract = transformer.extract_countries_from_remarks(sdn_dob_extract)
        sdn_countries_extract.show(10, truncate=False)

        sdn_gender_extract = transformer.extract_gender_from_remarks(sdn_countries_extract)
        sdn_gender_extract.show(10, truncate=False)

        logger.info(f"{message.TRANSFORMATION_SUCCESS} sdn data.")
        return sdn_gender_extract
    


    # ALTERNATE NAMES DATA TRANSFORMATIONS
    def transform_alt_data(self, alt_df:DataFrame):

        transformer = DataTransformer(self.spark)

        alt_cleaned = transformer.format_names(
                        alt_df.na.replace("-0- ", None).select(['ent_num', 'alt_type', 'alt_name']),
                        'alt_name'
                    )
        alt_cleaned.show(10, truncate=False)

        alt_filtered = transformer.filter_aka_types(alt_cleaned)

        merged_alt_names = transformer.merge_alt_names_for_same_ent(alt_filtered)
        merged_alt_names.show(10, truncate=False)

        logger.info(f"{message.TRANSFORMATION_SUCCESS} alt data.")
        return merged_alt_names



    # ADDRESS DATA TRANSFORMATIONS
    def transform_add_data(self, add_df:DataFrame):

        transformer = DataTransformer(self.spark)

        add_cleaned = add_df.na.replace("-0- ", None)\
                            .select(['Ent_num', 'Address', 'City/State/Province/Postal Code', 'Country'])\
                            .na.drop(how="all")
        add_cleaned.show(10, truncate=False)

        add_full_address = transformer.get_residential_address(add_cleaned)
        add_full_address.show(10, truncate=False)

        merged_countries = transformer.merge_multiple_countries_for_same_ent(add_full_address)
        merged_countries.show(10, truncate=False)

        logger.info(f"{message.TRANSFORMATION_SUCCESS} add data.")
        return merged_countries



    # WATCHLIST CREATION TRANSFORMATIONS
    def transform_watchlist(self, sdn_df:DataFrame, alt_df:DataFrame, add_df:DataFrame):
        
        transformer = DataTransformer(self.spark)

        watchlist_data = transformer.gather_watchlist_data(sdn_df, alt_df, add_df)
        watchlist_data.show(10, truncate=False)
        
        watchlist_name_extracted = transformer.extract_watchlist_name(watchlist_data)
        watchlist_name_extracted.show(10, truncate=False)

        watchlist_address_extracted = transformer.extract_watchlist_address(watchlist_name_extracted)
        watchlist_address_extracted.show(10, truncate=False)

        final_watchlist = watchlist_address_extracted.select(
            "WATCHLIST_ID",
            "WATCHLIST_NAME",
            "WATCHLIST_GENDER",
            "WATCHLIST_NATIONALITY",
            "WATCHLIST_COUNTRY_OF_BIRTH",
            "WATCHPERSON_RESIDENTIAL_ADDRESS",
            "WATCHLIST_DATE_OF_BIRTH"
        )
        
        logger.info(f"{message.TRANSFORMATION_SUCCESS} watchlist data.")
        return final_watchlist
    

    
    # COMPLIANCE MATCH TRANSFORMATIONS
    def transform_compliance_alert(self, customers_df:DataFrame, watchlist_df:DataFrame):

        transformer = DataTransformer(self.spark)

        watchlist_array = transformer.get_watchlist_names_array(watchlist_df)
        watchlist_array.show(10, truncate=False)

        alerts = transformer.generate_compliance_match(customers_df, watchlist_array).select(
            "CUSTOMER_ID",
            "CUSTOMER_NAME",
            "CUSTOMER_COUNTRY",
            "CUSTOMER_DATE_OF_BIRTH",
            "ALERT"
        )
        alerts.show(10, truncate=False)

        logger.info(f"{message.TRANSFORMATION_SUCCESS} compliance check.")
        return alerts
    



    # DATA LOADING
    def load(self, spark_df, data_name, type="db", mode="append", primary_key=None):
        
        loader = DataLoader(self.spark)

        loader.load_data(spark_df, data_name, type=type, mode=mode, primary_key=primary_key)




    # VISUALIZATIONS

    # COMPLIANCE ALERT VISUALIZATION
    def visualize_compliance_alerts(self, compliance_df:DataFrame, alert_col_name, title):

        alerts = compliance_df.fillna({alert_col_name: "NO_ALERT"})\
                                .groupBy(alert_col_name).count()\

        x_col_name = "ALERT"
        y_col_name = "COUNT"

        data_to_plot = [{x_col_name: row[alert_col_name], y_col_name: row["count"]} for row in alerts.collect()]

        visualizer = Visualizer(data_to_plot)
        visualizer.get_pie_plot(
                                    values=y_col_name, 
                                    names=x_col_name, 
                                    title=title
                                )
        alerts.show()
        logger.info(f"{message.VISUALIZATION_SUCCESSFULLY} compliance alerts")



    # COUNTRY DISTRIBUTION VISUALIZATION
    def visualize_country_distribution(self, country_df:DataFrame, country_col_name, title):

        country_counts_df = country_df.groupBy(country_col_name).count().dropna()

        x_col_name = "Country"
        y_col_name = "Count"
        data_to_plot = [{x_col_name: row[country_col_name], y_col_name: row["count"]} for row in country_counts_df.collect()]

        visualizer = Visualizer(data_to_plot)
        visualizer.get_geo_map_plot(
                                        locations_name=x_col_name,
                                        color=y_col_name,
                                        title=title
                                    )
        country_counts_df.show()
        logger.info(f"{message.VISUALIZATION_SUCCESSFULLY} country distribution")



    # AGE GROUP DISTRIBUTION VISUALIZATION
    def visualize_age_group_distribution(self, date_of_birth_df:DataFrame, dob_col_name, title):


        transformer = DataTransformer(self.spark)

        age_grp_col = "Age_Group"

        age_group_df = transformer.extract_age_grps_from_dob(date_of_birth_df, dob_col_name, age_grp_col_name=age_grp_col)

        age_grp_counts_df = age_group_df.groupBy(age_grp_col).count().dropna()\
                                        .orderBy(age_grp_col)

        x_col_name = "Age_Grp"
        y_col_name = "Count"
        data_to_plot = [{x_col_name: row["Age_Group"], y_col_name: row["count"]} for row in age_grp_counts_df.collect()]

        visualizer = Visualizer(data_to_plot)
        visualizer.get_bar_plot(
                                    x_col=x_col_name, 
                                    y_col=y_col_name, 
                                    title=title
                                )

        age_grp_counts_df.show()
        logger.info(f"{message.VISUALIZATION_SUCCESSFULLY} age group distributions")    