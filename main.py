# %%
import os
os.getcwd()
os.chdir("/mnt/c/Capstone_main")

# %% [markdown]
# ### Importing custom DataPipeline class

# %%
from pyspark_scripts.data_pipeline import DataPipeline
from utils.schema import sdn_schema, add_schema, alt_schema, customer_schema

# %% [markdown]
# Creating a DataPipeline object for ETL tasks

# %%
data = DataPipeline(pipeline_name="Transformer")

# %% [markdown]
# ## 1. Data Ingestion

# %%
customer_data = "Customer_data.csv"
codes_data = "CODES.csv"
sdn_data = "sdn.csv"
alt_data = "alt.csv"
add_data = "add.csv"

# %% [markdown]
# ##### A. Reading Customer Data

# %%
customers_df = data.extract(customer_data, schema=customer_schema)
customers_df.show(10, truncate=False)
customers_df.printSchema()

# %% [markdown]
# ##### B. Reading Country codes data

# %%
country_codes_df = data.extract(codes_data, header=True)
country_codes_df.show(10)
country_codes_df.printSchema()

# %% [markdown]
# ##### C. Reading Specially Designated Nationals (SDN) data

# %%
sdn_df = data.extract(sdn_data, schema=sdn_schema)
sdn_df.show(10, truncate=False)
sdn_df.printSchema()

# %% [markdown]
# ##### D. Reading Alternate names data

# %%
alt_df = data.extract(alt_data, schema=alt_schema)
alt_df.show(10, truncate=False)
alt_df.printSchema()

# %% [markdown]
# ##### E. Reading Address data

# %%
add_df = data.extract(add_data, schema=add_schema)
add_df.show(10, truncate=False)
add_df.printSchema()

# %% [markdown]
# ## 2. Data Transformations

# %% [markdown]
# #### A. Customer Data Transformations
# 
# > Our `transform_customer_data()` below does the following transformations:
# >
# > - Concatenating `first_name` and `last_name` of <b>Customer_Data</b>
# >
# > - Modifying different date formats of `Date_of_Birth` to a single format.
# >
# > - Removing duplicate rows from <b>Country Codes</b> table
# >
# > - Getting full country names for corresponding `Country codes` in <b>Customer Data</b>

# %%
customers_data_processed_df = data.transform_customer_data(customers_df, country_codes_df)

# %%
customers_data_processed_df.show(truncate=False)

# %% [markdown]
# #### B. SDN data Transformations
# 
# > Our `transform_sdn_data()` below does the following transformations:
# >
# > 1. Cleaning sdn data- *Formatting null values, Filtering out columns*.
# >
# > 2. Extracting multiple aka names as **AKA** from Remarks.
# >
# > 3. Extracting Date of birth as **DOB** from Remarks.
# >
# > 4. Extracting Place of birth as **POB** from Remarks.
# >
# > 5. Extracting multiple Nationality countries as **Nationality** from Remarks.
# >
# > 6. Extracting multiple Passport countries as **Passport_country** from Remarks.
# >
# > 7. Extracting Gender as **Gender** from Remarks.

# %%
sdn_transformed_df = data.transform_sdn_data(sdn_df)

# %%
col_name = "Nationality"
sdn_transformed_df.select("Ent_num", col_name, "POB_country").filter(f"{col_name} IS NOT NULL AND SIZE(SPLIT({col_name}, ';')) > 1").show(truncate=False)
sdn_transformed_df.printSchema()

# %% [markdown]
# #### C. Alt Names Data Transformations
# 
# > Our `transform_alt_data()` below does the following transformations:
# >
# > 1. Cleaning alt data- *Formatting null values, Filtering out columns*.
# >
# > 2. Filtering **aka** type alt names
# >
# > 3. Aggregating multiple alt names for same entity as **Alt_names**

# %%
alt_transformed_df = data.transform_alt_data(alt_df)

# %% [markdown]
# #### D. Address Data Transformations
# 
# > Our `transform_add_data()` below does the following transformations:
# >
# > 1. Cleaning add data: *Formatting null values, Filtering out columns, Removing duplicates, Removing null rows*.
# >
# > 2. Aggregating multiple data for same entity
# >
# > 3. Extracting full address as **Residential_address**

# %%
add_transformed_df = data.transform_add_data(add_df)

# %% [markdown]
# #### E. Watchlist Creation Transformations
# 
# > Our `transform_watchlist()` below does the following transformations for watchlist creation:
# >
# > 1. Collecting required watchlist data from Sdn, Alt and Add data.
# >
# > 2. Renaming columns:  
# >    - `Ent_num` as **WATCHLIST_ID**, 
# >    - `Gender` as **WATCHLIST_GENDER**, 
# >    - `POB` as **WATCHLIST_COUNTRY_OF_BIRTH**, 
# >    - `DOB` as **WATCHLIST_DATE_OF_BIRTH**
# >
# > 3. Merging `SDN_Name`, `AKA` and `Alt_names` to create **WATCHLIST_NAME**.
# >
# > 4. Merging `Nationality`, `Passport_country` to create **WATCHLIST_NATIONALITY**

# %%
watchlist_df = data.transform_watchlist(sdn_df=sdn_transformed_df, alt_df=alt_transformed_df, add_df=add_transformed_df)

# %%
watchlist_df.filter("WATCHLIST_GENDER IS NOT NULL").show()

# %% [markdown]
# ## 3. Compliance Check
# 
# > Our `transform_compliance_alert()` does the following transformations:
# >
# > 1. Creating watchlist name array from WATCHLIST_NAME
# >
# > 2. Checking compliance match between Watchlist data and Customer data:
# >    - When CUSTOMER_NAME is in WATCHLIST_NAME and CUSTOMER_COUNTRY matches either in WATCHLIST_NATIONALITY or WATCHLIST_COUNTRY_OF_BIRTH,
#        generate **MEDIUM ALERT**.
# >
# >    - When along with CUSTOMER_NAME and CUSTOMER_COUNTRY, CUSTOMER_DATE_OF_BIRTH also matches to WATCHLIST_DATE_OF_BIRTH,
#        generate **HIGH ALERT**.

# %%
compliance_match_df = data.transform_compliance_alert(customers_df = customers_data_processed_df, watchlist_df = watchlist_df)

# %%
alerts_df = compliance_match_df.filter("Alert IS NOT NULL")
alerts_df.show(truncate=False)
alerts_df.printSchema()

# %% [markdown]
# ## 4. Data Loading

# %% [markdown]
# ##### Load Watchlist data into DB

# %%
data.load(watchlist_df, data_name="watchlist", type="db", mode="append", primary_key="WATCHLIST_ID")

# %% [markdown]
# ##### Save Watchlist data as file

# %%
data.load(watchlist_df, data_name="watchlist", type="file", mode="overwrite")

# %% [markdown]
# ##### Loading processed Customer data in DB table

# %%
data.load(customers_data_processed_df, data_name="processed_customers_data", type="db", mode="append", primary_key="CUSTOMER_ID")

# %% [markdown]
# ##### Save processed Customer data as file

# %%
data.load(customers_data_processed_df, data_name="processed_customers_data", type="file", mode="overwrite")

# %% [markdown]
# ##### Loading Compliance Alerts data in DB table

# %%
data.load(alerts_df, data_name="compliance_alert", type="db", mode="append", primary_key="CUSTOMER_ID")

# %% [markdown]
# ##### Save Compliance Alerts data as file

# %%
data.load(alerts_df, data_name="compliance_alert", type="file", mode="overwrite")

# %% [markdown]
# ## 5. Visualization

# %%

data.visualize_compliance_alerts(compliance_match_df, 
                                 alert_col_name="Alert",
                                 title="Alerts Distribution")

# %%
data.visualize_compliance_alerts(alerts_df, 
                                 alert_col_name="Alert",
                                 title="Comparison of Alerts")

# %%

data.visualize_country_distribution(
                                        watchlist_df,
                                        country_col_name="WATCHLIST_COUNTRY_OF_BIRTH",
                                        title="Distribution of Watchlisted Entities over the world"
                                    )

# %%

data.visualize_country_distribution(
                                        customers_data_processed_df,
                                        country_col_name="CUSTOMER_COUNTRY",
                                        title="Distribution of Customers over the world"
                                    )

# %%

data.visualize_age_group_distribution(
                                        watchlist_df, 
                                        dob_col_name="WATCHLIST_DATE_OF_BIRTH", 
                                        title="Age Distribution of Watchlisted Entities"
                                    )

# %%

data.visualize_age_group_distribution(
                                        customers_data_processed_df, 
                                        dob_col_name="CUSTOMER_DATE_OF_BIRTH", 
                                        title="Age Distribution of Customers"
                                    )

# %%



