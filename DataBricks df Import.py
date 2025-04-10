# Databricks notebook source
# MAGIC %md
# MAGIC #####  reportingLake = "azurestoreprod.dfs.core.windows.net/"
# MAGIC ##### Container="container"
# MAGIC ##### name ="/folder1/folder2/folder3/"
# MAGIC ##### Path="abfss://"+reportingContainer+"@"+reportingLake+name+"/*"
# MAGIC ##### python_df = spark.read.option("header","true").option("recursiveFileLookup","true").parquet(Path)
# MAGIC ##### SQL_table_fm_pydf = python_df.createOrReplaceTempView("sql_table")
# MAGIC ##### or SQL_table from PARQUET FILE PATH 
# MAGIC ##### DROP TABLE IF EXISTS SQL_TABLE;
# MAGIC
# MAGIC #####-- Create a SQL Table with specified schema
# MAGIC ####CREATE OR REPLACE TEMP VIEW SQL_TABLE AS
# MAGIC ####SELECT *
# MAGIC ####FROM parquet.`abfss://CONTAINER@REPORTINGLAKE/NAME/*`;

# COMMAND ----------

# Create financial year and peroid - need later
period = "2024-09-31"
year = "2024-25"
FY = "FY"+year
Quarter = "Q3" # Get latest Quarter

#Lake address
reportingLake = "azureconnectionprod.dfs.core.windows.net/"
# Containers
restrictedContainer="restricted"
unrestrictedContainer="unrestricted"

# COMMAND ----------

# DBTITLE 1,Function to import Python DF
def import_py_df(lake, container, foldername):
  Path="abfss://"+container+"@"+lake+foldername+"/*"
  df = spark.read.option("header","true").option("recursiveFileLookup","true").parquet(Path)
  return df
  

# COMMAND ----------

# DBTITLE 1,Import all Data Frames
# Set up folder paths for df import
latest_qx ="/name1/folder1/folder2/2/"+FY+Quarter
df_archive = "/name1/folder1/archive/2/"
date_ref = "/reference/folder/datesfolder/"  
lookup_prov = "/reference/lookup/Provider/"

# Import df using function above
latest_df = import_py_df(reportingLake, restrictedContainer, latest_qx) #imports latest df
latest_df = latest_df .filter(latest_df.Period == period)  # filter on specified month end date

archive_df = import_py_df(reportingLake, restrictedContainer, df_archive) 

date_df = import_py_df(reportingLake, unrestrictedContainer, date_ref) 
lookup_prov_df = import_py_df(reportingLake, unrestrictedContainer, lookup_prov)


# COMMAND ----------

# DBTITLE 1,Create SQL Tables - from python data frames
# These sql tables can be called as tables in SQL queries
# df..createOrReplaceTempView("new_sql_table_name")
latest_df.createOrReplaceTempView("sql_latest")
archive_df.createOrReplaceTempView("sql_archive")
date_df.createOrReplaceTempView("sql_date")
lookup_prov_df.createOrReplaceTempView("sql_lookup_prov")

# COMMAND ----------

# DBTITLE 1,Dummy SQL query
# MAGIC %sql
# MAGIC
# MAGIC SELECT 
# MAGIC SUM(l.id) as total
# MAGIC ,CAST(d.published_date AS DATE) AS published_date
# MAGIC , p.region
# MAGIC FROM 
# MAGIC   sql_latest AS l
# MAGIC WHERE  
# MAGIC   Prov_type = 'NHS ACUTE' 
# MAGIC AND
# MAGIC   l.latest_flag = 1
# MAGIC INNER JOIN 
# MAGIC   sql_date AS d
# MAGIC   on l.period = d.period
# MAGIC INNER JOIN 
# MAGIC   sql_lookup_prov AS p
# MAGIC   ON l.code = p.org_code
# MAGIC GROUP BY 
# MAGIC   d.published_date
# MAGIC   p.region
# MAGIC

# COMMAND ----------

# DBTITLE 1,Change SQL output to Python df
transformed_df = _sqldf

# COMMAND ----------

# DBTITLE 1,set up new folder location
# Creates a directory at the specified Azure storage location
location_folder = "/project_folder/team/folder/"
delta_path="abfss://"+unrestrictedContainer+"@"+reportingLake+location_folder

#this will create a new folder in the location specified
dbutils.fs.mkdirs(delta_path)

# COMMAND ----------

# DBTITLE 1,Write data to Delta table
# folder location
location_folder = "/project_folder/team/folder/"

delta_path="abfss://"+unrestrictedContainer+"@"+reportingLake+location_folder
transformed_df.write.format("delta").save(delta_path)


# COMMAND ----------

# DBTITLE 1,Importing data from data written out
# once saved can import the data saved like below
import_delta_df = spark.read.format("delta").load(delta_path)
