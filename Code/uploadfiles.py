# Databricks notebook source
# MAGIC %sql
# MAGIC SHOW DATABASES

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC java.lang.Runtime.getRuntime.availableProcessors * (sc.statusTracker.getExecutorInfos.length -1)
# MAGIC java.lang.Runtime.getRuntime.availableProcessors 
# MAGIC sc.statusTracker.getExecutorInfos.length

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DESCRIBE DATABASE default;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SHOW TABLES

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DESCRIBE TABLE allstatements

# COMMAND ----------

dbutils.fs.ls('dbfs:/user/hive/warehouse/')

dbutils.fs.ls('dbfs:/user/hive/warehouse/allstatements/')
dbutils.fs.ls('dbfs:/FileStore/tables')
#dbutils.fs.ls('dbfs:/user/hive/warehouse/')
dbutils.fs.ls('/mnt')
dbutils.fs.ls('/mnt/all-files')
#dbutils.fs.ls('/mnt/all-files/bankfiles')
#dbutils.fs.ls('/mnt/all-files/IncomingStatements/')

dbutils.fs.ls('/mnt/all-files/myfiles/')

myfilePath = '/mnt/all-files/myfiles/MyTransactionsWithWellsChecking.txt'
mytranswithoutmounting = spark.read.format('csv').options(header='true', inferSchema='true', sep ='\t').load(myfilePath)

display(mytranswithoutmounting)

mytranswithoutmounting.select('*')

mytranswithoutmounting.write.mode("overwrite").saveAsTable(name = "mytranswithoutmountingSavedTable")



# COMMAND ----------

#display(mytranswithoutmounting)
df = mytranswithoutmounting.select('*')
display(df)

df.createOrReplaceTempView('v_temp_mytranswithoutmounting')

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- SELECT * FROM mytranswithoutmounting; -- this fails becuase a table does not get created and is not accessible from sql
# MAGIC 
# MAGIC SELECT * FROM v_temp_mytranswithoutmounting -- this succeeds because a view is access from sql;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- SHOW CREATE TABLE mytranswithoutmounting; this fails
# MAGIC 
# MAGIC SHOW CREATE TABLE v_temp_mytranswithoutmounting -- this fails because it must be a permanent view 

# COMMAND ----------

# MAGIC %sql 
# MAGIC SHOW CREATE TABLE mytranswithoutmountingSavedTable

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Creating a table defined over a CSV file without creating a parquet copy of the file
# MAGIC DROP TABLE  IF EXISTS f_mytranswithoutmounting;
# MAGIC -- This creates a table, but always will just use the original CSV file and will not copy to a parquet file
# MAGIC 
# MAGIC CREATE TABLE spark_catalog.default.f_mytranswithoutmounting (TranDate STRING,   TranRef INT,   TranAmount DOUBLE,   Descritpion STRING) 
# MAGIC USING CSV OPTIONS ('header' = 'true', 'inferSchema' = 'true', 'sep' = '\t')
# MAGIC LOCATION '/mnt/all-files/myfiles/MyTransactionsWithWellsChecking.txt';
# MAGIC 
# MAGIC SELECT * FROM spark_catalog.default.f_mytranswithoutmounting;
# MAGIC 
# MAGIC SHOW CREATE TABLE spark_catalog.default.f_mytranswithoutmounting

# COMMAND ----------

# MAGIC %sql
# MAGIC -- CREATE A permanent VIEW FROM A TABLE which is read directly from a CSV file (not a parquet copy of the csv)
# MAGIC DROP VIEW  IF EXISTS v_mytranswithoutmounting;
# MAGIC -- This creates a table, but always will just use the original CSV file and will not copy to a parquet file
# MAGIC 
# MAGIC CREATE VIEW spark_catalog.default.v_mytranswithoutmounting AS 
# MAGIC SELECT TranDate,   TranRef,   TranAmount ,   Descritpion as `Description`, substring(Descritpion,2,3) as `Description_2_3`  
# MAGIC FROM f_mytranswithoutmounting ;
# MAGIC 
# MAGIC SELECT * FROM spark_catalog.default.v_mytranswithoutmounting;
# MAGIC 
# MAGIC SHOW CREATE TABLE spark_catalog.default.v_mytranswithoutmounting;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT *, instr(Description, 'Bill'), levenshtein(Description,'Bill'), sentences(Description) FROM spark_catalog.default.v_mytranswithoutmounting
# MAGIC --WHERE regexp(Description,'[A-M]*A');
# MAGIC WHERE levenshtein(Description,'Bill') BETWEEN length('Bill') AND (length(Description) - length('Bill')) 
# MAGIC --SHOW CREATE TABLE v_mytranswithoutmounting;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE EXTENDED v_mytranswithoutmounting;
# MAGIC DESCRIBE TABLE EXTENDED f_mytranswithoutmounting;
# MAGIC 
# MAGIC DESCRIBE TABLE EXTENDED mytrans

# COMMAND ----------

# MAGIC %sh
# MAGIC 
# MAGIC cat /user/hive/warehouse/allstatements/part-00000-97f95be4-849b-49b9-9c65-e23077798094-c000.snappy.parquet

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM mytrans

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM allstatements

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DESCRIBE TABLE EXTENDED banktrans

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM banktrans

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SHOW FUNCTIONS 

# COMMAND ----------

# Connection Type 1: Connect and Mount to blob container using account key
# Connect to account storage blob container using account key
# using  "wasbs:"

dbutils.help()
dbutils.fs.help()

adlsStorageAccountName = "reconciliationdevdl"

#option 1 Using Account Key
accountKey ="oFDRYNyEVaQaGLfHGdrObtX7qh3QsgaC86olov37EDSR+q1vEBkJu3RUg8gi1ZSAieZVzOIFx9up+AStnsn55w=="

#config = "fs.azure.account.key.<StorageAccountName>.blob.core.windows.net"
config = "fs.azure.account.key." + adlsStorageAccountName + ".blob.core.windows.net"

print(config)
spark.conf.set(config,accountKey)

adlsContainerName = "file-uploads"

#dbutils.fs.ls("wsabs://<containername>@<storageaccount>.blob.core.windows.net")
adlsRoot = "wasbs://" + adlsContainerName + "@" + adlsStorageAccountName+ ".blob.core.windows.net/"

print(adlsRoot)

dbutils.fs.ls(adlsRoot)

allFilesPath = adlsRoot + "all-files"
dbutils.fs.ls(allFilesPath)

statementPaths = allFilesPath + "/Statements"
display(dbutils.fs.ls(statementPaths))

#read file without mounting
myfilePath = allFilesPath + "/myfiles/MyTransactionsWithWellsChecking.txt"
mytrans = spark.read.format('csv').options(header='true', inferSchema='true', sep ='\t').load(myfilePath)

display(mytrans)

mytrans.select('*')

#read file after mounting
mountPoint = "/mnt/all-files"
config = { config : accountKey }
if not any(mount.mountPoint == mountPoint for mount in dbutils.fs.mounts()):
  dbutils.fs.mount(
    source = allFilesPath,
    mount_point = mountPoint,
    extra_configs = config)

display(dbutils.fs.ls(mountPoint))

myfileMountedPath = mountPoint + "/myfiles/MyTransactionsWithWellsChecking.txt"
mytrans = spark.read.format('csv').options(header='true', inferSchema='true', sep ='\t').load(myfileMountedPath)

display(mytrans)

dbutils.fs.unmount(mountPoint)



# COMMAND ----------

# Connection Type 2: Connect to dfs storage using an Account Key, Mount using secrets
# Connect to account storage dfs (distributed file system?)
# using "abfss:"
# dfs is recommended for very large files.
# You can access "abfss:" protocol directly usin an AccountKey
# But this fails when trying to mount the file system using AccountKey.
# The reason it fails is that you can't mount the ABFSS protocol using the storage key. 
# To mount using the abfss protocal you to use a service Service Principal (docs), and it requires another set of parameters for
# extra_configs: 

dbutils.help()
dbutils.fs.help()

#spark.conf.set("fs.azure.account.key.<storageaccount>.dfs.core.windows.net","<accountkey>")
adlsStorageAccountName = "reconciliationdevdl"

#option 1 Using Account Key
accountKey ="oFDRYNyEVaQaGLfHGdrObtX7qh3QsgaC86olov37EDSR+q1vEBkJu3RUg8gi1ZSAieZVzOIFx9up+AStnsn55w=="

#config = "fs.azure.account.key.<StorageAccountName>.dfs.core.windows.net"
config = "fs.azure.account.key." + adlsStorageAccountName + ".dfs.core.windows.net"

print(config)
spark.conf.set(config,accountKey)

adlsContainerName = "file-uploads"

#dbutils.fs.ls("abfss://<containername>@<storageaccount>.dfs.core.windows.net")
adlsRoot = "abfss://" + adlsContainerName + "@" + adlsStorageAccountName+ ".dfs.core.windows.net/"

print(adlsRoot)

dbutils.fs.ls(adlsRoot)

allFilesPath = adlsRoot + "all-files"
dbutils.fs.ls(allFilesPath)

statementPaths = allFilesPath + "/Statements"
display(dbutils.fs.ls(statementPaths))

#read file without mounting
myfilePath = allFilesPath + "/myfiles/MyTransactionsWithWellsChecking.txt"
mytrans = spark.read.format('csv').options(header='true', inferSchema='true', sep ='\t').load(myfilePath)

display(mytrans)

mytrans.select('*')

#read file after mounting
mountPoint = "/mnt/all-files"

ClientId = "3dbf4446-780d-45d0-8967-1d863c49c1f3"
clientSecret = "RRi8Q~rPTYktfm1d~w.dZpJTC_TNsBLqq~bxUbJS"
tenantId = "176fb781-5ed4-4dcd-bc75-14fe0a62e71b"
endpoint = "https://login.microsoftonline.com/" + tenantId + "/oauth2/token"

config = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": ClientId,
           "fs.azure.account.oauth2.client.secret": clientSecret,
           "fs.azure.account.oauth2.client.endpoint": endpoint}

if not any(mount.mountPoint == mountPoint for mount in dbutils.fs.mounts()):
  dbutils.fs.mount(
    source = allFilesPath,
    mount_point = mountPoint,
    extra_configs = config)

display(dbutils.fs.ls(mountPoint))

myfileMountedPath = mountPoint + "/myfiles/MyTransactionsWithWellsChecking.txt"
mytrans = spark.read.format('csv').options(header='true', inferSchema='true', sep ='\t').load(myfileMountedPath)

display(mytrans)

dbutils.fs.unmount(mountPoint)

# COMMAND ----------

# Connection Type 3: Connect and Mount to block storage using an SAS Token
# Connect to account storage container bob using SAS key (Shared access signature  from storage account)
# using "wasbs:"
# Then get an sas key by opening the storage account and going to "Shared access signature:" 
# and generating an SAS token  

dbutils.help()
dbutils.fs.help()

#spark.conf.set("fs.azure.sas.<containername>.<StorageAccountName>.blob.core.windows.net", <sasKey>)
adlsStorageAccountName = "reconciliationdevdl"

#option 2 Using SAS
adlsContainerName = "file-uploads"
# remember to remove the question mark fro the SAS token.
sasKey = "sv=2021-06-08&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2022-08-11T03:08:59Z&st=2022-08-10T19:08:59Z&spr=https&sig=6wcoEbKKN%2Fj0Slisx5T5UPJ8A4zFNwJBCmTgzFhFiIw%3D"
#config = "fs.azure.sas.<containername>.<StorageAccountName>.blob.core.windows.net"
config = "fs.azure.sas." + adlsContainerName + "." + adlsStorageAccountName + ".blob.core.windows.net"

print(config)
spark.conf.set(config,sasKey)

#dbutils.fs.ls("wsabs://<containername>@<storageaccount>.blob.core.windows.net")
adlsRoot = "wasbs://" + adlsContainerName + "@" + adlsStorageAccountName+ ".blob.core.windows.net/"

print(adlsRoot)

dbutils.fs.ls(adlsRoot)

allFilesPath = adlsRoot + "all-files"
dbutils.fs.ls(allFilesPath)

statementPaths = allFilesPath + "/Statements"
display(dbutils.fs.ls(statementPaths))

#read file without mounting
myfilePath = allFilesPath + "/myfiles/MyTransactionsWithWellsChecking.txt"
mytrans = spark.read.format('csv').options(header='true', inferSchema='true', sep ='\t').load(myfilePath)

display(mytrans)

mytrans.select('*')

#read file after mounting
config = { config : sasKey }
mountPoint = "/mnt/all-files"
if not any(mount.mountPoint == mountPoint for mount in dbutils.fs.mounts()):
  dbutils.fs.mount(
    source = allFilesPath,
    mount_point = mountPoint,
    extra_configs = config)

display(dbutils.fs.ls(mountPoint))

myfileMountedPath = mountPoint + "/myfiles/MyTransactionsWithWellsChecking.txt"
mytrans = spark.read.format('csv').options(header='true', inferSchema='true', sep ='\t').load(myfileMountedPath)

display(mytrans)

dbutils.fs.unmount(mountPoint)



# COMMAND ----------

# Connection Type 4: Mount to dfs storage using secrets ad keyvault
# Python code to mount and access Azure Data Lake Storage Gen2 Account to Azure Databricks with Service Principal and OAuth
# Author: Dhyanendra Singh Rathore

import sys
print('python version: ' + sys.version)


# Define the variables used for creating connection strings
adlsAccountName = "reconciliationdevdl"
adlsContainerName = "file-uploads"
adlsFolderName = "all-files"
mountPoint = "/mnt/all-files"

# read this for creating ApplicationId, tenantId and ClientSecret
# https://docs.microsoft.com/en-us/azure/databricks/security/aad-storage-service-principal

# go to Azure Active directory and add an App Registartion
# Step 1: Azure Active directory and add an App Registartion and add an App Registrations
# From App Registration get: 
# a. Application Id (aka Client Id)
# b. Tenant Id
# Step 2: Under App Registration 
# a. Create a Secret under (create certficates and secrets)
# Get ClientSecret
# Step 3: create a dataVault resources
# a. create the vault
# b. create a secret scope
# https://docs.microsoft.com/en-us/azure/databricks/security/secrets/secret-scopes
# Step 4: Place all 3 secrets into the secret scope of the data vault.
# a. Add name values pair s Client Id, Tenant Id and ClientSecret
# Step 5: Go to Storage Account/Access Control +Add Role Assigment
# Select the role "Storage Blob Data Controller"
# add yur use id to this role

applicationId = dbutils.secrets.get(scope="DatabricksKeyVault",key="ClientId")


# Application (Client) Secret Key
authenticationKey = dbutils.secrets.get(scope="DatabricksKeyVault",key="ClientSecret")

# Directory (Tenant) ID
tenandId = dbutils.secrets.get(scope="DatabricksKeyVault",key="TenantId")

endpoint = "https://login.microsoftonline.com/" + tenandId + "/oauth2/token"
source = "abfss://" + adlsContainerName + "@" + adlsAccountName + ".dfs.core.windows.net/" + adlsFolderName

# Connecting using Service Principal secrets and OAuth
configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": applicationId,
           "fs.azure.account.oauth2.client.secret": authenticationKey,
           "fs.azure.account.oauth2.client.endpoint": endpoint}

# Mounting ADLS Storage to DBFS
# Mount only if the directory is not already mounted
if not any(mount.mountPoint == mountPoint for mount in dbutils.fs.mounts()):
  dbutils.fs.mount(
    source = source,
    mount_point = mountPoint,
    extra_configs = configs)
statementsDirectory = mountPoint + "/Statements"
dbutils.fs.ls(statementsDirectory)

#dbutils.fs.unmount(mountPoint)


# COMMAND ----------

# MAGIC %sh 
# MAGIC python --version
# MAGIC #Another way to find python version
# MAGIC #using %sh allows you to run linuyx shel lcomands inside the databrick cluster

# COMMAND ----------

mytrans = spark.read.format('csv').options(header='true', inferSchema='true', sep ='\t').load('/mnt/all-files/myfiles/MyTransactionsWithWellsChecking.txt')

display(mytrans)

mytrans.select('*')


# COMMAND ----------

banktrans = spark.read.format('csv').options(header='true', inferSchema='true', sep ='\t').load('/mnt/all-files/bankfiles/StatementFromWells.txt')

display(banktrans)

banktrans.select("*")



# COMMAND ----------

mytrans.select("*").show()
mytrans.write.mode("overwrite").saveAsTable(name = "mytrans")

banktrans.select("*").show()
banktrans.write.mode("overwrite").saveAsTable(name = "banktrans") 


# COMMAND ----------

# MAGIC 
# MAGIC %sql
# MAGIC SELECT CAST(banktrans.TranAmount as decimal(23,2)) - CAST( mytrans.TranAmount as decimal(23,2)) As Diff, *  
# MAGIC FROM banktrans
# MAGIC INNER JOIN mytrans
# MAGIC ON
# MAGIC banktrans.TranDate = mytrans.TranDate
# MAGIC AND 
# MAGIC ( 
# MAGIC  banktrans.TranAmount = mytrans.TranAmount 
# MAGIC  OR 
# MAGIC  (banktrans.TranAmount != mytrans.TranAmount AND  ABS(banktrans.TranAmount - mytrans.TranAmount) < mytrans.TranAmount / 100 )
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT CAST(banktrans.TranAmount as decimal(23,2)) - CAST( mytrans.TranAmount as decimal(23,2)) As Diff, *  
# MAGIC FROM banktrans
# MAGIC INNER JOIN mytrans
# MAGIC ON
# MAGIC banktrans.TranDate = mytrans.TranDate
# MAGIC AND 
# MAGIC ( 
# MAGIC  banktrans.TranAmount = mytrans.TranAmount 
# MAGIC  OR 
# MAGIC  (banktrans.TranAmount != mytrans.TranAmount AND  ABS(banktrans.TranAmount - mytrans.TranAmount) < mytrans.TranAmount / 100 )
# MAGIC )
# MAGIC WHERE CAST(banktrans.TranAmount as decimal(23,2)) - CAST( mytrans.TranAmount as decimal(23,2)) <> 0

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC SUM(CAST(banktrans.TranAmount AS decimal(23,2))) AS SumBankSide, 
# MAGIC SUM(CAST(mytrans.TranAmount as decimal(23,2))) AS SumMySide, 
# MAGIC SUM(CAST(banktrans.TranAmount as decimal(23,2))) - SUM(CAST(mytrans.TranAmount as decimal(23,2))) As Diff
# MAGIC FROM banktrans
# MAGIC INNER JOIN mytrans
# MAGIC ON
# MAGIC banktrans.TranDate = mytrans.TranDate
# MAGIC AND 
# MAGIC ( 
# MAGIC  banktrans.TranAmount = mytrans.TranAmount 
# MAGIC  OR 
# MAGIC  (banktrans.TranAmount != mytrans.TranAmount AND  ABS(banktrans.TranAmount - mytrans.TranAmount) < mytrans.TranAmount / 100 )
# MAGIC )

# COMMAND ----------

alltransactions = spark.read.format('csv').options(header='true', inferSchema='true', sep ='\t').load('/mnt/all-files/AccountRecon/AllTransactions.txt')

display(alltransactions)

# COMMAND ----------

allstatements = spark.read.format('csv').options(header='true', inferSchema='true', sep ='\t').load('/mnt/all-files/AccountRecon/StatementFromAccount*.txt')

display(allstatements)

# COMMAND ----------

accountlookup = spark.read.format('csv').options(header='true', inferSchema='true', sep ='\t').load('/mnt/all-files/AccountRecon/AccountLookup.txt')

display(accountlookup)

# COMMAND ----------

alltransactions.write.mode("overwrite").saveAsTable(name = "alltransactions")
accountlookup.write.mode("overwrite").saveAsTable(name = "accountlookup")
allstatements.write.mode("overwrite").saveAsTable(name = "allstatements")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT CAST(alltransactions.TranAmount as decimal(23,2)) - CAST( allstatements.TranAmount as decimal(23,2)) As Diff, *  
# MAGIC FROM alltransactions
# MAGIC INNER JOIN accountlookup
# MAGIC ON
# MAGIC alltransactions.AccountId = accountlookup.AccountId
# MAGIC INNER JOIN allstatements
# MAGIC ON
# MAGIC accountlookup.AccountName = allstatements.AccountName
# MAGIC AND
# MAGIC alltransactions.TranDate = allstatements.TranDate
# MAGIC AND 
# MAGIC ( 
# MAGIC  alltransactions.TranAmount = allstatements.TranAmount 
# MAGIC  OR 
# MAGIC  (alltransactions.TranAmount != allstatements.TranAmount AND  ABS(alltransactions.TranAmount - allstatements.TranAmount) < allstatements.TranAmount / 100 )
# MAGIC )

# COMMAND ----------

# Create a schema for the dataframe
from pyspark.sql.types import *
mytransjson = spark.read.option("multiline","true").json('/mnt/all-files/Statements/StatementFromAccount1.json')
display(mytransjson)
mytransjson.printSchema()

from pyspark.sql.types import StructType, StructField, DateType, StringType, IntegerType, DoubleType
schema = StructType ([
        StructField('TranDate', StringType(), False),
        StructField('AccountName', StringType(), True),
        StructField('TranRef', IntegerType(), True),
        StructField('TranAmount', DoubleType(), True),
        StructField('Description', StringType(), True)
    ])

#mytransjson = spark.read.schema(schema).json('/mnt/all-files/Statements/StatementFromAccount1.json')
#mytransjson = spark.read.option("multiline","true").schema(schema).json('/mnt/all-files/Statements/StatementFromAccount1.json')
mytransjson2 = spark.read.json("/mnt/all-files/Statements/StatementFromAccount1.json", schema, multiLine=True)
#mytransjson = spark.read.option("multiline","true").json("/mnt/all-files/Statements/StatementFromAccount1.json")

mytransjson2.printSchema()
display(mytransjson2)



# COMMAND ----------

#from pyspark.sql.functions import explode, col
from pyspark.sql.functions import *
#from pyspark.sql.functions import to_date,  col
from pyspark.sql.types import StructType, StructField, DateType, StringType, IntegerType, DoubleType
#transactions = spark.read.option("multiline","true").json('/mnt/all-files/Statements/StatementFromAccount*.json')

transactions = spark.read \
.option("multiline","true") \
.option("cloudFiles.inferColumnTypes","true") \
.option("cloudFiles.schemaHints", "TranDate DATE, TranRef INTEGER, TranAmount DOUBLE") \
.json('/mnt/all-files/Statements/StatementFromAccount*.json')

transactions.printSchema()

print(spark.conf.get("spark.databricks.clusterUsageTags.sparkVersion"))


#transactions = spark.read.option("multiline","true").json('/mnt/all-files/Statements/StatementFromAccount*.json')

display(transactions)

schema = StructType([
    StructField('TranDate', DateType(), False),
    StructField('AccountName', StringType(), True),
    StructField('TranRef', IntegerType(), True),
    StructField('TranAmount', DoubleType(), True),
    StructField('Description', StringType(), True)
])

#transactions.printSchema()

df1 = transactions.withColumn('TranDate',to_date(transactions.TranDate, 'MM-dd-yyyy'))
df1.printSchema()

display(df1)

df1 = transactions.select(
        transactions.TranDate,
        col('TranDate').alias('TranDate2'),
        to_date(transactions.TranDate, 'MM-dd-yyyy').alias('TranDate3'),
        to_date(col('TranDate'), 'MM-dd-yyyy').alias('TranDate4'),
        to_date('TranDate', 'MM-dd-yyyy').alias('TranDate5'),
        transactions.AccountName,
        transactions.TranRef,
        col('TranAmount').cast('double').alias('TranAmount'),
        transactions.Description)

df1.printSchema()

display(df1)

#t2 = transactions.select(explode(".").alias("transactions"))

#display(t2)

#t3 = t2.select(
#    col("transactions.TranDate").alias("TranDate"),
#    col("transactions.AccountName").alias("AccountName"),
#    col("transactions.TranRef").alias("TranRef"),
#    col("transactions.TranAmount").alias("TranAmount"),
#    col("transactions.Description").alias("Description")
#       )

#display(t3)






# COMMAND ----------

schema = 'id int, dob string'
sampleDF = spark.createDataFrame(
[[1,'2021-01-01'], 
[2,'2021-01-02']], schema=schema)

display(sampleDF) 

sampleDF.select('id','dob' ,to_date('dob', 'yyyy-dd-mm').alias('dob_date')).show()

sampleDF2 = spark.createDataFrame(
[[1,'11-30-2021'], 
[2,'11-30-2022']], schema=schema)

display(sampleDF2) 
sampleDF2 = sampleDF2.select('id','dob' ,to_date('dob', 'MM-dd-yyyy').alias('dob_date')).show()
display(sampleDF2)


# COMMAND ----------

# Databricks AutoLoader (datbricks readstream cloudfiles format)
# The benefits of using databricks readstream cloudfiles include 
#   a. Performance
#   b. Manages stability when semi-structured data arrives in high volumes and changes over time
#   c. Supports Delat Live tables and supports structured streaming apps better than  Apache Spark stream processing  
#   https://docs.databricks.com/ingestion/auto-loader/index.html#benefits-over-apache-spark-filestreamsource
# Delta Live Tables basics: https://docs.databricks.com/data-engineering/delta-live-tables/index.html
# using Autoloader with Delta Live Table: https://docs.databricks.com/ingestion/auto-loader/dlt.html
# using Autoloader with Structured Stream Apps: https://docs.databricks.com/ingestion/auto-loader/structured-streaming.html
from pyspark.sql.functions import *
#from pyspark.sql.functions import to_date,  col
from pyspark.sql.types import StructType, StructField, DateType, StringType, IntegerType, DoubleType
#transactions = spark.read.option("multiline","true").json('/mnt/all-files/Statements/StatementFromAccount*.json')

#spark.readStream.format("cloudFiles")
#      .schema("title STRING, id INT, revisionId INT, revisionTimestamp TIMESTAMP, revisionUsername STRING, revisionUsernameId INT, text STRING")
#      .option("cloudFiles.format", "parquet")
#      .load("/databricks-datasets/wikipedia-datasets/data-001/en_wikipedia/articles-only-parquet")

#@dlt.table
#def transactions():
#  return (
#    spark.readstream \
#    .format("cloudFiles") \
#    .schema("TranDate DATE, TranRef INTEGER, TranAmount DOUBLE") \
#    .option("cloudFiles.inferColumnTypes","true") \
#    .option("cloudFiles.format", "json") \
#    .option("multiline","true") \
#    .json('/mnt/all-files/IncomingStatements/')
#  )

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split

spark = SparkSession \
    .builder \
    .appName("StructuredNetworkWordCount") \
    .getOrCreate()

print('apache spark version: ' + spark.version)

source_path = '/mnt/all-files/IncomingStatements/'
checkpoint_path = '/tmp/delta/IncomingStatements/_checkpoints'
write_path = '/tmp/delta/IncomingStatements'

subscriptionId = "ab2bce3d-2636-43af-b673-03ad3cf7df5f"
tenandId = dbutils.secrets.get(scope="DatabricksKeyVault",key="TenantId")
print('tenandId: ' + tenandId)
clientId = dbutils.secrets.get(scope="DatabricksKeyVault",key="ClientId")
print('clientId: ' + clientId)
clientSecret = dbutils.secrets.get(scope="DatabricksKeyVault",key="clientSecret")
print('clientSecret: ' + clientSecret)

transactions = spark.readStream \
.option("cloudFiles.subscriptionId", subscriptionId) \
.option("cloudFiles.tenantId", tenandId) \
.option("cloudFiles.clientId", clientId) \
.option("cloudFiles.clientSecret", clientSecret) \
.format("cloudFiles") \
.option("cloudFiles.format", "json") \
.option("cloudFiles.useNotifications","true") \
.option("cloudFiles.inferColumnTypes","true") \
.option("multiline","true") \
.schema("TranDate DATE, AccountName STRING, TranRef INTEGER, TranAmount DOUBLE, Description STRING") \
.load(source_path)

print('hello1')

transactions.printSchema()
print('hello2')

display(transactions)
print('hello3')

transactions.writeStream.format('delta') \
  .option('checkpointLocation', checkpoint_path) \
  .start(write_path)


# COMMAND ----------

transactions.printSchema()
display(transactions)

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split

spark = SparkSession \
    .builder \
    .appName("StructuredNetworkWordCount") \
    .getOrCreate()


lines = spark \
    .readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

words = lines.select(
   explode(
       split(lines.value, " ")
   ).alias("word")
)

#display(words) 

# Generate running word count
wordCounts = words.groupBy("word").count()

#display(wordCounts)

# Start running the query that prints the running counts to the console
query = wordCounts \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()


# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT CAST(alltransactions.TranAmount as decimal(23,2)) - CAST( allstatements.TranAmount as decimal(23,2)) As Diff, *  
# MAGIC FROM alltransactions
# MAGIC INNER JOIN accountlookup
# MAGIC ON
# MAGIC alltransactions.AccountId = accountlookup.AccountId
# MAGIC INNER JOIN allstatements
# MAGIC ON
# MAGIC accountlookup.AccountName = allstatements.AccountName
# MAGIC AND
# MAGIC alltransactions.TranDate = allstatements.TranDate
# MAGIC AND 
# MAGIC ( 
# MAGIC  alltransactions.TranAmount = allstatements.TranAmount 
# MAGIC  OR 
# MAGIC  (alltransactions.TranAmount != allstatements.TranAmount AND  ABS(alltransactions.TranAmount - allstatements.TranAmount) < allstatements.TranAmount / 100 )
# MAGIC )

# COMMAND ----------

bounds = spark.sql("SELECT min(AccountId) AS minAccountId, max(accountId) AS maxAccountId FROM alltransactions")

bounds

display(bounds)

#spdf = spark.sql("SELECT * FROM alltransactions", partition_cols=["AccountId"], lowerBound=bounds.minAccountId, upperBound=bounds.maxAccountId)

spdf = spark.sql("SELECT * FROM alltransactions").repartition(3, ['AccountId','TranDate'])
spdf.printSchema()

display(spdf)

#spdf.rdd.getNumPartitions()

# this will show rows in each partition
#spdf.rdd.glom().collect()

spdf.write.mode("overwrite").option("overwriteSchema", "true").partitionBy( ['AccountId','TranDate']).saveAsTable(name = "alltransactionsPartitioned")

spdf = spark.sql("SELECT * FROM alltransactionsPartitioned")
spdf.printSchema()

display(spdf)

spdf.rdd.getNumPartitions()

# this will show rows in each partition
spdf.rdd.glom().collect()




# COMMAND ----------



display(dbutils.fs.ls("/user/hive/warehouse/alltransactionspartitioned"))

display(dbutils.fs.ls("/user/hive/warehouse/alltransactionspartitioned/_delta_log/")) 
display(dbutils.fs.ls("/user/hive/warehouse/alltransactionspartitioned/AccountId=101987678"))
display(dbutils.fs.ls("/user/hive/warehouse/alltransactionspartitioned/AccountId=101987678/TranDate=11-30-21/"))

display(dbutils.fs.ls("/user/hive/warehouse/alltransactionspartitioned/AccountId=101987679"))
display(dbutils.fs.ls("/user/hive/warehouse/alltransactionspartitioned/AccountId=101987679/TranDate=11-30-21/"))

display(dbutils.fs.ls("/user/hive/warehouse/alltransactionspartitioned/AccountId=101987680"))
display(dbutils.fs.ls("/user/hive/warehouse/alltransactionspartitioned/AccountId=101987680/TranDate=11-30-21/"))

display(dbutils.fs.ls("/user/hive/warehouse/alltransactionspartitioned/part-00000-4214e5b9-ebb5-4121-9240-e3cbbe580291-c000.snappy.parquet"))
display(dbutils.fs.ls("/user/hive/warehouse/alltransactionspartitioned/part-00001-715429b3-3707-47f6-a330-a38368b163a7-c000.snappy.parquet"))
display(dbutils.fs.ls("/user/hive/warehouse/alltransactionspartitioned/part-00002-72e1be1f-7245-4fbb-985f-15dbd5611e9f-c000.snappy.parquet"))



        

# COMMAND ----------


# read a catalog partition file

psdf = spark.read.parquet("/user/hive/warehouse/alltransactionspartitioned/AccountId=101987679/TranDate=11-30-21/part-00001-217f10be-390f-4311-af36-9a0f5be49a3c.c000.snappy.parquet")
 
display(psdf)

# COMMAND ----------

spdf = spark.sql("SELECT * FROM alltransactionspartitioned")
display(spdf)

# COMMAND ----------

spdf = spark.sql("SELECT * FROM alltransactionspartitioned where AccountId = '101987678' ")
display(spdf)

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW PARTITIONS alltransactionspartitioned

# COMMAND ----------

spdf = spark.read.format("delta").load("/user/hive/warehouse/alltransactionspartitioned/AccountId=101987678/TranDate=11-30-21/")
display(spdf) 

# COMMAND ----------


spdf = spark.read.format("delta").load("/user/hive/warehouse/alltransactionspartitioned/part-00002-72e1be1f-7245-4fbb-985f-15dbd5611e9f-c000.snappy.parquet")
display(spdf)

