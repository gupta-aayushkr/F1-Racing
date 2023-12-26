# Databricks notebook source
# MAGIC %md
# MAGIC # Access Azure Data Lake using Service Principal
# MAGIC
# MAGIC ## Steps to Follow
# MAGIC
# MAGIC ### 1. Register Azure AD Application / Service Principal
# MAGIC
# MAGIC 1.1. Go to the [Azure Portal](https://portal.azure.com/).
# MAGIC
# MAGIC 1.2. Navigate to **Azure Active Directory** > **App registrations**.
# MAGIC
# MAGIC 1.3. Click on **New registration**.
# MAGIC
# MAGIC 1.4. Fill in the necessary details, such as the application name.
# MAGIC
# MAGIC 1.5. Once registered, note down the **Application (client) ID** and **Directory (tenant) ID**.
# MAGIC
# MAGIC ### 2. Generate a Secret/Password for the Application
# MAGIC
# MAGIC 2.1. In the Azure Portal, go to the registered application.
# MAGIC
# MAGIC 2.2. Navigate to **Certificates & secrets**.
# MAGIC
# MAGIC 2.3. Under the **Secrets** section, click on **New client secret**.
# MAGIC
# MAGIC 2.4. Provide a description, set the expiry, and click on **Add**. Note down the generated secret value.
# MAGIC
# MAGIC ### 3. Set Spark Config with App/Client Id, Directory/Tenant Id & Secret
# MAGIC
# MAGIC 3.1. In your Spark application, set the configuration parameters:
# MAGIC
# MAGIC    ```bash
# MAGIC    spark.conf.set("fs.azure.account.auth.type.<your-storage-account>.dfs.core.windows.net", "OAuth")
# MAGIC    spark.conf.set("fs.azure.account.oauth.provider.type.<your-storage-account>.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
# MAGIC    spark.conf.set("fs.azure.account.oauth2.client.id.<your-storage-account>.dfs.core.windows.net", "<Application-ID>")
# MAGIC    spark.conf.set("fs.azure.account.oauth2.client.secret.<your-storage-account>.dfs.core.windows.net", "<Client-Secret>")
# MAGIC    spark.conf.set("fs.azure.account.oauth2.client.endpoint.<your-storage-account>.dfs.core.windows.net", "https://login.microsoftonline.com/<Directory-ID>/oauth2/token")
# MAGIC ```
# MAGIC
# MAGIC Replace `<Application-ID>`, `<Client-Secret>`, and `<Directory-ID>` with the corresponding values obtained in steps 1.5 and 2.4.
# MAGIC
# MAGIC ### 4. Assign Role 'Storage Blob Data Contributor' to the Data Lake
# MAGIC
# MAGIC 4.1. Navigate to your Data Lake Storage account in the Azure Portal.
# MAGIC
# MAGIC 4.2. Click on **Access control (IAM)**.
# MAGIC
# MAGIC 4.3. Click on **+ Add role assignment**.
# MAGIC
# MAGIC 4.4. Choose the role **Storage Blob Data Contributor**.
# MAGIC
# MAGIC 4.5. Select the application or service principal created in step 1.
# MAGIC
# MAGIC 4.6. Click on **Save** to grant the necessary permissions.
# MAGIC
# MAGIC Follow these practical steps to register an Azure AD Application, generate credentials, configure Spark, and assign the required role for accessing Azure Data Lake using a Service Principal.
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

client_id = dbutils.secrets.get(scope='formula1-scope', key='client-id-adb')

# COMMAND ----------

client_id

# COMMAND ----------

client_id = dbutils.secrets.get(scope='formula1-scope', key='client-id-adb')
tenant_id = dbutils.secrets.get(scope='formula1-scope', key='tenant-id-adb')
client_secret = dbutils.secrets.get(scope='formula1-scope', key='client-secret-adb')

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula01dl.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.formula01dl.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.formula01dl.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.formula01dl.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.formula01dl.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula01dl.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula01dl.dfs.core.windows.net"))

# COMMAND ----------

