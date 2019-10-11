# Databricks notebook source
dbutils.widgets.text("input", ""."")
dbutils.widgets.get("input")
FileName = getArgument("SourceFile")
FilePath = getArgument("SourceFolder")
print("Param -\'input':")
print(FileName)
print(FilePath)

# COMMAND ----------

# test / not pipeline ready
# vcf_path = "/mnt/staging/VCFfiles/annotated_tomato_150.vcf.gz"

# COMMAND ----------

def add_position_bin(df, bin_width=10000000):
  """
  add a bin for each variant, which will be used for partitioning the Delta-lake
  The start position
  :param df: dataframe
  :param bin_width: int, bin_width, default = 10,000,000 (10Mb)
  """
  df = df.withColumn("bin_start", fx.col("start") - (fx.col("start") % bin_width)). \
          withColumn("bin_end", fx.col("start") - (fx.col("start") % bin_width) + bin_width). \
          withColumn("bin", fx.concat(fx.col("bin_start").cast(StringType()), 
                                      fx.lit("-"), 
                                      fx.col("bin_end").cast(StringType()))). \
          drop("bin_start", "bin_end")
  return df

# COMMAND ----------

import pyspark.sql.functions as fx
from pyspark.sql.types import *
from pyspark.sql.functions import lit

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://kpistoropen.blob.core.windows.net/collateral/roadshow/logo_spark_tiny.png)  Read in VCF using Databricks' optimized VCF reader

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Specify the path to the VCF file, if there are multiple files, use wildcard (*). 

# COMMAND ----------

vcf_path = "/mnt/"+ FilePath + "/" + FileName

# COMMAND ----------

# test / not pipeline ready
# FileName = "annotated_tomato_360.vcf.gz"

# COMMAND ----------

vcf = (
  spark
  .read
  .format("com.databricks.vcf")
  .option("includeSampleIds", True)
  .option("flattenInfoFields", True)
  .load(vcf_path)
)

# COMMAND ----------

vcf.printSchema()

# COMMAND ----------

display(vcf.withColumn("genotypes", fx.expr("genotypes[0]")).limit(10))

# COMMAND ----------

vcf = add_position_bin(vcf)

# COMMAND ----------

vcf.printSchema()

# COMMAND ----------

vcf = vcf.withColumn("FileName", lit(FileName))

# COMMAND ----------

# test / not pipeline ready
# vcf = vcf.withColumn("FileName", lit("annotated_tomato_150.vcf.gz"))

# COMMAND ----------

display(vcf.limit(10))

# COMMAND ----------

outputPath = "/FileStore/Tables" + "/" + FileName

# COMMAND ----------

# test / not pipeline ready
# outputPath = "/FileStore/Tables/annotated_tomato_150.vcf.gz"

# COMMAND ----------

dbutils.fs.rm(outputPath, True)

# COMMAND ----------

(
  vcf
  .write
  .format("delta")
  .partitionBy("contigName", "bin")
  .save(outputPath)
)

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS genomes;
# MAGIC CREATE TABLE genomes USING DELTA LOCATION 'dbfs:/FileStore/Tables/annotated_tomato_150.vcf.gz';
# MAGIC OPTIMIZE genomes ZORDER BY (start)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from genomes LIMIT 10