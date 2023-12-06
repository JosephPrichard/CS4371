# Databricks notebook source
# MAGIC %scala
# MAGIC
# MAGIC import org.apache.spark.sql.functions._
# MAGIC
# MAGIC val rdd = sc.textFile("dbfs:/FileStore/tables/city_temperature.csv")
# MAGIC var df = rdd
# MAGIC   .map(lines => lines.split(","))
# MAGIC   .map(a => (a(0), a(7)))
# MAGIC   .toDF("Region", "AvgTemperature")
# MAGIC
# MAGIC df
# MAGIC   .where(df("AvgTemperature") =!= -99)
# MAGIC   .where(df("Region") =!= "Region")
# MAGIC   .groupBy(df("Region"))
# MAGIC   .agg(
# MAGIC     avg(df("AvgTemperature")).as("Average")
# MAGIC   )
# MAGIC   .show();

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC import org.apache.spark.sql.functions._
# MAGIC
# MAGIC val rdd = sc.textFile("dbfs:/FileStore/tables/city_temperature.csv")
# MAGIC val df = rdd
# MAGIC   .map(lines => lines.split(","))
# MAGIC   .map(a => (a(0), a(6), a(7)))
# MAGIC   .toDF("Region", "Year", "AvgTemperature")
# MAGIC
# MAGIC df
# MAGIC   .where(df("AvgTemperature") =!= -99)
# MAGIC   .where(df("Region") === "Asia")
# MAGIC   .groupBy(df("Year"))
# MAGIC   .agg(
# MAGIC     avg(df("AvgTemperature")).as("Average")
# MAGIC   )
# MAGIC   .show();

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC import org.apache.spark.sql.functions._
# MAGIC
# MAGIC val rdd = sc.textFile("dbfs:/FileStore/tables/city_temperature.csv")
# MAGIC val df = rdd
# MAGIC   .map(lines => lines.split(","))
# MAGIC   .map(a => (a(1), a(3), a(7)))
# MAGIC   .toDF("Country", "City", "AvgTemperature")
# MAGIC
# MAGIC df
# MAGIC   .where(df("AvgTemperature") =!= -99)
# MAGIC   .where(df("Country") === "Spain")
# MAGIC   .groupBy(df("City"))
# MAGIC   .agg(
# MAGIC     avg(df("AvgTemperature")).as("Average")
# MAGIC   )
# MAGIC   .show();

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC import org.apache.spark.sql.functions._
# MAGIC
# MAGIC val rdd1 = sc.textFile("dbfs:/FileStore/tables/city_temperature.csv")
# MAGIC val rdd2 = sc.textFile("dbfs:/FileStore/tables/country_list.csv")
# MAGIC
# MAGIC val df1 = rdd1
# MAGIC   .map(lines => lines.split(","))
# MAGIC   .map(a => (a(1), a(3), a(7)))
# MAGIC   .toDF("Country", "City", "AvgTemperature")
# MAGIC val df2 = rdd2
# MAGIC   .map(lines => lines.split(","))
# MAGIC   .map(a => (a(1).replaceAll("\"", "")))
# MAGIC   .toDF("Capital")
# MAGIC
# MAGIC df1
# MAGIC   .where(df1("AvgTemperature") =!= -99)
# MAGIC   .join(df2, df1("City") === df2("Capital"), "inner")
# MAGIC   .groupBy(df1("City"), df1("Country"))
# MAGIC   .agg(
# MAGIC     avg(df1("AvgTemperature")).as("Average")
# MAGIC   )
# MAGIC   .show(100);

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC import org.apache.spark.sql.functions._
# MAGIC
# MAGIC val rdd1 = sc.textFile("dbfs:/FileStore/tables/city_temperature.csv")
# MAGIC val rdd2 = sc.textFile("dbfs:/FileStore/tables/country_list.csv")
# MAGIC
# MAGIC val df1 = rdd1
# MAGIC   .map(lines => lines.split(","))
# MAGIC   .map(a => (a(1), a(3), a(7)))
# MAGIC   .toDF("Country", "City", "AvgTemperature")
# MAGIC val arrCapitals = rdd2
# MAGIC   .map(lines => lines.split(","))
# MAGIC   .map(a => (a(1).replaceAll("\"", ""), ""))
# MAGIC   .collect()
# MAGIC
# MAGIC val broadcast = sc.broadcast(arrCapitals.toMap)
# MAGIC
# MAGIC df1
# MAGIC   .where(df1("AvgTemperature") =!= -99)
# MAGIC   .filter((x: Row) => broadcast.value.contains(x.getAs[String]("City")))
# MAGIC   .groupBy(df1("City"), df1("Country"))
# MAGIC   .agg(
# MAGIC     avg(df1("AvgTemperature")).as("Average")
# MAGIC   )
# MAGIC   .show(100);

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC import org.apache.spark.sql.functions._
# MAGIC
# MAGIC val rdd1 = sc.textFile("dbfs:/FileStore/tables/city_temperature.csv")
# MAGIC val rdd2 = sc.textFile("dbfs:/FileStore/tables/country_list.csv")
# MAGIC
# MAGIC val df1 = rdd1
# MAGIC   .map(lines => lines.split(","))
# MAGIC   .map(a => (a(1), a(3), a(6), a(7)))
# MAGIC   .toDF("Country", "City", "Year", "AvgTemperature")
# MAGIC val df2 = rdd2
# MAGIC   .map(lines => lines.split(","))
# MAGIC   .map(a => (a(1).replaceAll("\"", "")))
# MAGIC   .toDF("Capital")
# MAGIC
# MAGIC val filterYears = udf((year: String) => year.toFloat >= 2000) 
# MAGIC val formatOutput = udf(
# MAGIC   (city: String, country: String, average: String) => 
# MAGIC     s"$city is the capital of $country and its average temperature is $average"
# MAGIC ) 
# MAGIC
# MAGIC df1
# MAGIC   .where(df1("AvgTemperature") =!= -99)
# MAGIC   .filter(filterYears(df1("Year")))
# MAGIC   .join(df2, df1("City") === df2("Capital"), "inner")
# MAGIC   .groupBy(df1("City"), df1("Country"))
# MAGIC   .agg(
# MAGIC     avg(df1("AvgTemperature")).as("Average")
# MAGIC   )
# MAGIC   .withColumn("Output", formatOutput(df1("City"), df1("Country"), $"Average"))
# MAGIC   .show(100, false);
