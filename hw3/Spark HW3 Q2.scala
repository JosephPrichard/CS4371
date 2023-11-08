// Databricks notebook source
// MAGIC %scala
// MAGIC val rdd = sc.textFile("file:/Workspace/Shared/city_temperature.csv")
// MAGIC
// MAGIC rdd
// MAGIC   .filter(line => line.split(",")(0) != "Region")
// MAGIC   .map(line => {
// MAGIC     val cols = line.split(",")
// MAGIC     var temp = 0.0f
// MAGIC     try {
// MAGIC       temp = cols(7).toFloat
// MAGIC     } catch {
// MAGIC       case e: NumberFormatException => {}
// MAGIC     }
// MAGIC     (cols(0), temp)
// MAGIC   })
// MAGIC   .groupByKey()
// MAGIC   .mapValues(temps => {
// MAGIC     var sum = 0.0f
// MAGIC     var size = 0
// MAGIC     temps.foreach(temp => {
// MAGIC       if (temp != -99) {
// MAGIC         sum += temp
// MAGIC         size += 1
// MAGIC       }
// MAGIC     })
// MAGIC     sum / size
// MAGIC   })
// MAGIC   .collect()
// MAGIC   .foreach(pair => println(pair))
// MAGIC

// COMMAND ----------

// MAGIC %scala
// MAGIC val rdd = sc.textFile("file:/Workspace/Shared/city_temperature.csv")
// MAGIC
// MAGIC rdd
// MAGIC   .filter(line => line.split(",")(0) == "Asia")
// MAGIC   .map(line => {
// MAGIC     val cols = line.split(",")
// MAGIC     var temp = 0.0f
// MAGIC     try {
// MAGIC       temp = cols(7).toFloat
// MAGIC     } catch {
// MAGIC       case e: NumberFormatException => {}
// MAGIC     }
// MAGIC     (cols(6), temp)
// MAGIC   })
// MAGIC   .groupByKey()
// MAGIC   .mapValues(temps => {
// MAGIC     var sum = 0.0f
// MAGIC     var size = 0
// MAGIC     temps.foreach(temp => {
// MAGIC       if (temp != -99) {
// MAGIC         sum += temp
// MAGIC         size += 1
// MAGIC       }
// MAGIC     })
// MAGIC     sum / size
// MAGIC   })
// MAGIC   .sortByKey(ascending = false)
// MAGIC   .collect()
// MAGIC   .foreach(pair => println(pair))

// COMMAND ----------

// MAGIC %scala
// MAGIC val rdd = sc.textFile("file:/Workspace/Shared/city_temperature.csv")
// MAGIC
// MAGIC rdd
// MAGIC   .filter(line => line.split(",")(1) == "Spain")
// MAGIC   .map(line => {
// MAGIC     val cols = line.split(",")
// MAGIC     var temp = 0.0f
// MAGIC     try {
// MAGIC       temp = cols(7).toFloat
// MAGIC     } catch {
// MAGIC       case e: NumberFormatException => {}
// MAGIC     }
// MAGIC     (cols(3), temp)
// MAGIC   })
// MAGIC   .groupByKey()
// MAGIC   .mapValues(temps => {
// MAGIC     var sum = 0.0f
// MAGIC     var size = 0
// MAGIC     temps.foreach(temp => {
// MAGIC       if (temp != -99) {
// MAGIC         sum += temp
// MAGIC         size += 1
// MAGIC       }
// MAGIC     })
// MAGIC     sum / size
// MAGIC   })
// MAGIC   .collect()
// MAGIC   .foreach(pair => println(pair))

// COMMAND ----------

// MAGIC %scala
// MAGIC val rdd_temps = sc.textFile("file:/Workspace/Shared/city_temperature.csv")
// MAGIC val rdd_capitals = sc.textFile("file:/Workspace/Shared/country-list.csv")
// MAGIC
// MAGIC rdd_temps
// MAGIC   .map(line => {
// MAGIC     val cols = line.split(",")
// MAGIC     var temp = 0.0f
// MAGIC     try {
// MAGIC       temp = cols(7).toFloat
// MAGIC     } catch {
// MAGIC       case e: NumberFormatException => {}
// MAGIC     }
// MAGIC     (cols(1), temp)
// MAGIC   })
// MAGIC   .groupByKey()
// MAGIC   .mapValues(temps => {
// MAGIC     var sum = 0.0f
// MAGIC     var size = 0
// MAGIC     temps.foreach(temp => {
// MAGIC       if (temp != -99) {
// MAGIC         sum += temp
// MAGIC         size += 1
// MAGIC       }
// MAGIC     })
// MAGIC     sum / size
// MAGIC   })
// MAGIC   .join(
// MAGIC     rdd_capitals
// MAGIC       .map(line => {
// MAGIC         val cols = line.replaceAll("[^a-zA-Z0-9,]", "").split(",")
// MAGIC         (cols(0), cols(1))
// MAGIC       })
// MAGIC   )
// MAGIC   .collect()
// MAGIC   .foreach(pair => println(pair))

// COMMAND ----------


