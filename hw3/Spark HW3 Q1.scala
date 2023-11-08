// Databricks notebook source
// MAGIC %scala
// MAGIC val rdd = sc.textFile("file:/Workspace/Shared/input_hw1.txt")
// MAGIC
// MAGIC rdd.flatMap(line => line.split(" "))
// MAGIC   .map(word => word.toLowerCase().replaceAll("[^a-zA-Z0-9]", ""))
// MAGIC   .filter(word => word != "")
// MAGIC   .map(word => (word, 1))
// MAGIC   .reduceByKey((a, b) => a + b)
// MAGIC   .collect()
// MAGIC   .foreach(pair => println(pair))

// COMMAND ----------

// MAGIC %scala
// MAGIC val rdd = sc.textFile("file:/Workspace/Shared/input_hw1.txt")
// MAGIC
// MAGIC rdd.flatMap(line => line.split(" "))
// MAGIC   .map(word => word.toLowerCase().replaceAll("[^a-zA-Z0-9]", ""))
// MAGIC   .filter(word => List("america", "president", "washington").contains(word))
// MAGIC   .map(word => (word, 1))
// MAGIC   .reduceByKey((a, b) => a + b)
// MAGIC   .collect()
// MAGIC   .foreach(pair => println(pair))

// COMMAND ----------

// MAGIC %scala
// MAGIC val rdd = sc.textFile("file:/Workspace/Shared/input_hw1.txt")
// MAGIC
// MAGIC rdd.flatMap(line => line.split(" "))
// MAGIC   .map(word => word.toLowerCase().replaceAll("[^a-zA-Z0-9]", ""))
// MAGIC   .filter(word => word != "")
// MAGIC   .map(word => (word, 1))
// MAGIC   .reduceByKey((a, b) => a + b)
// MAGIC   .sortBy(_._2, ascending = false)
// MAGIC   .take(10)
// MAGIC   .foreach(pair => println(pair))
// MAGIC
