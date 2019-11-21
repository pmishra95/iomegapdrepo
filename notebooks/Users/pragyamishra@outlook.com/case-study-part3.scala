// Databricks notebook source
// MAGIC %sql
// MAGIC 
// MAGIC val getOrderAmount = (units: Int, unitPrice: Int, itemdiscount: Int) => {
// MAGIC   val total = (units * unitPrice)
// MAGIC   val discount = ((total * itemdiscount) / 100).asInstanceOf[Int]
// MAGIC   
// MAGIC   (total - discount).asInstanceOf[Int]
// MAGIC }
// MAGIC 
// MAGIC val getCustomerType = (credit: Int) => {
// MAGIC   if(credit < 10000) "Silver"
// MAGIC   else if(credit >= 10000 && credit < 25000) "Gold"
// MAGIC   else "Platinum"
// MAGIC }
// MAGIC 
// MAGIC spark.udf.register("getCustomerType", getCustomerType)
// MAGIC spark.udf.register("getOrderAmount", getOrderAmount)

// COMMAND ----------


val getOrderAmount = (units: Int, unitPrice: Int, itemdiscount: Int) => {
  val total = (units * unitPrice)
  val discount = ((total * itemdiscount) / 100).asInstanceOf[Int]
  
  (total - discount).asInstanceOf[Int]
}

val getCustomerType = (credit: Int) => {
  if(credit < 10000) "Silver"
  else if(credit >= 10000 && credit < 25000) "Gold"
  else "Platinum"
}

spark.udf.register("getCustomerType", getCustomerType)
spark.udf.register("getOrderAmount", getOrderAmount)

// COMMAND ----------

// MAGIC 
// MAGIC %sql
// MAGIC 
// MAGIC SELECT o.orderid AS OrderId, o.orderdate AS OrderDate, c.customername AS CustomerName, p.title AS ProductTitle,
// MAGIC     c.address AS CustomerLocation, getCustomerType(c.credit) AS CustomerType,
// MAGIC     getOrderAmount(o.units, p.unitprice, p.itemdiscount) AS OrderAmount,
// MAGIC     p.unitprice AS UnitPrice, p.itemdiscount AS ItemDiscount,
// MAGIC     o.billingaddress AS BillingAddress, o.remarks AS OrderRemarks
// MAGIC   FROM PracticeDB.Orders o
// MAGIC   INNER JOIN  PracticeDB.Customers c ON c.customerid = o.customer
// MAGIC   INNER JOIN PracticeDB.Products p ON p.productid = o.product
// MAGIC   WHERE o.billingaddress IN ( 'Bangalore', 'Trivandrum', 'Hyderabad', 'Mumbai', 'Chennai', 'New Delhi')
// MAGIC   ORDER BY OrderAmount
// MAGIC   
// MAGIC   

// COMMAND ----------

val statement = """

  SELECT CAST(o.orderid AS STRING) AS OrderId, o.orderdate AS OrderDate, c.customername AS CustomerName, p.title AS ProductTitle,
     c.address AS CustomerLocation, getCustomerType(c.credit) AS CustomerType,
     CAST(getOrderAmount(o.units, p.unitprice, p.itemdiscount) AS STRING) AS OrderAmount,
     CAST(p.unitprice AS STRING) AS UnitPrice, CAST(p.itemdiscount AS STRING) AS ItemDiscount,
     o.billingaddress AS BillingAddress, o.remarks AS OrderRemarks
    FROM PracticeDB.Orders o
    INNER JOIN  PracticeDB.Customers c ON c.customerid = o.customer
    INNER JOIN PracticeDB.Products p ON p.productid = o.product
    WHERE o.billingaddress IN ( 'Bangalore', 'Trivandrum', 'Hyderabad', 'Mumbai', 'Chennai', 'New Delhi')
    ORDER BY OrderAmount

"""

val processedOrders = spark.sql(statement)

processedOrders.printSchema

display(processedOrders)


// COMMAND ----------

val outputLocation2 = "https://storagegen2source.dfs.core.windows.net/data/processed-orders-cdm"
val cdmModel2 = "ordersystem"
val appId2 = "ebf3d1b7-4e7c-452e-b518-e3407c4f78d2"
val tenantId2 = "bdc0ec73-921d-4da3-88f6-49ca18bdc84d"
val secret2 = "H84Da@:7@dMhj-umqrR6iAUHEAk95L=I"

// COMMAND ----------

processedOrders
  .write
  .format("com.microsoft.cdm")
  .option("entity", "processedordersv2")
  .option("appId", appId2)
  .option("appKey", secret2)
  .option("tenantId", tenantId2)
  .option("cdmFolder", outputLocation2)
  .option("cdmModelName", cdmModel2)
  .save()
  

// COMMAND ----------

  val orders = spark
  .read
  .format("com.microsoft.cdm")
  .option("entity", "processedordersv2")
  .option("appId", appId2)
  .option("appKey", secret2)
  .option("tenantId", tenantId2)
  .option("cdmModel", "https://storagegen2source.dfs.core.windows.net/data/processed-orders-cdm/model.json")
  .load()
  

// COMMAND ----------

display(orders)