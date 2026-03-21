node2 :
spark-shell --total-executor-cores 20 --executor-memory 5G --name fansy

val data = sqlContext.sql("select * from law_init1.data_101003_real_url_gt_one")


////////// 1. 编码数据

val (userCol,itemCol,userMetaTable,itemMetaTable,newUserCol,newItemCol) = ("userid","url","law_init1.data_101003_usermeta","law_init1.data_101003_itemmeta","uid","pid")
case class User_ID(user:String,id:Long)
case class Item_ID(item:String,id:Long)
import sqlContext.implicits._
val allUsers= data.select(userCol).distinct().orderBy(userCol).rdd.map(_.getString(0)).zipWithIndex().map(x => User_ID(x._1,x._2)).toDF
val allItems = data.select(itemCol).distinct().orderBy(itemCol).rdd.map(_.getString(0)).zipWithIndex().map(x => Item_ID(x._1,x._2)).toDF

allUsers.registerTempTable("userMetaTable_00")
allItems.registerTempTable("itemMetaTable_00")

sqlContext.sql("create table "+ userMetaTable +" as select * from userMetaTable_00")
sqlContext.sql("create table "+ itemMetaTable+" as select * from itemMetaTable_00")


// join
data.join(allUsers,data(userCol) === allUsers("user"),"leftouter").drop(allUsers("user")).withColumnRenamed("id",newUserCol).join(allItems,data(itemCol) === allItems("item"),"leftouter").drop(allItems("item")).withColumnRenamed("id",newItemCol).registerTempTable("encoded_00")
sqlContext.sql("create table law_init1.data_101003_encoded as select * from encoded_00")