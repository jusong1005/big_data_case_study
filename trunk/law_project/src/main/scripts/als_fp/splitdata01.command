node2 :
spark-shell --total-executor-cores 20 --executor-memory 5G --name fansy

val data = sqlContext.sql("select * from law_init1.data_101003_encoded")

val(trainPercent,validatePercent, testPercent,sortedColumn) = (0.8,0.1,0.1,"timestamp_format")

val percents = Array(trainPercent,validatePercent,testPercent)
val all_size = data.count()
val real_precents = percents.map(x =>(x /percents.sum * all_size).toLong)//
val allSortedColumnData = data.select(sortedColumn).orderBy(sortedColumn).rdd.map(_.getString(0)).zipWithIndex().map(x => (x._2,x._1))
val firstSplitPoint = allSortedColumnData.lookup(real_precents(0)).head

val (train,validate,test) = if( testPercent >= 0.0) {
      val secondSplitPoint = allSortedColumnData.lookup(real_precents(0)+real_precents(1)).head
      (
        data.filter(sortedColumn + " < '" + firstSplitPoint +"'"),
        data.filter(sortedColumn + " >= '" + firstSplitPoint + "' and " + sortedColumn + " < '" + secondSplitPoint+"'"),
        data.filter(sortedColumn + " >= '" + secondSplitPoint+"'")
      )
    }else{
      (
        data.filter(sortedColumn + " < " + firstSplitPoint),
        data.filter(sortedColumn + " >= " + firstSplitPoint ),
        null
      )
    }

train.registerTempTable("train00")
validate.registerTempTable("validate00")
test.registerTempTable("test00")

sqlContext.sql("drop table law_init1.data_101003_encoded_train")
sqlContext.sql("drop table law_init1.data_101003_encoded_validate")
sqlContext.sql("drop table law_init1.data_101003_encoded_test")

sqlContext.sql("create table law_init1.data_101003_encoded_train as select * from train00")
sqlContext.sql("create table law_init1.data_101003_encoded_validate as select * from validate00")
sqlContext.sql("create table law_init1.data_101003_encoded_test as select * from test00")