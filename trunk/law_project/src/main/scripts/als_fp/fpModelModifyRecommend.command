node2 :
spark-shell --total-executor-cores 20 --executor-memory 5G --name fansy

val trainTable = "law_init1.data_101003_encoded_train"
val validateTable = "law_init1.data_101003_encoded_validate"
val URL_SETS_COLUMN = "URL_SETS_COLUMN"
val uidCol = "uid"
val pidCol = "pid"
import org.apache.spark.mllib.fpm.FPGrowth
val minSupport = 0.00001
val minConfidence = 0.2
//
val data = sqlContext.sql("select "+ uidCol +" , " + pidCol+" from "+ trainTable).distinct.groupBy(uidCol).agg(collect_set(col(pidCol)) as URL_SETS_COLUMN).filter(size(col(URL_SETS_COLUMN)) > 1)

// 2. 把数据构建成 FPModel需要的数据
val train = data.select(URL_SETS_COLUMN).rdd.map(row => row.getSeq[Long](0)).map(x => x.toArray)
train.cache

// 3.
val fpg = new FPGrowth().setMinSupport(minSupport)
val model = fpg.run(train)
//model.freqItemsets.count

val rules = model.generateAssociationRules(minConfidence)

val antecedent_length_count = rules.map(x => (x.antecedent.length,1)).reduceByKey((x,y) => x+y).collect().sortBy(x => -x._1)
// Array((4,65), (3,204), (2,538), (1,4163))

import org.apache.spark.mllib.fpm.AssociationRules.Rule
import org.apache.spark.mllib.fpm.{AssociationRules, FPGrowth}


val length_confidence = new Ordering[AssociationRules.Rule[Long]]{
  override def compare(x: Rule[Long], y: Rule[Long]): Int = if(x.antecedent.length != y.antecedent.length){
    y.antecedent.length - x.antecedent.length
  }else {
    if (- x.confidence + y.confidence > 0){
      1
    }else if(x.confidence == y.confidence){
      0
    }else{
      -1
    }
  }
}
val rules_origin = rules.collect
val rules_ordered = rules_origin.sorted(length_confidence)













//////////////////////////////// 测试
for (minSupport <- Array(0.00001,0.00003,0.0001,0.0003,0.001,0.003)) {
	val fpg = new FPGrowth().setMinSupport(minSupport)
	val model = fpg.run(train)
	val num = model.freqItemsets.count
	println(minSupport+":"+num)
}
1.0E-5:46757
3.0E-5:4796
1.0E-4:456
3.0E-4:58
0.001:1
0.003:0


for(minConfidence <- Array (0.0003,0.001,0.003,0.01,0.03,0.1,0.3)) println(minConfidence+":"+model.generateAssociationRules(minConfidence).count)
3.0E-4:6708
0.001:6708
0.003:6708
0.01:6701
0.03:6587
0.1:5935
0.3:3944
