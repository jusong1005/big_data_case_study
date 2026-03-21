node2 :
spark-shell --total-executor-cores 20 --executor-memory 5G --name fansy

val train = sqlContext.sql("select * from law_init1.data_101003_encoded_train")
val validate = sqlContext.sql("select * from law_init1.data_101003_encoded_validate")

/////////////// 算法参数：
 val ranks:Array[Int] = Array(8,10,12,15)
 val iterations:Array[Int]= Array(8,10,15,20)
 val regs:Array[Double]= Array(0.001,0.03,0.09,0.1,0.3,0.9)
 val alphas:Array[Double]= Array(0.3,1.0,3.0)
 val implicitPrefs:Array[Boolean] = Array(true,false)
 val userCol:String="uid"
 val itemCol:String="pid"
 val ratingCol:String="u_p_rate"
 val bestModelPath:String = "fansy/law/best_als_model"

import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

def trans_times_2_rate(times:Long):Double =  if(times <=8 )  times else if (times > 23) 10 else 9

val times_2_rate = udf{(times:Long) => trans_times_2_rate(times)}
val trainRdd = train.select(userCol,itemCol).groupBy(userCol,itemCol).agg(count(lit(1)) as ratingCol).withColumn(ratingCol, times_2_rate(col(ratingCol)) ).rdd.map(x => Rating(x.getLong(0).toInt,x.getLong(1).toInt,x.getDouble(2))).repartition(24)
val validateRdd = validate.select(userCol,itemCol).distinct().rdd.map(x => Rating(x.getLong(0).toInt,x.getLong(1).toInt,1.0)).repartition(8) // validate 评分没有用，不需要转换
trainRdd.cache()
validateRdd.cache()

case class ALSParam(rank:Int,iteration:Int, reg:Double,alpha:Double,implicitPrefs:Boolean)

def getAls(train:RDD[Rating],alsParam:ALSParam) ={
    if(alsParam.implicitPrefs) {
      ALS.trainImplicit(train,alsParam.rank,alsParam.iteration,alsParam.reg,alsParam.alpha)
    }else{
      ALS.train(train,alsParam.rank,alsParam.iteration,alsParam.reg)
    }
  }

def getWithRecommend(model:MatrixFactorizationModel, validate:RDD[Rating], recNum:Int)
  :RDD[(Int,(Array[Int], Array[Int]))]={
    validate.map(x => (x.user,x.product)).combineByKey[Array[Int]](
      (x :Int) => Array (x) ,
    (c:Array[Int],v:Int) =>  c :+ v,
      (c1:Array[Int],c2:Array[Int]) => c1 ++ c2
    ).map{ x => (x._1, (x._2, model.recommendProducts(x._1,recNum).map(_.product)))}
  }

  def evalute(predictResult: RDD[(Int, (Array[Rating], Array[Rating]))]):RDD[(Double,Double,Double)] = {
    val tp_fp_fn = predictResult.map{x =>
      val realItems = x._2._1.map(_.product).toSet
      val recItems = x._2._2.map(_.product).toSet
      ( realItems.&(recItems).size, recItems.diff(realItems).size, realItems.diff(recItems).size)
    }
    tp_fp_fn.map[(Double,Double,Double)](x =>
      (
        if(x._1 == 0) 0.0 else  x._1.toDouble /(x._1 + x._2),
        if(x._1 == 0) 0.0 else  x._1.toDouble /(x._1 + x._3),
        2 * x._1.toDouble /(x._1 + x._2) * x._1.toDouble /(x._1 + x._3)/ (x._1.toDouble /(x._1 + x._2) + x._1.toDouble /(x._1 + x._3))
      )
    )
  }

var bestRank = 8
var bestIter = 8
var bestReg = 0.1
var bestAlpha = 1.0
var bestImplicitPrefs = true
var bestF = 0.0
var bestModel :MatrixFactorizationModel= null
val recNum = 10

val rank =8
val iter = 10
val reg = 0.1
val alpha = 1.0
val implicitPref = true


println("rank\titeration\treg\timplicitPrefs\talpha\tprecision\t\recall\tfMeasure")
    for(rank <- ranks ; iter <- iterations; reg <- regs; alpha <- alphas; implicitPref <- implicitPrefs){
      // 1. 建模
      val model = getAls(trainRdd,ALSParam(rank,iter,reg,alpha,implicitPref))
      // 2. 预测
      val predictResult = getWithRecommend(model,validateRdd,10)
      // 3. 评估
      val precision_recall_fMeasure = evalute(predictResult).map(x =>(1,x)).reduce((x1,x2) =>
        (x1._1+x2._1,(x1._2._1+x2._2._1, x1._2._2+x2._2._2, x1._2._3+x2._2._3)))

      val precision = precision_recall_fMeasure._2._1 / precision_recall_fMeasure._1
      val recall = precision_recall_fMeasure._2._2 / precision_recall_fMeasure._1
      val fMeasure = precision_recall_fMeasure._2._3 / precision_recall_fMeasure._1

      if(fMeasure > bestF){
        bestF = fMeasure
        bestAlpha = alpha
        bestImplicitPrefs = implicitPref
        bestIter = iter
        bestRank = rank
        bestReg = reg
        bestModel = model
      }
      println(rank+"\t" + iter +"\t" + reg+"\t"+implicitPref+"\t"+alpha+"\t"+precision+"\t"+recall+"\t"+fMeasure)
    }







////////////////////////////////////  spark-submit
spark-submit --total-executor-cores 20 --executor-memory 5G --name als_fp --class com.tipdm.als_fp.AlsModelSelection --files hdfs://server1:8020/user/root/hive-site.xml als_fp.jar law_init1.data_101003_encoded_train law_init1.data_101003_encoded_validate 8,10,12,15 8,10,15,20 0.03,0.09,0.1,0.3,0.9 0.3,1.0,3.0 true,false uid pid u_p_rate fansy/law/best_als_model 10,20,30 als_fp_app > als_fp.log 2>&1 &


spark-submit --total-executor-cores 20 --executor-memory 5G --name als_fp --class com.tipdm.als_fp.AlsModelSelection --files hdfs://server1:8020/user/root/hive-site.xml als_fp.jar law_init1.data_101003_encoded_train law_init1.data_101003_encoded_validate 8 10 0.1 1.0 true uid pid u_p_rate fansy/law/best_als_model 10 als_fp_app >> als_fp.log &
