package org.apache.spark

import com.github.fommil.netlib.F2jBLAS
import org.apache.spark.mllib.rdd.MLPairRDDFunctions._
import org.apache.spark.rdd.RDD
import org.apache.spark.util.BoundedPriorityQueue

/**
  * ALS 使用的工具类
  * //@Author: fansy 
  * //@Time: 2018/10/26 11:34
  * //@Email: fansy1990@foxmail.com
  */
object AlsUtil {
  @transient private var _f2jBLAS: F2jBLAS = new F2jBLAS()

  /**
    * Makes recommendations for all users (or products).
    *
    * Note: the previous approach used for computing top-k recommendations aimed to group
    * individual factor vectors into blocks, so that Level 3 BLAS operations (gemm) could
    * be used for efficiency. However, this causes excessive GC pressure due to the large
    * arrays required for intermediate result storage, as well as a high sensitivity to the
    * block size used.
    *
    * The following approach still groups factors into blocks, but instead computes the
    * top-k elements per block, using dot product and an efficient [[BoundedPriorityQueue]]
    * (instead of gemm). This avoids any large intermediate data structures and results
    * in significantly reduced GC pressure as well as shuffle data, which far outweighs
    * any cost incurred from not using Level 3 BLAS operations.
    *
    * @param rank        rank
    * @param srcFeatures src features to receive recommendations
    * @param dstFeatures dst features used to make recommendations
    * @param num         number of recommendations for each record
    * @return an RDD of (srcId: Int, recommendations), where recommendations are stored as an array
    *         of (dstId, rating) pairs.
    */
  def recommendForAll(
                       rank: Int,
                       srcFeatures: RDD[(Int, Array[Double])],
                       dstFeatures: RDD[(Int, Array[Double])],
                       num: Int): RDD[(Int, Array[(Int, Double)])] = {
    val srcBlocks = blockify(srcFeatures)
    val dstBlocks = blockify(dstFeatures)
    val ratings = srcBlocks.cartesian(dstBlocks).flatMap { case (srcIter, dstIter) =>
      val m = srcIter.size
      val n = math.min(dstIter.size, num)
      val output = new Array[(Int, (Int, Double))](m * n)
      var i = 0
      val pq = new BoundedPriorityQueue[(Int, Double)](n)(Ordering.by(_._2))
      srcIter.foreach { case (srcId, srcFactor) =>
        dstIter.foreach { case (dstId, dstFactor) =>
          // We use F2jBLAS which is faster than a call to native BLAS for vector dot product
          val score = _f2jBLAS.ddot(rank, srcFactor, 1, dstFactor, 1)
          pq += dstId -> score
        }
        pq.foreach { case (dstId, score) =>
          output(i) = (srcId, (dstId, score))
          i += 1
        }
        pq.clear()
      }
      output.toSeq
    }
    ratings.topByKey(num)(Ordering.by(_._2))
  }


  /**
    * Blockifies features to improve the efficiency of cartesian product
    * TODO: SPARK-20443 - expose blockSize as a param?
    */
  private def blockify(
                        features: RDD[(Int, Array[Double])],
                        blockSize: Int = 4096): RDD[Seq[(Int, Array[Double])]] = {
    features.mapPartitions { iter =>
      iter.grouped(blockSize)
    }
  }


}
