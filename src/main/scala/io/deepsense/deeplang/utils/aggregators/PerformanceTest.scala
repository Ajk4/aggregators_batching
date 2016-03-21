package io.deepsense.deeplang.utils.aggregators

import java.lang

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.{SparkConf, SparkContext}

import io.deepsense.deeplang.utils.aggregators.AggregatorBatch.BatchedResult

object PerformanceTest {

  def data = Stream.from(0).map { i =>
    Row(
      i % 100D,
      i % 200D,
      i % 300D,
      i % 400D,
      i % 500D
    )
  }.take(1000000)

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Application").setMaster("local[8]")
    val sc = new SparkContext(conf)
    val rows = sc.parallelize(data, 12)

    val buckets: Array[Double] = Array(0, 100, 200, 300, 400, 500)

    // job 0 - forces Spark to cache data in the cluster. It will take away data-loading overhead
    // from future jobs and make future measurements reliable.
    rows.foreach((_) => ())

    val seqResult = sequentialProcessing(rows, buckets)
    val batchedResult = batchedProcessing(rows, buckets)

    scala.io.StdIn.readLine()
  }

  private def sequentialProcessing(rows: RDD[Row], buckets: Array[Double]): Seq[Array[Long]] =
    for (i <- 0 until 5) yield {
      val column = rows.map(extractDoubleColumn(i))
      column.histogram(buckets, true)
    }

  private def batchedProcessing(rows: RDD[Row], buckets: Array[Double]): Seq[Array[Long]] = {
    val aggregators = for (i <- 0 until 5) yield
      HistogramAggregator(buckets, true).mapInput(extractDoubleColumn(i))
    val batchedResult = AggregatorBatch.executeInBatch(rows, aggregators)
    for (aggregator <- aggregators) yield batchedResult.forAggregator(aggregator)
  }

//  Usage example:
//
//  // Aggregators to execute in a batch
//  val aggregators: Seq[Aggregator[_,_]] = ???
//  // Batched result executed in one spark call
//  val batchedResult: BatchedResult = AggregatorBatch.executeInBatch(rows, aggregators)
//  // Accessing result is done by passing origin aggregator to the result object
//  val firstAggregator = aggregators(0)
//  // Type of `firstResult` is derived from output type of `firstAggregator`
//  val firstResult = batchedResult.forAggregator(firstAggregator)

  private def extractDoubleColumn(col: Int)(row: Row) =
    Double.unbox(row.get(col).asInstanceOf[lang.Double])

}
