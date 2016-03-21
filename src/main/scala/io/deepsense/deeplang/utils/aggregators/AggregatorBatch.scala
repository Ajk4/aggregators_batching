package io.deepsense.deeplang.utils.aggregators

import org.apache.spark.rdd.RDD

object AggregatorBatch {

  /**
    * Class used for providing option to access different return type per each bundled
    * aggregator in type-safe way.
    */
  case class BatchedResult(rawResults: Map[Aggregator[_, _], Any]) {

    def forAggregator[U, _](aggregator: Aggregator[U, _]): U = {
      rawResults(aggregator).asInstanceOf[U]
    }
  }

  def executeInBatch[T](
      rdd: RDD[T],
      aggregators: Seq[Aggregator[_, T]]): BatchedResult = {
    val batch = MultiAggregator[Any, T](aggregators.map(_.asInstanceOf[Aggregator[Any, T]]))
    val results = batch.execute(rdd)

    val rawResultsMap: Map[Aggregator[_, _], Any] = (aggregators zip results).map {
      case (aggregator, result) => aggregator -> result
    }.toMap
    BatchedResult(rawResultsMap)
  }

  private case class MultiAggregator[U, T](aggregators: Seq[Aggregator[U, T]])
    extends Aggregator[Seq[U], T] {

    override def initialElement: Seq[U] = aggregators.map(_.initialElement)

    override def mergeValue(accSeq: Seq[U], elem: T): Seq[U] = {
      (accSeq, aggregators).zipped.map { (acc, aggregator) =>
        aggregator.mergeValue(acc, elem)
      }
    }

    override def mergeCombiners(leftSeq: Seq[U], rightSeq: Seq[U]): Seq[U] = {
      (leftSeq, rightSeq, aggregators).zipped.map { (left, right, aggregator) =>
        aggregator.mergeCombiners(left, right)
      }
    }
  }
}
