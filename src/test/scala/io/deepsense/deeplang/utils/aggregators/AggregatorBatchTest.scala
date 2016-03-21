package io.deepsense.deeplang.utils.aggregators

class AggregatorBatchTest extends ApacheSparkTest {

  "AggregatorBatch" should {
    "properly execute all aggregation operations for provided aggregators" in {
      val rdd = sparkContext.parallelize(Seq(1, 2, 3))

      val setAggregator = SetAggregator()
      val sumAggregator = SumAggregator()

      val results = AggregatorBatch.executeInBatch(rdd, Seq(setAggregator, sumAggregator))

      results.forAggregator(setAggregator) shouldEqual Set(1, 2, 3)
      results.forAggregator(sumAggregator) shouldEqual 6
    }
  }
}

case class SetAggregator() extends Aggregator[Set[Int], Int] {
  override def initialElement: Set[Int] = Set.empty
  override def mergeValue(acc: Set[Int], elem: Int): Set[Int] = acc + elem
  override def mergeCombiners(left: Set[Int], right: Set[Int]): Set[Int] = left ++ right
}

case class SumAggregator() extends Aggregator[Int, Int] {
  override def initialElement: Int = 0
  override def mergeValue(acc: Int, elem: Int): Int = acc + elem
  override def mergeCombiners(left: Int, right: Int): Int = left + right
}