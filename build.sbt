name := "SparkDataReportsWithAggregators"

version := "1.0"

scalaVersion := "2.11.7"

val spark = (name: String) => "org.apache.spark" %% s"spark-$name" % "1.6.0"

//libraryDependencies += spark("core")
libraryDependencies ++= Seq(
  spark("core"),
  spark("sql"),
  spark("mllib"),
  "org.scalatest" %% "scalatest" % "3.0.0-SNAP4" % Test
)