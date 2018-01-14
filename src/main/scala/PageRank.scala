import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx.{Graph, GraphLoader, Pregel}

/**
  * PageRank main class
  */
object Pagerank {
  /**
    * Main method
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {
    val sparkContext = SparkSession
      .builder
      .appName("PageRank-GraphX")
      .config("spark.eventLog.enabled", "true")
      .config("spark.eventLog.dir", "logs")
      .config("spark.executor.memory", "8g")
      .config("spark.driver.memory", "8g")
      .config("spark.executor.cores", "4")
      .config("spark.task.cpus", "1")
      .master("spark://10.254.0.141:7077")
      .getOrCreate().sparkContext

    // Load graph from HDFS file
    val graph = GraphLoader.edgeListFile(sparkContext, args(0))

    // Create input graph
    val inputGraph: Graph[Int, Int] = graph.outerJoinVertices(graph.outDegrees)((vid, _, degOpt) => degOpt.getOrElse(0))

    // Output graph
    val outputGraph = inputGraph.mapTriplets(triplet => 1.0 / triplet.srcAttr).mapVertices((id, _) => 1.0)

    // Calculate ranks
    val ranks = Pregel(outputGraph, 0.0, 20)(
      (id, attr, msgSum) => 0.15 + (0.85 * msgSum),
      triplet => Iterator((triplet.dstId, triplet.srcAttr * triplet.attr)),
      (a, b) => a + b)
      .vertices

    // Save ranks
    ranks.coalesce(1, true).saveAsTextFile("page-rank-output")
  }

}
