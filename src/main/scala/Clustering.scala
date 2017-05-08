/** Import the spark and math packages */ 
import org.apache.spark._ 
import org.apache.spark.graphx._
import scala.runtime.ScalaRunTime._
import org.apache.spark.rdd.RDD
import scala.util.control._
import java.io._
import org.apache.log4j.Logger
import org.apache.log4j.Level


 
object Clustering { 
  def main(args: Array[String]) { 
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
 
    /** Create the SparkConf object */ 
    val conf = new SparkConf().setAppName("Clustering") 
    /** Create the SparkContext */ 
    val spark = new SparkContext(conf)
    val t_1 = System.nanoTime()/1000000.0
    
    val ps = PartitionStrategy.CanonicalRandomVertexCut
    
    var graph:Graph[Int, Int] = GraphLoader.edgeListFile(spark, args(0))
      .partitionBy(ps)

    if (args(1) == 3)
      graph = graph.partitionBy(PartitionStrategy.RandomVertexCut)
    else if (args(1) == 2)
      graph = graph.partitionBy(PartitionStrategy.EdgePartition1D)
    else if (args(1) == 1)
      graph = graph.partitionBy(PartitionStrategy.EdgePartition2D)

    val t0 = System.nanoTime()/1000000.0
    val triCounts = graph.triangleCount().vertices
    val outDegree = graph.outDegrees.cache()
    val inDegree = graph.inDegrees.cache()
    val degree = outDegree.fullOuterJoin(inDegree)
      .map{case (id, (a,b)) => (id, a.getOrElse(0)+b.getOrElse(0)) }
    val clustering = triCounts.join(degree)
      .map{case(id, (tc, deg)) => if (tc > 0) 2*tc/(deg*(deg-1.0)) else 0.0}
      .reduce((a,b) => a+b)
    println(stringOf(clustering/graph.numVertices))
    val t2 = System.nanoTime()/1000000.0
    
    //, stringOf(degree.collect()), stringOf(triCounts.collect()))
    //println(stringOf(degree.collect()))
    //println(stringOf(inDegree.collect()))
    //println(stringOf(outDegree.collect()))
    println("Elapsed time: " + (t0-t_1) +"\t" + (t2 - t0) + "\t"+ (t2-t_1) + "ms\n")

    /** Stop the SparkContext */ 
    spark.stop() 

  } 
} 
