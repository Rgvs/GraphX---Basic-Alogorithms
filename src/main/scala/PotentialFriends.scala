/** Import the spark and math packages */ 
import scala.math.random 
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.graphx._
import scala.runtime.ScalaRunTime._
import org.apache.spark.rdd.RDD
import scala.util.control._
import java.io._
import scala.collection.mutable.Queue
import scala.collection.mutable.Map
import org.apache.log4j.Logger
import org.apache.log4j.Level


 
object PotentialFriends { 
  def main(args: Array[String]) { 
    /** Create the SparkConf object */ 
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val conf = new SparkConf().setAppName("PotentialFriends") 
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

    val verties = graph.vertices.collect();
    val rnd = new scala.util.Random
    val index = 1 + rnd.nextInt(1000)
    val vertex = verties(index)._1
    println("start vertex "+ vertex) 
    val nbrs = graph.collectNeighborIds(EdgeDirection.Either).collect()
    val vertex_nbrs = nbrs.filter(v => v._1.equals(vertex))(0)._2
    println("vertex nbrs " + stringOf(vertex_nbrs))
    var raw_vertex: Array[Long] = Array()
    vertex_nbrs.foreach(v1 => {
      val raw_vertex_2nbr = nbrs.filter(v => v._1.equals(v1) )(0)._2
      val vertex_2nbr = raw_vertex_2nbr.filter(v => !vertex_nbrs.contains(v) && vertex != v )
      raw_vertex = raw_vertex ++ vertex_2nbr
      //println(stringOf(raw_vertex))
      //println(stringOf(nbrs))
    })
    val potential_friends = spark.parallelize(raw_vertex.map(v => (v, 1)))
    val pf =  potential_friends.reduceByKey((a,b)=> a + b )
      .map(item => item.swap)
      .sortByKey(false)
      .map(item => item.swap).collect()

    println("Potetial Friends")
    pf.foreach(println)
    val t2 = System.nanoTime()/1000000.0
    
    println("Elapsed time: " + (t0-t_1) +"\t" + (t2 - t0) + "\t"+ (t2-t_1) + "ms\n")
    /** Stop the SparkContext */ 
    spark.stop() 
   } 
} 
