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
    val conf = new SparkConf().setAppName("TriangleCount") 
    /** Create the SparkContext */ 
    val spark = new SparkContext(conf) 
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    var graph = GraphLoader.edgeListFile(spark, "./Graph/facebook.edge_list")
 
    val verties = graph.vertices.collect();
    val vertex = verties(4)._1
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



    


    

    /** Stop the SparkContext */ 
    spark.stop() 
   } 
} 
