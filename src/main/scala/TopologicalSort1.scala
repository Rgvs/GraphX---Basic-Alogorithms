/** Import the spark and math packages */ 
import scala.math.random 
import org.apache.spark._ 
import org.apache.spark.graphx._
import scala.runtime.ScalaRunTime._
import org.apache.spark.rdd.RDD
import scala.util.control._
import java.io._
import scala.collection.mutable.Map
import org.apache.log4j.Logger
import org.apache.log4j.Level


 
object TopologicalSort1 { 
  def main(args: Array[String]) { 
    /** Create the SparkConf object */ 
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    
    val conf = new SparkConf().setAppName("TopologicalSort1") 
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

 
    val pw = new PrintWriter(new File("topological.txt"))
   
    val t0 = System.nanoTime()/1000000.0
    
    val inDeg_map: Map[VertexId, Boolean] = Map()
    var flag = true
    val vertices = graph.vertices.collect()
    
    vertices.foreach(v => {
      inDeg_map(v._1) = false
    })
    
    while(graph.numVertices > 0) {
      val inDeg = graph.inDegrees
      val inDeg_list = inDeg.collect()
      if (flag == true) { 
        inDeg_list.foreach(v => {
          inDeg_map(v._1) = true
        })
      }
      else {
        inDeg_list.foreach(v => {
          inDeg_map(v._1) = false
        })
      }
      
      val inDeg_zero = vertices.filter(vertex => (flag && !inDeg_map(vertex._1)) || (!flag && inDeg_map(vertex._1)))
      for (x <- inDeg_zero)
          pw.write(x._1+"\n")
      graph = graph.subgraph(vpred =(vid, attr) => ( flag && inDeg_map(vid) ) || (!flag && !inDeg_map(vid)))
      flag = !flag
    }
    
    val t2 = System.nanoTime()/1000000.0
    
    println("Elapsed time: " + (t0-t_1) +"\t" + (t2 - t0) + "\t"+ (t2-t_1) + "ms\n")
    /** Stop the SparkContext */ 
    spark.stop() 
    pw.close()

  } 
} 
