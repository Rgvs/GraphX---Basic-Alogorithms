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
    val conf = new SparkConf().setAppName("TriangleCount") 
    /** Create the SparkContext */ 
    val spark = new SparkContext(conf) 
   
    var graph = GraphLoader.edgeListFile(spark, "../Graph/facebook.edge_list")
    val pw = new PrintWriter(new File("topological.txt"))
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    while(graph.numVertices > 0) {
      val inDeg = graph.inDegrees
      val inDeg_list = inDeg.collect()
      val inDeg_map: Map[VertexId, Boolean] = Map()
     
      val vertices = graph.vertices.collect()
      vertices.foreach(v => {
        inDeg_map(v._1) = false
      })
      inDeg_list.foreach(v => {
        inDeg_map(v._1) = true
      })
      val inDeg_zero = vertices.filter(vertex => !inDeg_map(vertex._1))
      for (x <- inDeg_zero)
          pw.write(x._1+"\n")
      println(stringOf(inDeg_zero))
      graph = graph.subgraph(vpred =(vid, attr) => inDeg_map(vid))
      println("Number of Vertices " + graph.numVertices  ) 
    }

    /** Stop the SparkContext */ 
    spark.stop() 
    pw.close()

  } 
} 
