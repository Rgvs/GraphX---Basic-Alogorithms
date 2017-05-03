/** Import the spark and math packages */ 
import scala.math.random 
import org.apache.spark._ 
import org.apache.spark.graphx._
import scala.runtime.ScalaRunTime._
import org.apache.spark.rdd.RDD
import scala.util.control._
import java.io._
import scala.collection.mutable.Queue



 
object TopologicalSort3 { 
  def main(args: Array[String]) { 
    /** Create the SparkConf object */ 
    val conf = new SparkConf().setAppName("TriangleCount") 
    /** Create the SparkContext */ 
    val spark = new SparkContext(conf) 
   
    var graph = GraphLoader.edgeListFile(spark, "../SBU/PGX/graphs/s4.edge_list")
    val pw = new PrintWriter(new File("topological.txt"))
    
    // graph = graph.mapEdges(e => 1) 
    
    //println(stringOf(graph.edges.collect()))
    def vertexProgram(vertexId: VertexId, value:Int, msg: Int): Int = {
      
      println(vertexId+"\nfinally\n\n")
      if (msg == 0){
        println(vertexId+"\n\n\n")
        return 0
      }
      else return 1;
      

    } 
    
    def sendMessage(triplet: EdgeTriplet[Int, Int]): Iterator[(VertexId, Int)] = {
      Iterator((triplet.dstId, 1))
    }
    
    def mergeMsg(msg1: Int, msg2: Int): Int = msg1 + msg2

    val tsgraph = graph.pregel(Int.MaxValue, 100, EdgeDirection.Out)(
      vertexProgram, sendMessage, mergeMsg)

    /** Stop the SparkContext */ 
    spark.stop() 
    pw.close()

  } 
} 
