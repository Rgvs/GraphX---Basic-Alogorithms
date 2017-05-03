/** Import the spark and math packages */ 
import scala.math.random 
import org.apache.spark._ 
import org.apache.spark.graphx._
import scala.runtime.ScalaRunTime._
import org.apache.spark.rdd.RDD
import scala.util.control._
import java.io._
import scala.collection.mutable.Queue
import org.apache.log4j.Logger
import org.apache.log4j.Level




 
object TopologicalSort2 { 
  def main(args: Array[String]) { 
    /** Create the SparkConf object */ 
    val conf = new SparkConf().setAppName("TriangleCount") 
    /** Create the SparkContext */ 
    val spark = new SparkContext(conf) 
   
    var graph = GraphLoader.edgeListFile(spark, "../Graph/facebook.edge_list")
    val pw = new PrintWriter(new File("topological.txt"))
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
   
    val inDeg = graph.inDegrees
    val inDeg_list = inDeg.map(vertex => vertex._1).collect()
    val vertices = graph.vertices
    val inDeg_zero = vertices.filter(vertex => !inDeg_list.contains(vertex._1))
    
    var inDeg_MapRDD = inDeg_zero.map(vertex => (vertex._1, 0)).union(inDeg)

    val queue = new Queue[(VertexId)]
    
    for (vertex <- inDeg_zero.toLocalIterator) 
       queue.enqueue(vertex._1) 
    
    val nbrs = graph.collectNeighbors(EdgeDirection.Out)
    
    while(!queue.isEmpty) {
      val vertex_top = queue.dequeue()
      pw.write(stringOf(vertex_top)+ "\n")
      val vertex_nbrs = nbrs.filter(vertex => vertex._1.equals(vertex_top)).collect()
      
      for (arr <- vertex_nbrs) {
        val nbrs_array = arr._2
        println(stringOf(arr) +" vertex Nbrsn ")
        for (vertex <- nbrs_array) {
          val oldInDeg = inDeg_MapRDD.filter(vertex_iter => (vertex_iter._1 == vertex._1)).first()._2
          val newInDeg = oldInDeg - 1
          //println("new vertex id of " + vertex._1 + " is " + newInDeg)
          
          inDeg_MapRDD = inDeg_MapRDD.map(vertex_iter => {
            if (vertex_iter._1 == vertex._1) 
              (vertex._1, newInDeg)
            else
              vertex_iter
          })

          if (newInDeg == 0)
            queue.enqueue(vertex._1)
        }
      }
    }


    

    /** Stop the SparkContext */ 
    spark.stop() 
    pw.close()

  } 
} 
