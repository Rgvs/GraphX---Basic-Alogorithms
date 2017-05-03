/** Import the spark and math packages */ 
import scala.math.random 
import org.apache.spark._ 
import org.apache.spark.graphx._
import scala.runtime.ScalaRunTime._
import org.apache.spark.rdd.RDD
import scala.util.control._
import java.io._
import scala.collection.mutable.Queue
import scala.collection.mutable.Map
import org.apache.log4j.Logger
import org.apache.log4j.Level


 
object Ramsey { 
  def main(args: Array[String]) { 
    /** Create the SparkConf object */ 
    val conf = new SparkConf().setAppName("TriangleCount") 
    /** Create the SparkContext */ 
    val spark = new SparkContext(conf) 
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    var graph = GraphLoader.edgeListFile(spark, "../facebook.edge_list")
    val c_w = new PrintWriter(new File("C.txt"))
    val i_w = new PrintWriter(new File("I.txt"))
    Thread.sleep(5000)
    def ramsey(G: Graph[Int, Int]): (Queue[Long], Queue[Long]) = {
      val q = new Queue[(Long)]
      val q1 = new Queue[(Long)] 
      val vertex_maps:Map[VertexId, Boolean] = Map() 
      
      if (G.numVertices == 0)
        return (q, q1)
      val verties = G.vertices.collect();
      
      verties.foreach(v => {
        vertex_maps(v._1) = false
      })
      
      //println(stringOf(vertex_maps)+"\n1\n\n")

      val vertex = verties(0)._1
      //println("start vertex is "+ vertex + "\n\n\n\n")
      val nbrs = G.collectNeighborIds(EdgeDirection.Either)

      val vertex_nbrs = nbrs.filter(v => v._1.equals(vertex)).collect()(0)._2
      vertex_nbrs.foreach( v => {
        vertex_maps(v) = true   
      })
      //println("vertex_nbrs " + stringOf(vertex_nbrs))
      //println(stringOf(vertex_maps)+"\n\n\n")
      val g1 = G.subgraph(vpred = (vid, attr) => vid!=vertex && vertex_maps(vid) )
      val g2 = G.subgraph(vpred = (vid, attr) => vid!=vertex && !vertex_maps(vid) && vid != vertex )
       
       
      //println("g1 " + g1.numVertices)  
      //println("g2 " + g2.numVertices)  
      val ci1 = ramsey(g1)
      val ci2 = ramsey(g2)
      
      
      ci1._1.enqueue(vertex)
      ci2._2.enqueue(vertex)
      val c = { if (ci1._1.length > ci2._1.length) ci1._1 else ci2._1 }
      val i = { if (ci1._2.length > ci2._2.length) ci1._2 else ci2._2 }
      
      println(c.length, i.length)

      return (c,i)
    }

    println(ramsey(graph)+ " answer")

    


    


    

    /** Stop the SparkContext */ 
    spark.stop() 
    c_w.close()
    i_w.close()

  } 
} 
