/** Import the spark and math packages */ 
import org.apache.spark._ 
import org.apache.spark.graphx._
import scala.runtime.ScalaRunTime._
import org.apache.spark.rdd.RDD
import scala.util.control._
import java.io._
import org.apache.log4j.Logger
import org.apache.log4j.Level


 
object TriangleCount { 
  def main(args: Array[String]) { 
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
 
    /** Create the SparkConf object */ 
    val conf = new SparkConf().setAppName("TriangleCount") 
    /** Create the SparkContext */ 
    val spark = new SparkContext(conf) 
    
    val t_1 = System.nanoTime()/1000000.0
    var graph = GraphLoader.edgeListFile(spark, args(0))
    //Thread.sleep(5000)
    val t0 = System.nanoTime()/1000000.0
    val triCounts = graph.triangleCount().vertices
     .map(x => x._2)
     .reduce((a,b) => a+b)
    val t2 = System.nanoTime()/1000000.0
    println(stringOf(triCounts/3))
    println("Elapsed time: " + (t0-t_1)+" "+(t2 - t0)+" "+" " + (t2-t_1) + "ms")

    /** Stop the SparkContext */ 
    spark.stop() 

  } 
} 
