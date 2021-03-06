name := "SparkPi Project" 
version := "1.0" 
scalaVersion := "2.11.8" 
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.1.0"
libraryDependencies += "org.apache.spark" %% "spark-graphx" % "2.1.0"
libraryDependencies += "org.scalaz" %% "scalaz-core" % "7.2.12"
/*fork in run := true
javaOptions in run ++= Seq(
    "-Dlog4j.debug=true",
      "-Dlog4j.configuration=log4j.properties")
outputStrategy := Some(StdoutOutput)*/
