import sbtassembly.MergeStrategy

name := "data_generator"

organization := "bi.ria"

version := "1.0"

sbtPlugin := true

publishTo := Some("Ria develop Artifactory" at "http://artifactory.datanet.ria:8081/artifactory/develop")


// We do not follow the standard scala structure. This maps our structure to the standard one.
scalaSource in Compile := baseDirectory.value / "src" / "main"
scalaSource in Test := baseDirectory.value / "src" / "test"
scalaSource in IntegrationTest := baseDirectory.value / "src" / "it"
javaSource in Compile := baseDirectory.value / "src" / "main"
javaSource in Test := baseDirectory.value / "src" / "test"
javaSource in IntegrationTest := baseDirectory.value / "src" / "it"


lazy val sparkVersion = "1.6.0"
libraryDependencies ++= Seq()
libraryDependencies += "org.scala-sbt" % "io" % "0.13.9" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"
libraryDependencies += "org.apache.spark" %% "spark-hive" % sparkVersion % "provided"
libraryDependencies += "com.databricks" %% "spark-csv" % "1.2.0"

libraryDependencies += "com.github.nscala-time" %% "nscala-time" % "2.12.0"


resolvers += Resolver.url("typesafe", url("http://repo.typesafe.com/typesafe/ivy-releases/"))(Resolver.ivyStylePatterns)
resolvers += "Ria develop Artifactory" at "http://artifactory.datanet.ria:8081/artifactory/develop"


assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

// Scalariform settings
import scalariform.formatter.preferences._
import com.typesafe.sbt.SbtScalariform
import com.typesafe.sbt.SbtScalariform.ScalariformKeys
SbtScalariform.scalariformSettings
ScalariformKeys.preferences := ScalariformKeys.preferences.value.setPreference(SpaceInsideParentheses, true)