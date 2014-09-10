name := "Geospot java parts"

version := "rolling"

libraryDependencies += "junit" % "junit" % "4.8.1" % "test"

libraryDependencies += "trove" % "trove" % "1.0.2"

libraryDependencies += "com.fasterxml.jackson.core" % "jackson-core" % "2.4.2"

libraryDependencies += "com.fasterxml.jackson.core" % "jackson-databind" % "2.4.2"

libraryDependencies += "com.fasterxml.jackson.module" % "jackson-module-scala_2.10" % "2.4.2"

libraryDependencies += "de.undercouch" % "bson4jackson" % "2.4.0"

scalaVersion := "2.11.2"
            
javacOptions ++= Seq("-source", "1.8")
