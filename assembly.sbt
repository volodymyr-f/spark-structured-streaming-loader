
import AssemblyKeys._ 

assemblySettings

jarName in assembly := "sparkStructuredStreamingLoader.jar"

mergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case PathList("META-INF", "mailcap") => MergeStrategy.discard
  case PathList("META-INF", "maven", "org.slf4j", "slf4j-api", xa @ _*) => MergeStrategy.rename
  case PathList("META-INF", "ECLIPSEF.RSA") => MergeStrategy.discard
  case PathList("com", "datastax", "driver", "core", "Driver.properties") => MergeStrategy.last
  case PathList("plugin.properties") => MergeStrategy.discard
  case x => MergeStrategy.first
}
