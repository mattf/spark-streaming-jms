import AssemblyKeys._

assemblySettings

mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
  {
    case PathList("META-INF", "maven","org.slf4j","slf4j-api", ps) if ps.startsWith("pom") => MergeStrategy.discard
    case PathList("com", "esotericsoftware", "minlog", xs @ _*)     => MergeStrategy.first
    case PathList("com", "google", "common", "base", xs @ _*)       => MergeStrategy.first
    case PathList("org", "apache", "commons", xs @ _*)              => MergeStrategy.first
    case PathList("org", "apache", "hadoop", xs @ _*)               => MergeStrategy.first
    case PathList("org", "apache", "spark", "unused",  xs @ _*)     => MergeStrategy.first
    case x => old(x)
  }
}
