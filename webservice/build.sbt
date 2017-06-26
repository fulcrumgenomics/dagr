assemblyJarName in assembly := "dagr-webservice-" + version.value + ".jar"
mainClass       in assembly := Some("dagr.webservice.DagrServerMain")
