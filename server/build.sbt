assemblyJarName in assembly := "dagr-server-" + version.value + ".jar"
mainClass       in assembly := Some("dagr.server.DagrServerMain")
