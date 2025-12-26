name := "spark-pubsub-connector-root"

version := "0.1.0"

lazy val root = (project in file("."))
  .aggregate(spark33, spark35, spark40)
  .settings(
    publish / skip := true,
    Compile / sources := Seq.empty,
    Test / sources := Seq.empty
  )

lazy val javaOpts = Seq(
  "-DRUST_BACKTRACE=1",
  "--add-opens=java.base/java.lang=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
  "-Xmx2G",
  "--add-opens=java.base/java.io=ALL-UNNAMED",
  "--add-opens=java.base/java.net=ALL-UNNAMED",
  "--add-opens=java.base/java.nio=ALL-UNNAMED",
  "--add-opens=java.base/java.util=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
  "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
  "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
  "--add-opens=jdk.unsupported/sun.misc=ALL-UNNAMED",
  "--add-exports=jdk.unsupported/sun.misc=ALL-UNNAMED",
  "--add-opens=java.base/sun.util.logging=ALL-UNNAMED",
  "--add-opens=java.base/javax.security.auth=ALL-UNNAMED",
  "-Dorg.apache.arrow.memory.util.MemoryUtil.DISABLE_UNSAFE_DIRECT_MEMORY_ACCESS=false",
  "-Darrow.memory.debug.allocator=false",
  "-Dio.netty.tryReflectionSetAccessible=true"
)



lazy val copyNativeLibs = taskKey[Unit]("Copies native libraries to resources")

lazy val commonSettings = Seq(
  version := "0.1.0",
  javacOptions ++= Seq("-source", "1.8", "-target", "1.8"),
  scalacOptions ++= Seq("-target:jvm-1.8", "-Xlint", "-deprecation", "-feature"),
  fork in Test := true,
  envVars in Test := Map(
    "PUBSUB_EMULATOR_HOST" -> sys.env.getOrElse("PUBSUB_EMULATOR_HOST", "localhost:8085")
  ),
  libraryDependencies += "com.google.cloud" % "google-cloud-core" % "2.33.0" exclude("commons-logging", "commons-logging"),
  copyNativeLibs := {
    val log = streams.value.log
    // baseDirectory is modules/spark35, parent is spark, parent parent is root
    val rootDir = baseDirectory.value.getParentFile.getParentFile
    val nativeTarget = rootDir / "native" / "target" / "release"
    val resourceDir = rootDir / "spark" / "src" / "main" / "resources"
    
    // Detect current Arch (rudimentary)
    val os = System.getProperty("os.name").toLowerCase
    val arch = System.getProperty("os.arch").toLowerCase
    
    val osName = if (os.contains("linux")) "linux" else if (os.contains("mac")) "darwin" else "unknown"
    val archName = if (arch == "amd64" || arch == "x86_64") "x86-64" else if (arch == "aarch64") "aarch64" else "unknown"
    
    if (osName != "unknown" && archName != "unknown") {
      val platformDir = resourceDir / s"$osName-$archName"
      if (!platformDir.exists()) platformDir.mkdirs()
      
      val extension = if (osName == "darwin") "dylib" else "so"
      val libName = s"libnative_pubsub_connector_glibc_2_31.$extension"
      val sourceFile = nativeTarget / s"libnative_pubsub_connector.$extension"
      
      if (sourceFile.exists()) {
        val destFile = platformDir / libName
        IO.copyFile(sourceFile, destFile)
        log.info(s"Copied native lib to $destFile")
      } else {
        log.warn(s"Native lib not found at $sourceFile. Run 'cargo build --release' in native/ first.")
      }
    }
  },
  // Run copyNativeLibs before Compile
  Compile / compile := ((Compile / compile) dependsOn copyNativeLibs).value
)

lazy val spark33 = (project in file("spark33"))
  .settings(commonSettings)
  .settings(
    name := "spark-pubsub-connector-3.3",
    scalaVersion := "2.12.18",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.3.4",
      "org.apache.spark" %% "spark-sql" % "3.3.4",
      "org.apache.spark" %% "spark-catalyst" % "3.3.4",
      "org.apache.arrow" % "arrow-vector" % "15.0.2",
      "org.apache.arrow" % "arrow-memory-netty" % "15.0.2",
      "org.apache.arrow" % "arrow-c-data" % "15.0.2",
      "org.scalatest" %% "scalatest" % "3.2.16" % Test
    ),
    Compile / unmanagedSourceDirectories += baseDirectory.value.getParentFile / "src" / "main" / "scala",
    Test / unmanagedSourceDirectories += baseDirectory.value.getParentFile / "src" / "test" / "scala",
    Compile / unmanagedResourceDirectories += baseDirectory.value.getParentFile / "src" / "main" / "resources",
    Test / unmanagedResourceDirectories += baseDirectory.value.getParentFile / "src" / "test" / "resources",
    Test / javaOptions ++= javaOpts :+ s"-Djava.library.path=${baseDirectory.value.getParentFile.getParentFile}/native/target/release"
  )

lazy val spark35 = (project in file("spark35"))
  .settings(commonSettings)
  .settings(
    name := "spark-pubsub-connector-3.5",
    scalaVersion := "2.12.18",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.5.3",
      "org.apache.spark" %% "spark-sql" % "3.5.3",
      "org.apache.spark" %% "spark-catalyst" % "3.5.3",
      "org.apache.arrow" % "arrow-vector" % "15.0.2",
      "org.apache.arrow" % "arrow-memory-netty" % "15.0.2",
      "org.apache.arrow" % "arrow-c-data" % "15.0.2",
      "org.scalatest" %% "scalatest" % "3.2.16" % Test
    ),
    Compile / unmanagedSourceDirectories += baseDirectory.value.getParentFile / "src" / "main" / "scala",
    Test / unmanagedSourceDirectories += baseDirectory.value.getParentFile / "src" / "test" / "scala",
    Compile / unmanagedResourceDirectories += baseDirectory.value.getParentFile / "src" / "main" / "resources",
    Test / unmanagedResourceDirectories += baseDirectory.value.getParentFile / "src" / "test" / "resources",
    Test / javaOptions ++= javaOpts :+ s"-Djava.library.path=${baseDirectory.value.getParentFile.getParentFile}/native/target/release",
    // Jackson Overrides to match Spark 3.5 expectation and avoid version mismatch errors
    dependencyOverrides ++= Seq(
      "com.fasterxml.jackson.core" % "jackson-databind" % "2.15.2",
      "com.fasterxml.jackson.core" % "jackson-core" % "2.15.2",
      "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.15.2"
    ),
    assembly / assemblyMergeStrategy := {
      case "org/apache/commons/logging/impl/NoOpLog.class" => MergeStrategy.discard
      case PathList("META-INF", "services", xs @ _*) => MergeStrategy.concat
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case _ => MergeStrategy.first
    }
  )

lazy val spark40 = (project in file("spark40"))
  .settings(commonSettings)
  .settings(
    name := "spark-pubsub-connector-4.0",
    scalaVersion := "2.13.13",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "4.0.0-preview2",
      "org.apache.spark" %% "spark-sql" % "4.0.0-preview2",
      "org.apache.spark" %% "spark-catalyst" % "4.0.0-preview2",
      "org.apache.arrow" % "arrow-vector" % "15.0.2",
      "org.apache.arrow" % "arrow-memory-netty" % "15.0.2",
      "org.apache.arrow" % "arrow-c-data" % "15.0.2",
      "org.scalatest" %% "scalatest" % "3.2.16" % Test
    ),
    // Explicitly pin Jackson/Netty to Spark 4.0 versions
    dependencyOverrides ++= Seq(
      "com.fasterxml.jackson.core" % "jackson-databind" % "2.17.2",
      "com.fasterxml.jackson.core" % "jackson-core" % "2.17.2",
      "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.17.2",
      "io.netty" % "netty-all" % "4.1.110.Final",
      "io.netty" % "netty-transport-native-epoll" % "4.1.110.Final",
      "com.github.luben" % "zstd-jni" % "1.5.6-5"
    ),
    Compile / unmanagedSourceDirectories += baseDirectory.value.getParentFile / "src" / "main" / "scala",
    Test / unmanagedSourceDirectories += baseDirectory.value.getParentFile / "src" / "test" / "scala",
    Compile / unmanagedResourceDirectories += baseDirectory.value.getParentFile / "src" / "main" / "resources",
    Test / unmanagedResourceDirectories += baseDirectory.value.getParentFile / "src" / "test" / "resources",
    Test / javaOptions ++= javaOpts :+ s"-Djava.library.path=${baseDirectory.value.getParentFile.getParentFile}/native/target/release"
  )
