name := "spark-pubsub-connector-root"

version := "0.1.0"

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
  "-Dorg.apache.arrow.memory.util.MemoryUtil.DISABLE_UNSAFE_DIRECT_MEMORY_ACCESS=false",
  "-Darrow.memory.debug.allocator=false",
  "-Dio.netty.tryReflectionSetAccessible=true"
)

lazy val commonSettings = Seq(
  version := "0.1.0",
  fork in Test := true,
  envVars in Test := Map(
    "LD_PRELOAD" -> "/lib/x86_64-linux-gnu/libgcc_s.so.1",
    "PUBSUB_EMULATOR_HOST" -> sys.env.getOrElse("PUBSUB_EMULATOR_HOST", "localhost:8085")
  )
)

lazy val spark3 = (project in file("."))
  .settings(commonSettings)
  .settings(
    name := "spark-pubsub-connector",
    scalaVersion := "2.12.18",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.5.0",
      "org.apache.spark" %% "spark-sql" % "3.5.0",
      "org.apache.spark" %% "spark-catalyst" % "3.5.0",
      "org.apache.arrow" % "arrow-vector" % "15.0.2",
      "org.apache.arrow" % "arrow-memory-netty" % "15.0.2",
      "org.apache.arrow" % "arrow-c-data" % "15.0.2",
      "org.apache.arrow" % "arrow-format" % "15.0.2",
      "org.scalatest" %% "scalatest" % "3.2.16" % Test
    ),
    dependencyOverrides ++= Seq(
      "org.apache.arrow" % "arrow-vector" % "15.0.2",
      "org.apache.arrow" % "arrow-memory-netty" % "15.0.2",
      "org.apache.arrow" % "arrow-memory-core" % "15.0.2",
      "org.apache.arrow" % "arrow-c-data" % "15.0.2",
      "com.fasterxml.jackson.core" % "jackson-databind" % "2.16.0",
      "com.fasterxml.jackson.core" % "jackson-core" % "2.16.0",
      "com.fasterxml.jackson.core" % "jackson-annotations" % "2.16.0",
      "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.16.0"
    ),
    Test / javaOptions ++= javaOpts :+ s"-Djava.library.path=${baseDirectory.value.getParent}/native/target/debug"
  )

lazy val spark4 = (project in file("spark4-build"))
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
      "org.apache.arrow" % "arrow-format" % "15.0.2",
      "org.scalatest" %% "scalatest" % "3.2.16" % Test
    ),
    Compile / unmanagedSourceDirectories += baseDirectory.value.getParentFile / "src" / "main" / "scala",
    Test / unmanagedSourceDirectories += baseDirectory.value.getParentFile / "src" / "test" / "scala",
    Compile / unmanagedResourceDirectories += baseDirectory.value.getParentFile / "src" / "main" / "resources",
    Test / unmanagedResourceDirectories += baseDirectory.value.getParentFile / "src" / "test" / "resources",
    Test / javaOptions ++= javaOpts :+ s"-Djava.library.path=${baseDirectory.value.getParentFile.getParent}/native/target/debug"
  )
