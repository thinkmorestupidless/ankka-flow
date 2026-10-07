// {{name}}: an ankka-flow streamlet. README.md has every command.
ThisBuild / scalaVersion := "{{scala_version}}"

lazy val descriptor      = taskKey[Unit]("Writes flow/descriptor.json from the streamlet's declaration")
lazy val descriptorCheck = taskKey[Unit]("Fails when flow/descriptor.json differs from the declaration")

lazy val root = project
  .in(file("."))
  .enablePlugins(JavaAppPackaging, DockerPlugin)
  .settings(
    name    := "{{name}}",
    version := "0.1.0",
    libraryDependencies ++= Seq(
      // The SDK of the ankka-flow release this project was written by; move it with the sidecar
      // image in docker-compose.yml.
      "com.thinkmorestupidless" %% "ankka-flow-sdk" % "{{flow_version}}",
      "org.slf4j"                % "slf4j-simple"   % "2.0.17",
      "org.scalameta"           %% "munit"          % "1.3.6" % Test
    ),
    Compile / mainClass := Some("{{package}}.Main"),
    // Forked, so the descriptor command's exit code is the task's.
    run / fork := true,
    // The image: the streamlet, the SDK and a Java runtime. It exposes no port: the sidecar in the
    // same pod dials 127.0.0.1:9010.
    dockerBaseImage      := "eclipse-temurin:21-jre",
    Docker / packageName := "{{name}}",
    dockerUpdateLatest   := true,
    descriptor := Def.taskDyn {
      val path = (baseDirectory.value / "flow" / "descriptor.json").getAbsolutePath
      (Compile / runMain).toTask(s" com.thinkmorestupidless.ankka.flow.sdk.Descriptor {{package}}.{{class}} $path")
    }.value,
    descriptorCheck := Def.taskDyn {
      val path = (baseDirectory.value / "flow" / "descriptor.json").getAbsolutePath
      (Compile / runMain).toTask(s" com.thinkmorestupidless.ankka.flow.sdk.Descriptor {{package}}.{{class}} $path --check")
    }.value
  )
