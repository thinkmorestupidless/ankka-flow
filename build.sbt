import Dependencies.*
import com.typesafe.sbt.packager.docker.DockerPlugin
import com.typesafe.sbt.packager.archetypes.JavaAppPackaging

ThisBuild / scalaVersion := V.scala
ThisBuild / organization := "com.thinkmorestupidless"
// No `ThisBuild / version`: sbt-dynver derives it from the nearest tag, as in ankka.
ThisBuild / versionScheme := Some("early-semver")
ThisBuild / homepage      := Some(url("https://github.com/thinkmorestupidless/ankka-flow"))
ThisBuild / licenses := List("Apache-2.0" -> url("https://www.apache.org/licenses/LICENSE-2.0"))
ThisBuild / developers := List(
  Developer(
    "thinkmorestupidless",
    "Trevor Burton-McCreadie",
    "",
    url("https://github.com/thinkmorestupidless")
  )
)

/**
 * One test suite at a time. The Kafka and k3s suites each start their own containers and contend
 * when they overlap; ankka measured 147s in parallel against 6s alone. Do not "optimise" this.
 */
Global / concurrentRestrictions += Tags.limit(Tags.Test, 1)

/** Image settings for the two applications that run in a cluster: the sidecar and the operator. */
lazy val dockerSettings = Seq(
  dockerBaseImage    := "eclipse-temurin:21-jre",
  dockerUpdateLatest := true,
  dockerRepository   := sys.env.get("DOCKER_REPOSITORY"),
  dockerLabels ++= Map(
    "org.opencontainers.image.source"      -> "https://github.com/thinkmorestupidless/ankka-flow",
    "org.opencontainers.image.licenses"    -> "Apache-2.0",
    "org.opencontainers.image.title"       -> (Docker / packageName).value,
    "org.opencontainers.image.description" -> s"${(Docker / packageName).value}, part of ankka-flow"
  ),
  // A Docker tag may not contain '+', and a dynver snapshot version does.
  Docker / version := version.value.replace('+', '-')
)

/**
 * Applications are never published; generating their API docs only adds warnings from generated
 * code.
 */
lazy val noDocs =
  Seq(Compile / doc / sources := Seq.empty, Compile / packageDoc / publishArtifact := false)

/** Test switches. Tests fork, and a forked JVM does not inherit sbt's own -D properties. */
lazy val forwardedTestSwitches = Seq(
  "flow.cluster.tests",
  "flow.conformance.target",
  "flow.conformance.only",
  "flow.mutation",
  "flow.fixtures.regenerate",
  "flow.benchmarks"
)

lazy val commonSettings = Seq(
  scalacOptions ++= Seq(
    "-deprecation",
    "-feature",
    "-unchecked",
    "-encoding",
    "UTF-8",
    "-Wunused:all",
    "-Wvalue-discard",
    "-source:3.7"
  ),
  javacOptions ++= Seq("--release", "21"),
  libraryDependencies ++= commonTest,
  Test / fork              := true,
  Test / parallelExecution := false,
  Test / javaOptions ++= Seq("-XX:+EnableDynamicAgentLoading"),
  Test / javaOptions ++= forwardedTestSwitches.flatMap(k => sys.props.get(k).map(v => s"-D$k=$v")),
  // The one place the Kafka image is written for code: no test names an image by a literal tag.
  Test / javaOptions += s"-Dflow.kafka.image=${V.kafkaImage}",
  // Suites resolve protocol/fixtures and samples/ relative to the repository root.
  Test / javaOptions += s"-Dflow.repo.root=${(LocalRootProject / baseDirectory).value.getAbsolutePath}",
  testFrameworks += new TestFramework("munit.Framework")
)

/**
 * The artefact every SDK copies: .proto files, DESCRIPTOR.md, fixtures, README. The Scala side is
 * the generated messages plus the descriptor's canonical JSON, validation, fingerprints and the
 * version rule. Depends on nothing of ankka-flow's.
 */
lazy val protocol = project
  .in(file("protocol"))
  .settings(commonSettings)
  .settings(
    name := "ankka-flow-protocol",
    Compile / PB.targets := Seq(
      scalapb.gen(grpc = true) -> (Compile / sourceManaged).value / "scalapb"
    ),
    // Generated code warns on every `_` wildcard under -source:3.7 and on -Wunused; keep the
    // hand-written sources strict and silence only what lands in sourceManaged.
    scalacOptions += s"-Wconf:src=${(Compile / sourceManaged).value.getAbsolutePath}/.*:s",
    scalacOptions += "-Wconf:src=.*/src_managed/.*:s",
    libraryDependencies ++= Seq(
      scalapbRuntime % "protobuf",
      scalapbRuntime,
      scalapbRuntimeGrpc,
      grpcStub,
      grpcNettyShaded,
      typesafeConfig
    )
  )

/** Blueprint verification, carried from cloudflow-blueprint. Pure: no Kafka, no cluster. */
lazy val blueprint = project
  .in(file("blueprint"))
  .dependsOn(protocol)
  .settings(commonSettings)
  .settings(
    name := "ankka-flow-blueprint",
    libraryDependencies ++= Seq(typesafeConfig)
  )

/** The AnkkaFlow resource model. fabric8 and Jackson only. */
lazy val crd = project
  .in(file("crd"))
  .settings(commonSettings)
  .settings(
    name := "ankka-flow-crd",
    libraryDependencies ++= Seq(fabric8, jacksonScala, jacksonYaml)
  )

/** The Prometheus JMX exporter agent, fetched by the build and baked into the sidecar image. */
lazy val JmxAgent = config("jmx-agent").hide

lazy val sidecarBuildInfo = Seq(
  buildInfoKeys    := Seq[BuildInfoKey](version),
  buildInfoPackage := "com.thinkmorestupidless.ankka.flow.sidecar"
)

/** The sidecar: owns Kafka for one streamlet and speaks the protocol to its process. */
lazy val sidecar = project
  .in(file("sidecar"))
  .dependsOn(protocol)
  .enablePlugins(JavaAppPackaging, DockerPlugin, BuildInfoPlugin)
  .settings(commonSettings)
  .settings(dockerSettings)
  .settings(sidecarBuildInfo)
  .settings(noDocs)
  .settings(
    name                := "ankka-flow-sidecar",
    publish / skip      := true,
    Compile / mainClass := Some("com.thinkmorestupidless.ankka.flow.sidecar.Main"),
    dockerExposedPorts  := Seq(2050),
    ivyConfigurations += JmxAgent,
    // src/universal/agent/prometheus.yaml arrives by convention; the agent jar joins it here.
    Universal / mappings ++= {
      val jars = (Compile / update).value.select(configurationFilter(JmxAgent.name))
      jars.map(j => j -> "agent/jmx_prometheus_javaagent.jar")
    },
    // Started by the launch script; the port defaults to 2050 and FLOW_METRICS_PORT overrides it.
    bashScriptExtraDefines += """addJava "-javaagent:${app_home}/../agent/jmx_prometheus_javaagent.jar=${FLOW_METRICS_PORT:-2050}:${app_home}/../agent/prometheus.yaml"""",
    libraryDependencies += "io.prometheus.jmx" % "jmx_prometheus_javaagent" % V.jmxExporter % JmxAgent,
    libraryDependencies ++= Seq(
      pekkoActorTyped,
      pekkoStream,
      pekkoSlf4j,
      pekkoKafka,
      kafkaClients,
      logback,
      pekkoActorTestkit   % Test,
      pekkoStreamTestkit  % Test,
      testcontainersKafka % Test,
      jacksonYaml         % Test
    ),
    dependencyOverrides += kafkaClients
  )

lazy val operatorBuildInfo = Seq(
  buildInfoKeys    := Seq[BuildInfoKey](version),
  buildInfoPackage := "com.thinkmorestupidless.ankka.flow.operator"
)

/** The operator: renders AnkkaFlow resources into topics, Secrets and Deployments. */
lazy val operator = project
  .in(file("operator"))
  .dependsOn(crd, blueprint, protocol, sidecar % "test->compile")
  .enablePlugins(JavaAppPackaging, DockerPlugin, BuildInfoPlugin)
  .settings(commonSettings)
  .settings(dockerSettings)
  .settings(operatorBuildInfo)
  .settings(noDocs)
  .settings(
    name           := "ankka-flow-operator",
    publish / skip := true,
    // FlowClusterSuite deploys the operator, the sidecar and the sample by this build's version tag.
    clusterImages := Def.taskDyn {
      if (sys.props.get("flow.cluster.tests").contains("off")) Def.task(())
      else
        Def.task {
          (Docker / publishLocal).value
          (sidecar / Docker / publishLocal).value
          (LocalRootProject / sampleImage).value
          ()
        }
    }.value,
    Test / test     := (Test / test).dependsOn(clusterImages).value,
    Test / testOnly := (Test / testOnly).dependsOn(clusterImages).evaluated,
    Test / javaOptions += s"-Dflow.image.tag=${(Docker / version).value}",
    Compile / mainClass := Some("com.thinkmorestupidless.ankka.flow.operator.Main"),
    libraryDependencies ++= Seq(
      fabric8,
      jacksonScala,
      kafkaClients,
      logback,
      testcontainersKafka % Test,
      testcontainersK3s   % Test,
      fabric8ServerMock   % Test
    ),
    dependencyOverrides += kafkaClients
  )

lazy val cliBuildInfo = Seq(
  buildInfoKeys    := Seq[BuildInfoKey](version),
  buildInfoPackage := "com.thinkmorestupidless.ankka.flow.cli"
)

/** `flow`: verify, generate, reset, version. */
lazy val cli = project
  .in(file("cli"))
  .dependsOn(blueprint, crd, protocol)
  .enablePlugins(JavaAppPackaging, BuildInfoPlugin)
  .settings(commonSettings)
  .settings(cliBuildInfo)
  .settings(noDocs)
  .settings(
    name                 := "ankka-flow-cli",
    publish / skip       := true,
    Compile / mainClass  := Some("com.thinkmorestupidless.ankka.flow.cli.Main"),
    executableScriptName := "flow",
    libraryDependencies ++= Seq(
      decline,
      fabric8,
      jacksonScala,
      jacksonYaml,
      logback,
      fabric8ServerMock % Test
    )
  )

/**
 * The sample's image, `sample-cart-router:<Docker version>`, built with docker from the repository
 * root. The k3s suite imports it; `-Dflow.cluster.tests=off` skips it, as ankka's sample image.
 */
lazy val sampleImage = taskKey[String]("Builds the sample cart router's image and returns its tag")

lazy val clusterImages =
  taskKey[Unit]("Builds every image the k3s suite deploys, unless -Dflow.cluster.tests=off")

lazy val root = project
  .in(file("."))
  .aggregate(protocol, blueprint, crd, sidecar, operator, cli)
  .settings(
    name           := "ankka-flow",
    publish / skip := true,
    commands += mutationCheck,
    sampleImage := {
      val tag  = s"sample-cart-router:${(sidecar / Docker / version).value}"
      val log  = streams.value.log
      val root = (LocalRootProject / baseDirectory).value
      val code = scala.sys.process
        .Process(
          Seq(
            "docker",
            "build",
            "-q",
            "-f",
            "samples/cart-router/Dockerfile",
            "-t",
            tag,
            "-t",
            "sample-cart-router:latest",
            "."
          ),
          root
        )
        .!(scala.sys.process.ProcessLogger(line => log.info(line), line => log.warn(line)))
      if (code != 0) throw new MessageOnlyException(s"docker build of $tag failed ($code)")
      tag
    }
  )

addCommandAlias(
  "buildAll",
  "; scalafmtCheckAll ; scalafmtSbtCheck ; compile ; test ; docker:publishLocal"
)

/**
 * SC-006: the commit-after-write suite must FAIL when the commit is moved ahead of the write. This
 * runs it with `-Dflow.mutation=commit-first` and fails the build if the suite still passes, which
 * would mean the suite no longer guards the ordering it exists to guard.
 */
lazy val mutationCheck = Command.command("mutationCheck") { state =>
  val mutated = Project
    .extract(state)
    .appendWithSession(
      Seq(sidecar / Test / javaOptions += "-Dflow.mutation=commit-first"),
      state
    )
  val outcome = scala.util.Try(
    Project
      .extract(mutated)
      .runInputTask(sidecar / Test / testOnly, " *SinkCommittingAfterKafkaSuite", mutated)
  )
  if (outcome.isSuccess)
    throw new MessageOnlyException(
      "mutation survived: SinkCommittingAfterKafkaSuite passed with the commit moved ahead of the write"
    )
  state.log.info(
    "mutation killed: SinkCommittingAfterKafkaSuite fails with the commit moved ahead of the write, as it must"
  )
  state
}
