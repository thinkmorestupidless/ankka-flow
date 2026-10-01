import sbt.*

/** Single source of truth for every external version in the build. Kept in step with ankka's. */
object Dependencies {

  object V {
    val scala = "3.9.0"

    val pekko      = "1.7.0"
    val pekkoKafka = "1.2.0"

    /** pekko-connectors-kafka 1.2.0's own line; pinned so every module agrees (research R8). */
    val kafkaClients = "3.9.2"

    val jsoniter       = "2.40.1"
    val logback        = "1.6.3"
    val munit          = "1.3.6"
    val testcontainers = "1.21.4"
    val fabric8        = "7.9.0"
    val decline        = "2.6.2"

    /** Must match the jackson-databind fabric8 resolves (ankka's `V.jackson` note). */
    val jackson = "2.21.4"

    /** The Kafka image every Kafka test, the compose files and the k3s suite run. */
    val kafkaImage = "apache/kafka:3.9.1"

    /** The Prometheus JMX exporter agent baked into the sidecar image (research R7). */
    val jmxExporter = "1.0.1"

    /** The Neo4j Java driver the graph merge sink writes with (feature 002, research R7). */
    val neo4jDriver = "5.28.5"

    /**
     * The Neo4j the graph merge sink is tested against: 5.26 LTS, the first with dynamic labels in
     * MERGE, which the sink's statements need.
     */
    val neo4jImage = "neo4j:5.26-community"
  }

  private def pekko(m: String) = "org.apache.pekko" %% s"pekko-$m" % V.pekko

  val pekkoActorTyped    = pekko("actor-typed")
  val pekkoStream        = pekko("stream")
  val pekkoSlf4j         = pekko("slf4j")
  val pekkoActorTestkit  = pekko("actor-testkit-typed")
  val pekkoStreamTestkit = pekko("stream-testkit")

  val pekkoKafka   = "org.apache.pekko" %% "pekko-connectors-kafka" % V.pekkoKafka
  val kafkaClients = "org.apache.kafka"  % "kafka-clients"          % V.kafkaClients

  /**
   * Typesafe Config at the version Pekko pins, so `blueprint` adds no second copy (R16 item 11).
   */
  val typesafeConfig = "com.typesafe" % "config" % "1.4.9"

  val jsoniterCore = "com.github.plokhotnyuk.jsoniter-scala" %% "jsoniter-scala-core" % V.jsoniter
  val jsoniterMacros =
    "com.github.plokhotnyuk.jsoniter-scala" %% "jsoniter-scala-macros" % V.jsoniter

  val logback             = "ch.qos.logback"     % "logback-classic"   % V.logback
  val munit               = "org.scalameta"     %% "munit"             % V.munit
  val testcontainersKafka = "org.testcontainers" % "kafka"             % V.testcontainers
  val testcontainersK3s   = "org.testcontainers" % "k3s"               % V.testcontainers
  val testcontainersNeo4j = "org.testcontainers" % "neo4j"             % V.testcontainers
  val neo4jDriver         = "org.neo4j.driver"   % "neo4j-java-driver" % V.neo4jDriver

  val fabric8           = "io.fabric8"                       % "kubernetes-client"       % V.fabric8
  val fabric8ServerMock = "io.fabric8"                       % "kubernetes-server-mock"  % V.fabric8
  val decline           = "com.monovore"                    %% "decline"                 % V.decline
  val jacksonScala      = "com.fasterxml.jackson.module"    %% "jackson-module-scala"    % V.jackson
  val jacksonYaml       = "com.fasterxml.jackson.dataformat" % "jackson-dataformat-yaml" % V.jackson

  // grpc-java with ScalaPB, not pekko-grpc (research R1): versions come from the compiler plugin
  // so generated code and runtime can never disagree.
  val scalapbRuntime: ModuleID =
    "com.thesamet.scalapb" %% "scalapb-runtime" % scalapb.compiler.Version.scalapbVersion
  val scalapbRuntimeGrpc: ModuleID =
    "com.thesamet.scalapb" %% "scalapb-runtime-grpc" % scalapb.compiler.Version.scalapbVersion
  val grpcNettyShaded: ModuleID =
    "io.grpc" % "grpc-netty-shaded" % scalapb.compiler.Version.grpcJavaVersion
  val grpcStub: ModuleID = "io.grpc" % "grpc-stub" % scalapb.compiler.Version.grpcJavaVersion

  val commonTest: Seq[ModuleID] = Seq(munit % Test, logback % Test)
}
