package com.thinkmorestupidless.ankka.flow.operator

import java.util.Properties

import scala.concurrent.duration.*

import org.apache.kafka.clients.admin.{Admin, AdminClientConfig, NewTopic}
import org.apache.kafka.common.acl.*
import org.apache.kafka.common.resource.{PatternType, ResourcePattern, ResourceType}
import org.testcontainers.kafka.KafkaContainer
import org.testcontainers.utility.DockerImageName

/**
 * What a machine may do with a topic, as the broker answers it (V8): a Kafka with the standard
 * authorizer and SASL/PLAIN users standing in for ankka's machines. `admin` is the super user;
 * `granted` may read and write `checkout.cart-checkouts`; `reader` may only read it; `ungranted`
 * holds nothing.
 */
class AnkkaClusterKafkaSuite extends munit.FunSuite:

  private val Topic = "checkout.cart-checkouts"

  private val kafka =
    new KafkaContainer(DockerImageName.parse(sys.props("flow.kafka.image")))
      // The listener clients use authenticates; the broker's and controller's own do not, and
      // they act as ANONYMOUS, which is a super user here.
      .withEnv(
        "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP",
        "BROKER:PLAINTEXT,PLAINTEXT:SASL_PLAINTEXT,CONTROLLER:PLAINTEXT"
      )
      .withEnv("KAFKA_SASL_ENABLED_MECHANISMS", "PLAIN")
      .withEnv("KAFKA_LISTENER_NAME_PLAINTEXT_SASL_ENABLED_MECHANISMS", "PLAIN")
      .withEnv(
        "KAFKA_LISTENER_NAME_PLAINTEXT_PLAIN_SASL_JAAS_CONFIG",
        "org.apache.kafka.common.security.plain.PlainLoginModule required " +
          """user_admin="admin-secret" user_granted="granted-secret" """ +
          """user_reader="reader-secret" user_ungranted="ungranted-secret";"""
      )
      .withEnv(
        "KAFKA_AUTHORIZER_CLASS_NAME",
        "org.apache.kafka.metadata.authorizer.StandardAuthorizer"
      )
      .withEnv("KAFKA_SUPER_USERS", "User:admin;User:ANONYMOUS")
      .withEnv("KAFKA_ALLOW_EVERYONE_IF_NO_ACL_FOUND", "false")

  private val executor = new KafkaExecutor

  private def sasl(user: String, password: String): Map[String, String] = Map(
    "security.protocol" -> "SASL_PLAINTEXT",
    "sasl.mechanism"    -> "PLAIN",
    "sasl.jaas.config" ->
      s"""org.apache.kafka.common.security.plain.PlainLoginModule required username="$user" password="$password";"""
  )

  private def topic(
      connection: Map[String, String],
      bootstrap: String = kafka.getBootstrapServers
  ) =
    ResolvedTopic(
      "checkouts",
      Topic,
      false,
      bootstrap,
      connection,
      Map.empty,
      Map.empty,
      None,
      None,
      Map.empty,
      Some(Machine("affiliates", "network"))
    )

  private def allow(user: String, op: AclOperation) =
    new AclBinding(
      new ResourcePattern(ResourceType.TOPIC, Topic, PatternType.LITERAL),
      new AccessControlEntry(s"User:$user", "*", op, AclPermissionType.ALLOW)
    )

  private val Both = Set(Grant.Right.Read, Grant.Right.Write)

  override def beforeAll(): Unit =
    kafka.start()
    val p = new Properties()
    p.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers)
    sasl("admin", "admin-secret").foreach((k, v) => p.put(k, v))
    val admin = Admin.create(p)
    try
      admin.createTopics(java.util.List.of(new NewTopic(Topic, 3, 1.toShort))).all().get()
      admin
        .createAcls(
          java.util.List.of(
            allow("granted", AclOperation.READ),
            allow("granted", AclOperation.WRITE),
            allow("reader", AclOperation.READ)
          )
        )
        .all()
        .get()
    finally admin.close()
    // The broker's authorizer applies ACLs from the metadata log asynchronously: until it has, a
    // granted user is refused like any other. Every case starts once the grant is enforced.
    val deadline = System.nanoTime + 60.seconds.toNanos
    while executor.grants(topic(sasl("granted", "granted-secret")), Both) != Grant.Granted do
      assert(System.nanoTime < deadline, "the broker never enforced the ACLs it was given")
      Thread.sleep(250)

  override def afterAll(): Unit =
    executor.close()
    kafka.stop()

  test("a machine with both rights is granted") {
    assertEquals(executor.grants(topic(sasl("granted", "granted-secret")), Both), Grant.Granted)
  }

  test("a machine that may only read lacks Write, and is granted for an inlet alone") {
    val t = topic(sasl("reader", "reader-secret"))
    assertEquals(executor.grants(t, Both), Grant.Lacks(Grant.Right.Write))
    assertEquals(executor.grants(t, Set(Grant.Right.Read)), Grant.Granted)
  }

  test("a machine with no grant at all lacks Read") {
    assertEquals(
      executor.grants(topic(sasl("ungranted", "ungranted-secret")), Both),
      Grant.Lacks(Grant.Right.Read)
    )
    assertEquals(
      executor.grants(topic(sasl("ungranted", "ungranted-secret")), Set(Grant.Right.Write)),
      Grant.Lacks(Grant.Right.Write)
    )
  }

  test("a wrong password is a refused credential") {
    assert(
      executor.grants(topic(sasl("granted", "wrong")), Both).isInstanceOf[Grant.CredentialRefused],
      executor.grants(topic(sasl("granted", "wrong")), Both).toString
    )
  }

  test("a broker that cannot be reached is unknown, not a refusal") {
    assert(
      executor.grants(topic(Map.empty, "127.0.0.1:1"), Both).isInstanceOf[Grant.Unknown]
    )
  }
