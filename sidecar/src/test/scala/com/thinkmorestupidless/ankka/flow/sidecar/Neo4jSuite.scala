package com.thinkmorestupidless.ankka.flow.sidecar

import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

import org.neo4j.driver.{AuthTokens, Driver, GraphDatabase}
import org.testcontainers.containers.Neo4jContainer
import org.testcontainers.utility.DockerImageName

/**
 * A real Neo4j per suite (the image `-Dflow.neo4j.image` names), and the plain-driver helpers the
 * suites use to read what the stage wrote without trusting the stage.
 *
 * Mixes in after `KafkaSuite` (`extends KafkaSuite with Neo4jSuite`): its `beforeAll` and
 * `afterAll` call `super`, so both containers start and stop.
 */
trait Neo4jSuite extends munit.FunSuite:

  val AdminPassword = "flow-test-password"

  val neo4j: Neo4jContainer[?] =
    new Neo4jContainer(
      DockerImageName
        .parse(sys.props.getOrElse("flow.neo4j.image", "neo4j:5.26-community"))
        .asCompatibleSubstituteFor("neo4j")
    ).withAdminPassword(AdminPassword)

  private var open: Option[Driver] = None

  override val munitTimeout: FiniteDuration = 3.minutes

  def boltUri: String = neo4j.getBoltUrl

  /** An admin driver on the test side, never the stage's. */
  def driver: Driver =
    open.getOrElse {
      val d = GraphDatabase.driver(boltUri, AuthTokens.basic("neo4j", AdminPassword))
      open = Some(d)
      d
    }

  override def beforeAll(): Unit =
    super.beforeAll()
    neo4j.start()

  override def afterAll(): Unit =
    open.foreach(_.close())
    neo4j.stop()
    super.afterAll()

  /** Every row the query returns, as maps of plain Java values. */
  def query(cypher: String, params: Map[String, AnyRef] = Map.empty): Vector[Map[String, AnyRef]] =
    val session = driver.session()
    try
      session
        .run(cypher, params.asJava)
        .list()
        .asScala
        .toVector
        .map(_.asMap().asScala.toMap)
    finally session.close()

  /** An empty graph with no constraint, for a test that must not see another's writes. */
  def clear(): Unit =
    query("MATCH (n) DETACH DELETE n"): Unit
    query("DROP CONSTRAINT element_id IF EXISTS"): Unit

  /** Freezes the container, as a database that stops answering does; `unpause` resumes it. */
  def pause(): Unit =
    neo4j.getDockerClient.pauseContainerCmd(neo4j.getContainerId).exec(): Unit

  def unpause(): Unit =
    neo4j.getDockerClient.unpauseContainerCmd(neo4j.getContainerId).exec(): Unit
