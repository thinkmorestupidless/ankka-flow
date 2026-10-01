package com.thinkmorestupidless.ankka.flow.sidecar

import java.nio.file.Paths
import java.time.Duration as JDuration
import java.util.concurrent.atomic.AtomicReference

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*
import scala.util.{Failure, Success, Try}

import com.thinkmorestupidless.ankka.flow.protocol.{Builtins, DescriptorValidation}
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.pattern.after
import org.neo4j.driver.{
  AuthTokens,
  Config,
  Driver,
  GraphDatabase,
  SessionConfig,
  TransactionConfig
}
import org.neo4j.driver.exceptions.ClientException
import org.slf4j.LoggerFactory

/**
 * The graph merge sink (feature 002, contracts/neo4j-merge-sink.md): the first stage built into the
 * sidecar. Each batch of graph deltas becomes one Neo4j write transaction; the batch completes, and
 * the sidecar commits its offsets, only once that transaction has committed.
 *
 * Every statement is version-guarded and state-shaped, so a batch applied twice — after a restart,
 * a rebalance or a reset to the start of the topic — leaves the graph as applying it once did.
 */
final class Neo4jMergeStage(
    deployed: Descriptor,
    config: StreamletConfig,
    events: EventSink,
    record: Neo4jMergeStage.Record = Neo4jMergeStage.Record.None
)(using system: ActorSystem)
    extends Stage
    with BatchProcessor:

  import Neo4jMergeStage.*

  private val log = LoggerFactory.getLogger(classOf[Neo4jMergeStage])

  /** The driver's API blocks; a batch waits on the blocking dispatcher, never the default one. */
  private given ExecutionContext =
    system.dispatchers.lookup("pekko.actor.default-blocking-io-dispatcher")

  private val name     = s"${config.pipeline}.${config.streamlet}"
  private val timeout  = transactionTimeout(config)
  private val credsDir = config.stage.flatMap(_.neo4j).map(_.credentialsDir).getOrElse("")

  @volatile private var secret: Option[Neo4jSecret] = None
  @volatile private var driver: Option[Driver]      = None
  private val current                               = new AtomicReference[Option[RunState]](None)

  private def redact(message: String): String =
    secret.fold(Option(message).getOrElse(""))(_.redact(message))

  // ── opening ──────────────────────────────────────────────────────────────────────────────────

  def open(stopped: () => Boolean): Option[Either[Stage.Refusal, Stage.Run]] =
    readSecret() match
      case Left(problem) =>
        log.error("refusing to start: {}", problem)
        Some(Left(Stage.Refusal(Vector(problem), 2)))
      case Right(_) =>
        descriptorProblems match
          case problems if problems.nonEmpty =>
            log.error("refusing to start: the deployed descriptor is not this sidecar's built-in")
            problems.foreach(p => log.error("  {}", p))
            Some(Left(Stage.Refusal(problems, 1)))
          case _ => connect(stopped).map(Right(_))

  /**
   * The credentials, read again on every attempt: a mounted Secret's files change in place when the
   * Secret does, so a corrected or rotated password is used without a restart.
   */
  private def readSecret(): Either[String, Neo4jSecret] =
    Neo4jSecret.read(Paths.get(credsDir)).map { s =>
      if !secret.contains(s) then
        discardDriver()
        secret = Some(s)
      s
    }

  /** Every way the deployed descriptor differs from the one this sidecar ships. */
  private def descriptorProblems: Vector[String] =
    val builtin = Builtins.neo4jMergeSink.getStreamlet
    DescriptorValidation
      .compare(deployed.streamlet, builtin)
      .map(
        _.replace("the process does not declare", "this sidecar's built-in does not declare")
          .replace("process declares", "this sidecar's built-in declares")
      )

  /** Until the database answers, is new enough, and has (or cannot be given) its constraint. */
  private def connect(stopped: () => Boolean): Option[Stage.Run] =
    var backoff                   = 500.millis
    var opened: Option[Stage.Run] = None
    while opened.isEmpty && !stopped() do
      readSecret().left.map(new IllegalStateException(_)).toTry.flatMap(attempt) match
        case Success(run) => opened = Some(run)
        case Failure(e) =>
          log.warn(
            "cannot open Neo4j at {}: {}; retrying in {}",
            secret.fold("?")(_.uri),
            redact(e.getMessage),
            backoff
          )
          Try(Thread.sleep(backoff.toMillis))
          backoff = (backoff * 2).min(10.seconds)
    opened

  private def attempt(s: Neo4jSecret): Try[Stage.Run] = Try {
    val d = driver.getOrElse {
      val created =
        GraphDatabase.driver(s.uri, AuthTokens.basic(s.username, s.password), driverConfig)
      driver = Some(created)
      created
    }
    try
      d.verifyConnectivity()
      val agent = serverAgent(d, s)
      if !supported(agent) then
        throw new IllegalStateException(
          s"$agent at ${s.uri} is older than Neo4j 5.26, which the merge needs (dynamic labels)"
        )
      ensureConstraint(d, s)
    catch
      case e: Throwable =>
        // A driver that could not connect keeps its pool; start from a clean one next time.
        discardDriver()
        throw e
    val state = new RunState
    current.set(Some(state))
    log.info("stage 'neo4j-merge-sink' for {} open against {} ({})", name, s.uri, s.database)
    state
  }

  /** The server's agent string (`Neo4j/5.26.1`), from a query's summary. */
  private def serverAgent(d: Driver, s: Neo4jSecret): String =
    val session = d.session(SessionConfig.forDatabase(s.database))
    try session.run("RETURN 1").consume().server().agent()
    finally session.close()

  private def ensureConstraint(d: Driver, s: Neo4jSecret): Unit =
    val session = d.session(SessionConfig.forDatabase(s.database))
    try session.run(Constraint).consume(): Unit
    catch
      case e: ClientException if e.code.startsWith("Neo.ClientError.Security") =>
        val note =
          s"$name: could not create constraint element_id: ${redact(e.getMessage)}; merges will scan until it exists"
        log.warn(note)
        events.warning("ConstraintNotCreated", note)
    finally session.close()

  // ── processing ───────────────────────────────────────────────────────────────────────────────

  def process(batch: InputBatch): Future[Outcome] =
    current.get match
      case Some(state) if state.failed.isCompleted =>
        state.failed.flatMap(Future.failed)
      case None => Future.failed(new StreamFailed("the stage is not open"))
      case Some(_) =>
        val reads =
          batch.records.map(r => Deltas.read(r.offset, r.getRecord.key, r.getRecord.value))
        reads.collectFirst { case Left(problem) => problem } match
          case Some(problem) =>
            record.failed(batch.inlet, batch.partition)
            Future.failed(failure(batch, problem))
          case None =>
            // Delete markers carry nothing to apply; a batch of them alone runs no transaction.
            val markers = reads.count(_ == Right(Deltas.Read.Marker))
            val folded  = Deltas.fold(reads.collect { case Right(Deltas.Read.Applied(d)) => d })
            // The transaction timeout is enforced by the server; a server that stops answering
            // enforces nothing, so the batch also has a deadline of its own.
            val deadline = after(timeout + DeadlineMargin)(
              Future.failed(
                new StreamFailed(
                  s"the transaction did not complete within ${timeout + DeadlineMargin}"
                )
              )
            )(using system)
            Future.firstCompletedOf(Seq(Future(write(folded)), deadline)).transform {
              case Success(written) =>
                record.applied(
                  batch.inlet,
                  batch.partition,
                  written,
                  batch.records.size - markers - written,
                  markers
                )
                Success(Outcome.Acked(Vector.empty))
              case Failure(e) =>
                record.failed(batch.inlet, batch.partition)
                // A connection that hung may still hold the transaction; never reuse it.
                discardDriver()
                Failure(failure(batch, redact(e.getMessage), e))
            }

  private def failure(batch: InputBatch, reason: String, cause: Throwable = null) =
    new StreamFailed(
      s"neo4j merge failed for inlet '${batch.inlet}' partition ${batch.partition}: $reason",
      cause
    )

  /** One transaction for the whole batch; the number of deltas it applied. */
  private def write(folded: Deltas.Folded): Int =
    if folded.size == 0 then 0
    else
      val d       = driver.getOrElse(throw new StreamFailed("the stage has no driver"))
      val s       = secret.get
      val session = d.session(SessionConfig.forDatabase(s.database))
      try
        session.executeWrite(
          tx =>
            def run(statement: String, key: String, rows: java.util.List[?]): Int =
              if rows.isEmpty then 0
              else
                tx.run(statement, Map[String, AnyRef](key -> rows).asJava)
                  .single()
                  .get("written")
                  .asInt()
            run(Statements.Nodes, "nodes", Deltas.nodeRows(folded.nodes)) +
              run(Statements.Edges, "edges", Deltas.edgeRows(folded.edges)) +
              run(
                Statements.NodeTombstones,
                "nodeTombstones",
                Deltas.nodeTombstoneRows(folded.nodeTombstones)
              ) +
              run(
                Statements.EdgeTombstones,
                "edgeTombstones",
                Deltas.edgeTombstoneRows(folded.edgeTombstones)
              )
          ,
          TransactionConfig.builder().withTimeout(JDuration.ofMillis(timeout.toMillis)).build()
        )
      finally session.close()

  /**
   * A write that finishes after its partition was revoked is not committed by the graph
   * (`InletGraph` turns it into `None`), and the next owner reads the batch again and finds it
   * applied, so there is nothing to cancel here.
   */
  def revoke(inlet: String, partition: Int, generation: Long): Unit = ()

  def close(): Unit = discardDriver()

  /** Closes the driver without waiting on a connection that may never answer. */
  private def discardDriver(): Unit = synchronized {
    driver.foreach(d => Try(d.closeAsync()))
    driver = None
  }

  private def driverConfig: Config =
    Config
      .builder()
      .withConnectionTimeout(10, java.util.concurrent.TimeUnit.SECONDS)
      .withConnectionAcquisitionTimeout(
        timeout.toMillis,
        java.util.concurrent.TimeUnit.MILLISECONDS
      )
      .withMaxTransactionRetryTime(timeout.toMillis, java.util.concurrent.TimeUnit.MILLISECONDS)
      .build()

  /** One opened run: fails when the supervisor tears it down, stops when it stops. */
  private final class RunState extends Stage.Run:
    private val failure              = Promise[Throwable]()
    def processor: BatchProcessor    = Neo4jMergeStage.this
    def failed: Future[Throwable]    = failure.future
    def fail(cause: Throwable): Unit = failure.trySuccess(cause): Unit
    def stop(reason: String): Unit   = failure.trySuccess(new StreamFailed(reason)): Unit

object Neo4jMergeStage:

  /** How long past the transaction timeout a batch waits before giving up on the server. */
  val DeadlineMargin: FiniteDuration = 5.seconds

  val Constraint =
    "CREATE CONSTRAINT element_id IF NOT EXISTS FOR (n:Element) REQUIRE n.id IS UNIQUE"

  /** What the stage reports per batch; the metrics beans implement it (StageMetrics). */
  trait Record:
    def applied(inlet: String, partition: Int, written: Int, stale: Int, markers: Int): Unit
    def failed(inlet: String, partition: Int): Unit

  object Record:
    val None: Record = new Record:
      def applied(inlet: String, partition: Int, written: Int, stale: Int, markers: Int): Unit =
        ()
      def failed(inlet: String, partition: Int): Unit = ()

  /** `transaction-timeout` from the resolved parameters, else the descriptor's default. */
  def transactionTimeout(config: StreamletConfig): FiniteDuration =
    Try(config.config.getDuration("transaction-timeout").toMillis.millis).getOrElse(30.seconds)

  /** `Neo4j/5.26.1`, `Neo4j/2025.01.0`: 5.26 or later. */
  def supported(agent: String): Boolean =
    val Version = """Neo4j/(\d+)\.(\d+).*""".r
    agent match
      case Version(major, minor) => major.toInt > 5 || (major.toInt == 5 && minor.toInt >= 26)
      case _                     => false

  /** The four statements of contracts/neo4j-merge-sink.md, verbatim. */
  object Statements:
    val Nodes: String =
      """UNWIND $nodes AS d
        |MERGE (n:Element {id: d.id})
        |  ON CREATE SET n._version = -1
        |WITH n, d WHERE n._version < d.version
        |REMOVE n:$([l IN labels(n) WHERE l <> 'Element'])
        |SET n = d.properties, n.id = d.id, n._version = d.version
        |SET n:$(d.labels)
        |RETURN count(n) AS written""".stripMargin

    val Edges: String =
      """UNWIND $edges AS d
        |MERGE (a:Element {id: d.from}) ON CREATE SET a._version = -1
        |MERGE (b:Element {id: d.to})   ON CREATE SET b._version = -1
        |MERGE (a)-[r:$(d.type) {id: d.id}]->(b)
        |  ON CREATE SET r._version = -1
        |WITH r, d WHERE r._version < d.version
        |SET r = d.properties, r.id = d.id, r._version = d.version
        |RETURN count(r) AS written""".stripMargin

    val NodeTombstones: String =
      """UNWIND $nodeTombstones AS d
        |MERGE (n:Element {id: d.id})
        |  ON CREATE SET n._version = -1
        |WITH n, d WHERE n._version < d.version
        |SET n._version = d.version, n._deleted = true
        |RETURN count(n) AS written""".stripMargin

    val EdgeTombstones: String =
      """UNWIND $edgeTombstones AS d
        |MERGE (a:Element {id: d.from}) ON CREATE SET a._version = -1
        |MERGE (b:Element {id: d.to})   ON CREATE SET b._version = -1
        |MERGE (a)-[r:$(d.type) {id: d.id}]->(b)
        |  ON CREATE SET r._version = -1
        |WITH r, d WHERE r._version < d.version
        |SET r._version = d.version, r._deleted = true
        |RETURN count(r) AS written""".stripMargin
