package com.thinkmorestupidless.ankka.flow.pekko

import java.nio.file.{Files, Path}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

import scala.concurrent.Await
import scala.concurrent.duration.*

import com.thinkmorestupidless.ankka.flow.descriptor.{
  Delivery,
  InletSettings,
  OutletSettings,
  Settings
}
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.stream.RestartSettings

/** The runner against a real broker: what it reads, writes and commits, and when. */
class RunnerKafkaSuite extends KafkaSuite:

  /**
   * Upper-cases each value, keeps the key and headers, adds a header; fails on `poison` while
   * asked.
   */
  final class Upper(val failing: AtomicBoolean = new AtomicBoolean(false))
      extends PekkoStreamlet("upper"):
    val in    = inlet("in", Contract("x.v1"), Codec.string)
    val out   = outlet("out", Contract("x.v1"), Codec.string)
    val seen  = new AtomicInteger(0)
    val tries = new AtomicInteger(0)
    def logic(ctx: Context): Logic = from(in) {
      _.map { r =>
        if r.value == "poison" then
          tries.incrementAndGet()
          if failing.get then throw new IllegalStateException("poison")
        seen.incrementAndGet()
        out(Record(r.key, r.value.toUpperCase, r.headers :+ ("upper" -> Array[Byte](1))))
      }
    }

  private val fast = Runner.Options(
    restart = RestartSettings(100.millis, 500.millis, 0.0),
    committer = identity,
    aliveEvery = 200.millis
  )

  private def settings(in: String, out: String, group: String) = Settings(
    pipeline = "p",
    streamlet = "upper",
    namespace = "n",
    local = true,
    parameters = Map.empty,
    inlets = Vector(
      InletSettings("in", in, group, s"$group-client", Map("bootstrap.servers" -> bootstrap))
    ),
    outlets = Vector(
      OutletSettings(
        "out",
        out,
        "p.upper.out",
        Delivery.AtLeastOnce,
        None,
        Map("bootstrap.servers" -> bootstrap)
      )
    )
  )

  private final case class Started(running: Runner.Running, system: ActorSystem[?], probes: Path):
    def stop(): Unit =
      Await.result(running.stop(), 30.seconds)
      system.terminate()
      Await.ready(system.whenTerminated, 30.seconds): Unit

  private def start(
      streamlet: PekkoStreamlet,
      s: Settings,
      options: Runner.Options = fast
  ): Started =
    val system = ActorSystem(Behaviors.empty[Nothing], "runner")
    val dir    = Files.createTempDirectory("probes")
    Started(Runner.start(streamlet, s, system, new Probes(dir), options), system, dir)

  private def values(topic: String, n: Int) = consumeAll(topic, n).map(r => str(r.value))

  test("a Pekko Streams streamlet reads its inlet and writes its outlet") {
    val (in, out) = (createTopic(uniqueTopic("in"), 3), createTopic(uniqueTopic("out"), 3))
    val upper     = new Upper
    val run       = start(upper, settings(in, out, uniqueTopic("g")))
    try
      publish(
        in,
        Seq((Some("k1"), "a", Seq("h" -> Array[Byte](7))), (Some("k2"), "b", Nil), (None, "c", Nil))
      )
      val got = consumeAll(out, 3)
      assertEquals(got.map(r => str(r.value)).toSet, Set("A", "B", "C"))
      val a = got.find(r => str(r.value) == "A").get
      assertEquals(str(a.key), "k1")
      assertEquals(a.headers.toArray.map(_.key).toList, List("h", "upper"))
      assertEquals(a.headers.lastHeader("h").value.toList, List[Byte](7))
      assertEquals(got.find(r => str(r.value) == "C").get.key, null)
    finally run.stop()
  }

  test("commits a record only after what it wrote is confirmed") {
    val (in, out) = (createTopic(uniqueTopic("in"), 1), createTopic(uniqueTopic("out"), 1))
    val group     = uniqueTopic("g")
    // Commits held back, so a kill lands after the writes and before their commit.
    val held  = fast.copy(committer = _.withMaxBatch(100000).withMaxInterval(1.hour))
    val first = start(new Upper, settings(in, out, group), held)
    publish(in, (1 to 5).map(n => (Some("k"), s"v$n", Nil)))
    assertEquals(values(out, 5), (1 to 5).map(n => s"V$n").toVector)
    assertEquals(committed(group), 0L, "nothing committed before the kill")
    // Killed: the system goes with no drain, so nothing it held is committed.
    first.system.terminate()
    Await.ready(first.system.whenTerminated, 30.seconds)

    val second = start(new Upper, settings(in, out, group))
    try
      // Read again from the last commit: every record written twice, none skipped.
      val all = values(out, 10)
      assertEquals(all.size, 10)
      assertEquals(
        all.groupBy(identity).view.mapValues(_.size).toMap,
        (1 to 5).map(n => s"V$n" -> 2).toMap
      )
    finally second.stop()
    assertEquals(committed(group), 5L, "a drained stop commits the last record")
  }

  test(
    "a record whose logic throws is retried, and nothing after it in its partition is committed"
  ) {
    val (in, out) = (createTopic(uniqueTopic("in"), 1), createTopic(uniqueTopic("out"), 1))
    val group     = uniqueTopic("g")
    val upper     = new Upper(new AtomicBoolean(true))
    val run       = start(upper, settings(in, out, group))
    try
      publish(in, Seq("a", "b", "poison", "c", "d").map(v => (Some("k"), v, Nil)))
      eventually()(assert(upper.tries.get >= 3, s"tries ${upper.tries.get}"))
      assert(committed(group) <= 2L, s"committed ${committed(group)}")
      assert(!values(out, 5, 2.seconds).exists(v => v == "C" || v == "D"))
      upper.failing.set(false)
      eventually()(assert(values(out, 20, 2.seconds).toSet == Set("A", "B", "POISON", "C", "D")))
    finally run.stop()
    assertEquals(committed(group), 5L)
  }

  /**
   * Each record written twice, as `v` and `v!`; `big`'s second while asked is past the producer's
   * request size limit, so it fails before it reaches the broker.
   */
  final class Twice(val failing: AtomicBoolean) extends PekkoStreamlet("upper"):
    val in    = inlet("in", Contract("x.v1"), Codec.string)
    val out   = outlet("out", Contract("x.v1"), Codec.string)
    val tries = new AtomicInteger(0)
    def logic(ctx: Context): Logic = from(in) {
      _.mapConcat { r =>
        if r.value == "big" then tries.incrementAndGet(): Unit
        val second =
          if r.value == "big" && failing.get then "x" * (2 * 1024 * 1024) else s"${r.value}!"
        Seq(out(Record(r.key, r.value)), out(Record(r.key, second)))
      }
    }

  test("a record made into two whose second write fails is retried, and its offset not committed") {
    val (in, out) = (createTopic(uniqueTopic("in"), 1), createTopic(uniqueTopic("out"), 1))
    val group     = uniqueTopic("g")
    val twice     = new Twice(new AtomicBoolean(true))
    val run       = start(twice, settings(in, out, group))
    try
      publish(in, Seq("a", "big").map(v => (Some("k"), v, Nil)))
      eventually()(assert(twice.tries.get >= 3, s"tries ${twice.tries.get}"))
      assert(committed(group) <= 1L, s"committed ${committed(group)}")
      twice.failing.set(false)
      eventually()(assert(values(out, 20, 2.seconds).toSet == Set("a", "a!", "big", "big!")))
    finally run.stop()
    assertEquals(committed(group), 2L)
  }

  private def values(topic: String, n: Int, within: FiniteDuration) =
    consumeAll(topic, n, within).map(r => str(r.value))

  test("two runners in one group share the partitions, and each key's order holds") {
    val (in, out) = (createTopic(uniqueTopic("in"), 4), createTopic(uniqueTopic("out"), 4))
    val group     = uniqueTopic("g")
    val (u1, u2)  = (new Upper, new Upper)
    val r1        = start(u1, settings(in, out, group))
    val r2        = start(u2, settings(in, out, group))
    try
      eventually() {
        assert(Files.exists(r1.probes.resolve("ready")) && Files.exists(r2.probes.resolve("ready")))
      }
      // Both members are in before anything is published, so no rebalance replays a record.
      eventually()(
        assertEquals(
          admin(
            _.describeConsumerGroups(java.util.List.of(group)).all().get().get(group).members().size
          ),
          2
        )
      )
      // Enough keys that both members' partitions get some.
      val keys = (0 until 40).map(k => s"k$k")
      publish(in, for n <- 0 until 10; k <- keys yield (Some(k), s"$k-$n", Nil))
      val got = consumeAll(out, 400)
      assertEquals(got.size, 400)
      got.groupBy(r => str(r.key)).foreach { (k, rs) =>
        assertEquals(
          rs.sortBy(_.offset).map(r => str(r.value)),
          (0 until 10).map(n => s"${k.toUpperCase}-$n").toVector,
          k
        )
      }
      val members = admin(
        _.describeConsumerGroups(java.util.List.of(group)).all().get().get(group).members()
      ).toString
      assert(u1.seen.get > 0 && u2.seen.get > 0, s"${u1.seen.get} and ${u2.seen.get}: $members")
    finally
      r1.stop()
      r2.stop()
  }

  test("ready appears once partitions are assigned, alive is touched, and ready goes on stop") {
    val (in, out) = (createTopic(uniqueTopic("in"), 1), createTopic(uniqueTopic("out"), 1))
    val run       = start(new Upper, settings(in, out, uniqueTopic("g")))
    val alive     = run.probes.resolve("alive")
    eventually()(assert(Files.exists(run.probes.resolve("ready"))))
    eventually()(assert(Files.exists(alive)))
    val first = Files.getLastModifiedTime(alive)
    eventually()(assert(Files.getLastModifiedTime(alive).compareTo(first) > 0))
    run.stop()
    assert(!Files.exists(run.probes.resolve("ready")))
  }
