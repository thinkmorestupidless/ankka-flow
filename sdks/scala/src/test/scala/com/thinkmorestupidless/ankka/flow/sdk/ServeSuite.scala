package com.thinkmorestupidless.ankka.flow.sdk

import java.net.InetSocketAddress
import java.util.concurrent.{CountDownLatch, LinkedBlockingQueue, TimeUnit}

import scala.concurrent.Promise

import ankka.flow.v1.discovery.{DiscoveryGrpc, SidecarInfo}
import ankka.flow.v1.payload.Record as RecordProto
import ankka.flow.v1.streamlet.{Batch as BatchProto, Emit as _, *}
import com.google.protobuf.ByteString
import com.thinkmorestupidless.ankka.flow.sdk.conformance.Conformance
import io.grpc.ManagedChannel
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder
import io.grpc.stub.StreamObserver

/** What the server does that the conformance suite does not single out. */
class ServeSuite extends munit.FunSuite:

  private final class Run(channel: ManagedChannel):
    val received  = new LinkedBlockingQueue[FromProcess]()
    val completed = Promise[Unit]()
    val in: StreamObserver[ToProcess] = StreamletGrpc
      .stub(channel)
      .run(new StreamObserver[FromProcess]:
        def onNext(m: FromProcess): Unit = received.add(m): Unit
        def onError(t: Throwable): Unit  = completed.tryFailure(t): Unit
        def onCompleted(): Unit          = completed.trySuccess(()): Unit)

    def start(config: String = ""): Unit =
      in.onNext(
        ToProcess(ToProcess.Message.Start(Start(conversationId = "c", configJson = config)))
      )
    def batch(id: Long, key: String, partition: Int = 0, inlet: String = "in"): Unit =
      val rec = InputRecord(
        0,
        0,
        Some(RecordProto(Some(ByteString.copyFromUtf8(key)), Nil, ByteString.copyFromUtf8("v")))
      )
      in.onNext(ToProcess(ToProcess.Message.Batch(BatchProto(id, inlet, partition, Seq(rec)))))
    def stop(): Unit = in.onNext(ToProcess(ToProcess.Message.Stop(Stop("done"))))
    def next(): FromProcess =
      Option(received.poll(10, TimeUnit.SECONDS)).getOrElse(fail("nothing received"))
    def isCompleted(within: Long = 5000): Boolean =
      val deadline = System.currentTimeMillis + within
      while !completed.isCompleted && System.currentTimeMillis < deadline do Thread.sleep(20)
      completed.isCompleted

  private def withServer(streamlet: Streamlet)(f: (Server, ManagedChannel) => Unit): Unit =
    val server  = Serve.start(streamlet)
    val channel = NettyChannelBuilder.forAddress("127.0.0.1", server.port).usePlaintext().build()
    try f(server, channel)
    finally
      channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS): Unit
      server.close()

  test("discovery answers the streamlet's descriptor") {
    val streamlet = new Conformance
    withServer(streamlet) { (_, channel) =>
      val spec = DiscoveryGrpc.blockingStub(channel).discover(SidecarInfo("1.0", "test"))
      assertEquals(spec, Descriptor.spec(streamlet))
      assertEquals(spec.getSdk.name, "ankka-flow-scala")
    }
  }

  test("the server binds the loopback address only") {
    withServer(new Conformance) { (server, _) =>
      assert(server.addresses.nonEmpty)
      server.addresses.foreach {
        case a: InetSocketAddress => assert(a.getAddress.isLoopbackAddress, a.toString)
        case other                => fail(s"not an inet address: $other")
      }
    }
  }

  test("the port is FLOW_PROCESS_PORT, else 9010") {
    assertEquals(Serve.port(Map.empty), 9010)
    assertEquals(Serve.port(Map("FLOW_PROCESS_PORT" -> "9123")), 9123)
  }

  test("batches of two partitions run at once") {
    val both = new CountDownLatch(2)
    val streamlet = new FixtureStreamlets.NoOp("together"):
      inlet("in", schemaName = "x.v1")
      val out = outlet("out", schemaName = "x.v1")
      override def process(batch: Batch): Iterable[Emit] =
        both.countDown()
        if !both.await(5, TimeUnit.SECONDS) then
          throw new RuntimeException("the other partition never ran")
        batch.records.map(r => out.emit(r))
    withServer(streamlet) { (_, channel) =>
      val run = new Run(channel)
      run.start()
      run.batch(1, "a", partition = 0)
      run.batch(2, "b", partition = 1)
      val acks = Vector.fill(4)(run.next()).collect {
        case FromProcess(FromProcess.Message.Ack(a), _) => a.batchId
      }
      assertEquals(acks.toSet, Set(1L, 2L))
    }
  }

  test("a start's configuration reaches the streamlet") {
    withServer(new Conformance) { (_, channel) =>
      val run = new Run(channel)
      run.start("""{"factor": 3, "mode": "echo"}""")
      run.batch(1, "multiply")
      val emits = Vector.fill(4)(run.next()).count(_.message.isEmit)
      assertEquals(emits, 3)
    }
  }

  test("stop completes the stream after the batches in flight") {
    withServer(new Conformance) { (_, channel) =>
      val run = new Run(channel)
      run.start()
      run.batch(1, "late")
      run.stop()
      assert(run.next().message.isEmit)
      assert(run.next().message.isAck)
      assert(run.isCompleted())
    }
  }

  test("a new conversation ends the previous one, which sends nothing more") {
    withServer(new Conformance) { (_, channel) =>
      val first = new Run(channel)
      first.start()
      first.batch(1, "echo")
      assert(first.next().message.isEmit)
      assert(first.next().message.isAck) // the server holds the first conversation
      first.batch(2, "late")
      val second = new Run(channel)
      second.start()
      assert(first.isCompleted())
      Thread.sleep(500)
      assert(first.received.isEmpty, s"the superseded conversation sent ${first.received}")
      second.batch(1, "echo")
      assert(second.next().message.isEmit)
      assert(second.next().message.isAck)
    }
  }

  test("a batch before start ends the conversation") {
    withServer(new Conformance) { (_, channel) =>
      val run = new Run(channel)
      run.batch(1, "echo")
      assert(run.isCompleted())
      assert(run.received.isEmpty)
    }
  }
