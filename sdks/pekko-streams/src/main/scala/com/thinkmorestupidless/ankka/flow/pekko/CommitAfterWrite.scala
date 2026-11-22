/*
 * Copyright (C) 2016-2026 Lightbend Inc. <https://www.lightbend.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.thinkmorestupidless.ankka.flow.pekko

import scala.collection.immutable
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.*

import org.apache.pekko.{Done, NotUsed}
import org.apache.pekko.kafka.{CommitWhen, CommitterSettings}
import org.apache.pekko.kafka.ConsumerMessage.{
  Committable,
  CommittableOffset,
  CommittableOffsetBatch,
  GroupTopicPartition
}
import org.apache.pekko.kafka.scaladsl.Committer
import org.apache.pekko.stream.scaladsl.{Flow, Keep, Sink}

/**
 * Commit-after-write, carried from Cloudflow's `PekkoStreamletLogic.sinkCommittingAfter`
 * (core/cloudflow-pekko/src/main/scala/cloudflow/pekkostream/PekkoStreamletLogic.scala), lifted out
 * of the streamlet logic class into a standalone function and made to materialise the committer's
 * completion so the runner can observe a failure.
 *
 * In the Pekko Streams SDK, "write" is "produce every record the logic emitted for the batch and
 * wait for the broker to confirm each one".
 */
object CommitAfterWrite:

  /**
   * The mutation SC-006 runs: with `-Dflow.mutation=commit-first` the batch's offsets are handed to
   * the committer before its write completes, and a failed write no longer stops them. The
   * commit-after-write suite must FAIL under it; `sbt mutationCheck` asserts that it does.
   */
  private def commitFirst: Boolean = sys.props.get("flow.mutation").contains("commit-first")

  def defaultCommitterSettings(base: CommitterSettings): CommitterSettings =
    base.withCommitWhen(CommitWhen.OffsetFirstObserved)

  /**
   * A sink for writing to a system outside Kafka that commits a record's offset only after the
   * write containing it has succeeded.
   *
   * Elements are grouped into batches of up to `batchSize`, or whatever has arrived within
   * `batchWithin`. Each batch goes to `write`, one at a time: a batch is not written until the one
   * before it has succeeded, so writes happen in the order the elements were read, and never
   * concurrently. When a write succeeds, the batch's offsets are committed. When it fails, the
   * stream fails, and nothing from that batch onwards is committed: the next reader starts from the
   * last committed offset, so every element is written at least once and none is skipped. `write`
   * must therefore tolerate seeing an element again.
   *
   * Offsets are handed to the committer as each batch completes; the committer itself may batch
   * commits further (`committerSettings`), which only ever delays a commit, never moves it ahead of
   * a write. The settings should commit a batch's offsets as soon as the committer sees them
   * (`CommitWhen.OffsetFirstObserved`): `NextOffsetObserved` guards streams that emit several
   * outputs per input, and here would only hold back the last batch before a topic goes quiet until
   * more data arrives.
   */
  def sinkCommittingAfter[T](
      write: immutable.Seq[T] => Future[Any],
      committerSettings: CommitterSettings,
      batchSize: Int = 1,
      batchWithin: FiniteDuration = 1.second
  )(using ExecutionContext): Sink[(T, Committable), Future[Done]] =
    // A batch size of one needs no grouping, and `groupedWithin` is not free: it was measured
    // holding a full group of one back by a median 14 ms before emitting it (research, SC-008).
    val grouped: Flow[(T, Committable), immutable.Seq[(T, Committable)], NotUsed] =
      if batchSize == 1 then Flow[(T, Committable)].map(immutable.Seq(_))
      else Flow[(T, Committable)].groupedWithin(batchSize, batchWithin)
    grouped
      .mapAsync(1) { batch =>
        val offsets = CommittableOffsetBatch(batch.map(_._2))
        if commitFirst then
          write(batch.map(_._1)): Unit
          Future.successful(((), offsets))
        else write(batch.map(_._1)).map(_ => ((), offsets))
      }
      .toMat(Committer.sinkWithOffsetContext[Unit](committerSettings))(Keep.right)

  /**
   * The runner's form: each element is written on its own, up to `parallelism` writes in flight and
   * their results kept in the order the elements were read. An offset is handed to the committer
   * once its element's write has succeeded and a later offset of its partition has been confirmed
   * too, or when the stream completes: several elements carrying one offset (one record made into
   * several) are all confirmed before it is committed. A failed write fails the stream with nothing
   * from it onwards committed, and what was held back is not committed either.
   */
  def flowCommittingAfter[T](
      write: T => Future[Any],
      committerSettings: CommitterSettings,
      parallelism: Int
  )(using ExecutionContext): Flow[(T, CommittableOffset), Done, NotUsed] =
    type Held = Map[GroupTopicPartition, CommittableOffset]
    Flow[(T, CommittableOffset)]
      .mapAsync(parallelism) { (element, offset) =>
        if commitFirst then
          write(element): Unit
          Future.successful(offset)
        else write(element).map(_ => offset)
      }
      .statefulMap(() => Map.empty: Held)(
        (held, offset) =>
          val key = offset.partitionOffset.key
          val released =
            held.get(key).filter(_.partitionOffset.offset < offset.partitionOffset.offset)
          // Under the mutation the offset goes straight to the committer, as it was handed over.
          if commitFirst then (held, CommittableOffsetBatch(offset))
          else (held.updated(key, offset), CommittableOffsetBatch(released.toSeq))
        ,
        held => Option.when(held.nonEmpty)(CommittableOffsetBatch(held.values.toSeq))
      )
      .filter(_.batchSize > 0)
      .via(Committer.flow(committerSettings))
