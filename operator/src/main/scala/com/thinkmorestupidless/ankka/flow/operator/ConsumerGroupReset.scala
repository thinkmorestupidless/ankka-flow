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

package com.thinkmorestupidless.ankka.flow.operator

import java.util.concurrent.TimeUnit

import scala.jdk.CollectionConverters.*

import org.apache.kafka.clients.admin.{Admin, OffsetSpec}
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition

/**
 * Moves a consumer group's committed offsets on one topic back to the earliest offset of each
 * partition. Carried from Cloudflow's `cloudflow.operator.action.ConsumerGroupReset`
 * (core/cloudflow-operator/src/main/scala/cloudflow/operator/action/ConsumerGroupReset.scala),
 * blocking rather than returning a Future: the operator reconciles on worker threads.
 */
object ConsumerGroupReset:

  final case class GroupHasActiveMembers(groupId: String, members: Int)
      extends RuntimeException(
        s"consumer group [$groupId] has $members active member(s); scale its streamlet to 0 and wait for its pods to " +
          "stop before resetting it"
      )

  private val Timeout = 20L

  /** @return the number of partitions reset. */
  def toEarliest(admin: Admin, groupId: String, topic: String): Int =
    val group = admin
      .describeConsumerGroups(List(groupId).asJava)
      .all()
      .get(Timeout, TimeUnit.SECONDS)
      .get(groupId)
    // Kafka also refuses to alter the offsets of a group with members, but its error names neither
    // the group nor what to do about it.
    if !group.members.isEmpty then throw GroupHasActiveMembers(groupId, group.members.size)
    val description = admin
      .describeTopics(List(topic).asJava)
      .allTopicNames()
      .get(Timeout, TimeUnit.SECONDS)
      .get(topic)
    val partitions =
      description.partitions.asScala.map(p => new TopicPartition(topic, p.partition)).toList
    val earliest =
      admin
        .listOffsets(partitions.map(_ -> OffsetSpec.earliest()).toMap.asJava)
        .all()
        .get(Timeout, TimeUnit.SECONDS)
    val offsets =
      earliest.asScala.map((partition, info) => partition -> new OffsetAndMetadata(info.offset))
    admin.alterConsumerGroupOffsets(groupId, offsets.asJava).all().get(Timeout, TimeUnit.SECONDS)
    partitions.size
