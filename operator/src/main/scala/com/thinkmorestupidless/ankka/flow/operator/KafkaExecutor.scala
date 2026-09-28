package com.thinkmorestupidless.ankka.flow.operator

import java.util.Properties
import java.util.concurrent.{ConcurrentHashMap, ExecutionException, TimeUnit}

import scala.jdk.CollectionConverters.*
import scala.util.Try

import org.apache.kafka.clients.admin.{Admin, AdminClientConfig, NewTopic}
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.errors.{TopicExistsException, UnknownTopicOrPartitionException}
import org.slf4j.LoggerFactory

/**
 * Everything the operator does to Kafka: describe, create and delete topics, and reset consumer
 * groups. One Admin client per set of brokers and credentials, as Cloudflow's `KafkaAdmins`.
 */
final class KafkaExecutor extends AutoCloseable:

  private val log     = LoggerFactory.getLogger(classOf[KafkaExecutor])
  private val admins  = new ConcurrentHashMap[(String, Map[String, String]), Admin]()
  private val Timeout = 20L

  def admin(t: ResolvedTopic): Admin =
    admins.computeIfAbsent(
      t.connectionKey,
      { case (bootstrap, connection) =>
        val props = new Properties()
        connection.foreach((k, v) => props.put(k, v))
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap)
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "15000")
        props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "20000")
        props.put(AdminClientConfig.CLIENT_ID_CONFIG, "ankka-flow-operator")
        Admin.create(props)
      }
    )

  private def cause(e: Throwable): Throwable = e match
    case ee: ExecutionException if ee.getCause != null => ee.getCause
    case other                                         => other

  def describe(t: ResolvedTopic): TopicState =
    try
      val d = admin(t)
        .describeTopics(java.util.List.of(t.name))
        .allTopicNames()
        .get(Timeout, TimeUnit.SECONDS)
        .get(t.name)
      val partitions  = d.partitions.size
      val replication = d.partitions.asScala.headOption.fold(0)(_.replicas.size)
      val configs =
        if t.topicConfig.isEmpty then Map.empty[String, String]
        else
          val resource = new ConfigResource(ConfigResource.Type.TOPIC, t.name)
          val all = admin(t)
            .describeConfigs(java.util.List.of(resource))
            .all()
            .get(Timeout, TimeUnit.SECONDS)
            .get(resource)
          t.topicConfig.keySet.flatMap(k => Option(all.get(k)).map(e => k -> e.value)).toMap
      TopicState.Exists(partitions, replication, configs)
    catch
      case e: Exception =>
        cause(e) match
          case _: UnknownTopicOrPartitionException => TopicState.Missing
          case other =>
            TopicState.Unreachable(Option(other.getMessage).getOrElse(other.getClass.getSimpleName))

  /** Creates a managed topic; one that already exists is left as it is (FR-019). */
  def ensure(t: ResolvedTopic): Unit =
    val topic =
      new NewTopic(t.name, t.partitions.get, t.replicas.get.toShort).configs(t.topicConfig.asJava)
    try
      admin(t).createTopics(java.util.List.of(topic)).all().get(Timeout, TimeUnit.SECONDS)
      log.info(
        "created topic {} ({} partitions, replication {})",
        t.name,
        t.partitions.get,
        t.replicas.get
      )
    catch
      case e: Exception =>
        cause(e) match
          case _: TopicExistsException => ()
          case other                   => throw other

  def delete(t: ResolvedTopic): Unit =
    try
      admin(t).deleteTopics(java.util.List.of(t.name)).all().get(Timeout, TimeUnit.SECONDS)
      log.info("deleted topic {}", t.name)
    catch
      case e: Exception =>
        cause(e) match
          case _: UnknownTopicOrPartitionException => ()
          case other                               => throw other

  /** Moves a group to the earliest offsets (FR-023); the number of partitions reset, or why not. */
  def reset(target: ResetTarget): Either[String, Int] =
    Try(
      ConsumerGroupReset.toEarliest(admin(target.topic), target.groupId, target.topic.name)
    ).toEither.left.map { e =>
      cause(e) match
        case g: ConsumerGroupReset.GroupHasActiveMembers => g.getMessage
        case other => Option(other.getMessage).getOrElse(other.getClass.getSimpleName)
    }

  def close(): Unit =
    admins.values.asScala.foreach(a => Try(a.close(java.time.Duration.ofSeconds(5))))
    admins.clear()
