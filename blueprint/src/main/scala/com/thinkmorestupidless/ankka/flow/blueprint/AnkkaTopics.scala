package com.thinkmorestupidless.ankka.flow.blueprint

/**
 * An ankka topic, named by its project and its name as `ankka projects topics` lists them. ankka
 * owns it: its Kafka name is `<project>.<name>`, the pipeline never creates or deletes it, and it
 * is reached through the Kafka cluster that holds the pipeline's machine credential.
 */
final case class AnkkaTopicRef(project: String, name: String):
  def kafkaName: String         = s"$project.$name"
  override def toString: String = s"$project/$name"

object AnkkaTopicRef:
  private val Part    = "[a-z0-9](?:[a-z0-9-]*[a-z0-9])?"
  private val Pattern = s"($Part)/($Part)".r

  def parse(value: String): Option[AnkkaTopicRef] =
    value match
      case Pattern(project, name) => Some(AnkkaTopicRef(project, name))
      case _                      => None
