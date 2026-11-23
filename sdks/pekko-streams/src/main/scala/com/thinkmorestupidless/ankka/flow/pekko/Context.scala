package com.thinkmorestupidless.ankka.flow.pekko

import org.apache.pekko.actor.typed.ActorSystem

/** What the logic is given: its parameters' values, and the actor system it runs in. */
trait Context:

  /** The parameter's value, typed as declared: the deployed value, else its default. */
  def apply[A](parameter: Parameter[A]): A

  def system: ActorSystem[?]

private[pekko] object Context:

  /**
   * Every declared parameter's value, from the text of each: `values` over the defaults. A
   * parameter with neither, or a value not of its type, fails naming the key.
   */
  def resolve(
      parameters: Seq[Parameter[?]],
      values: Map[String, String],
      sys: ActorSystem[?]
  ): Context =
    val resolved: Map[String, Any] = parameters.map { p =>
      val text = values
        .get(p.key)
        .orElse(p.default)
        .getOrElse(
          throw new IllegalArgumentException(s"parameter '${p.key}' has no default and no value")
        )
      p.key -> p
        .parse(text)
        .fold(e => throw new IllegalArgumentException(s"parameter '${p.key}': $e"), identity)
    }.toMap
    new Context:
      def apply[A](parameter: Parameter[A]): A =
        resolved
          .getOrElse(
            parameter.key,
            throw new IllegalArgumentException(s"parameter '${parameter.key}' is not declared")
          )
          .asInstanceOf[A]
      def system: ActorSystem[?] = sys
