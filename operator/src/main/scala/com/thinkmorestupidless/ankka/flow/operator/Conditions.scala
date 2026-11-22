package com.thinkmorestupidless.ankka.flow.operator

import com.thinkmorestupidless.ankka.flow.crd.{AnkkaFlow, Condition}

/**
 * The pipeline's conditions (research R17). A condition is listed while it holds; `Ready` is always
 * listed, `True` or `False`. A condition that keeps its status keeps the time it last changed, so
 * an unchanged report is not written again.
 */
object Conditions:

  val FlinkOperatorMissingMessage =
    "install the Flink Kubernetes Operator (see Install the platform)"

  def stateStoreMissingMessage(clustersNamespace: String): String =
    s"create the Secret ${StateStore.SecretName} in namespace $clustersNamespace with a dir key"

  def holds(kind: String, reason: String, message: String, now: String): Condition =
    Condition(kind, Condition.True, reason, message, now)

  def ready(isReady: Boolean, reason: String, message: String, now: String): Condition =
    Condition(
      Condition.Ready,
      if isReady then Condition.True else Condition.False,
      reason,
      message,
      now
    )

  /** `next`, each keeping its previous transition time when its status did not change. */
  def merge(previous: List[Condition], next: List[Condition]): List[Condition] =
    next.map { c =>
      previous.find(p => p.`type` == c.`type` && p.status == c.status) match
        case Some(p) if p.lastTransitionTime.nonEmpty =>
          c.copy(lastTransitionTime = p.lastTransitionTime)
        case _ => c
    }

  def previous(resource: AnkkaFlow): List[Condition] =
    Option(resource.getStatus).map(_.conditions).getOrElse(Nil)

  /**
   * What a pipeline with a Flink streamlet needs of the cluster and lacks: the Flink operator's CRD
   * and the state store (research R11). Empty for a pipeline with none.
   */
  def prerequisites(
      hasFlink: Boolean,
      observed: Observed,
      settings: Settings,
      now: String
  ): List[Condition] =
    if !hasFlink then Nil
    else
      Option
        .when(!observed.flinkCrd)(
          holds(
            Condition.FlinkOperatorMissing,
            "CrdNotFound",
            FlinkOperatorMissingMessage,
            now
          )
        )
        .toList ++
        Option
          .when(observed.stateStore.isEmpty)(
            holds(
              Condition.StateStoreMissing,
              "SecretNotFound",
              stateStoreMissingMessage(settings.clustersNamespace),
              now
            )
          )
          .toList
