package com.thinkmorestupidless.ankka.flow.operator

import com.thinkmorestupidless.ankka.flow.crd.AnkkaFlowStatus
import io.fabric8.kubernetes.api.model.Secret
import io.fabric8.kubernetes.api.model.apps.Deployment
import io.fabric8.kubernetes.api.model.rbac.{Role, RoleBinding}
import io.fabric8.kubernetes.api.model.ServiceAccount

/** What a reconcile does, as data. `Rendering` decides; the executors act. */
enum Action:
  case EnsureTopic(topic: ResolvedTopic)
  case DeleteTopic(topic: ResolvedTopic)
  case EnsureSecret(secret: Secret)
  case EnsureServiceAccount(account: ServiceAccount)
  case EnsureRole(role: Role)
  case EnsureRoleBinding(binding: RoleBinding)
  case ApplyDeployment(deployment: Deployment)
  case DeleteDeployment(namespace: String, name: String)
  case DeleteSecret(namespace: String, name: String)
  case ResetGroup(target: ResetTarget)
  case MarkResetDone(id: String)
  case EnsureFinalizer
  case RemoveFinalizer
  case RecordEvent(reason: String, eventType: String, note: String)
  case SetStatus(status: AnkkaFlowStatus)

  def describe: String = this match
    case EnsureTopic(t)                  => s"ensure topic ${t.name}"
    case DeleteTopic(t)                  => s"delete topic ${t.name}"
    case EnsureSecret(s)                 => s"ensure secret ${s.getMetadata.getName}"
    case EnsureServiceAccount(a)         => s"ensure service account ${a.getMetadata.getName}"
    case EnsureRole(r)                   => s"ensure role ${r.getMetadata.getName}"
    case EnsureRoleBinding(b)            => s"ensure role binding ${b.getMetadata.getName}"
    case ApplyDeployment(d)              => s"apply deployment ${d.getMetadata.getName}"
    case DeleteDeployment(_, n)          => s"delete deployment $n"
    case DeleteSecret(_, n)              => s"delete secret $n"
    case ResetGroup(t)                   => s"reset group ${t.groupId}"
    case MarkResetDone(id)               => s"mark reset $id done"
    case EnsureFinalizer                 => "ensure finalizer"
    case RemoveFinalizer                 => "remove finalizer"
    case RecordEvent(reason, kind, note) => s"event $kind $reason: $note"
    case SetStatus(s)                    => s"status ${s.phase}"

/** One inlet's consumer group to move to the earliest offset (FR-023). */
final case class ResetTarget(
    streamlet: String,
    inlet: String,
    groupId: String,
    topic: ResolvedTopic
)
