package com.thinkmorestupidless.ankka.flow.crd

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.dataformat.yaml.{YAMLFactory, YAMLGenerator}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import io.fabric8.kubernetes.api.model.HasMetadata
import io.fabric8.kubernetes.client.utils.KubernetesSerialization

/** The resource's identity, read back off the annotations so the strings cannot drift. */
object AnkkaFlowDefinition:

  val resourceClass: Class[AnkkaFlow] = classOf[AnkkaFlow]

  val group: String   = HasMetadata.getGroup(resourceClass)
  val version: String = HasMetadata.getVersion(resourceClass)
  val kind: String    = HasMetadata.getKind(resourceClass)
  val plural: String  = HasMetadata.getPlural(resourceClass)

  /** `flow.ankka.thinkmorestupidless.com/v1alpha1`. */
  val apiVersion: String = HasMetadata.getApiVersion(resourceClass)

  /** `ankkaflows.flow.ankka.thinkmorestupidless.com`. */
  val crdName: String = s"$plural.$group"

  /** Where the CRD manifest lives on the classpath. */
  val manifestResource: String = "/ankka-flow/crd/ankkaflow.yaml"

  /** The label and annotation domain. */
  val domain: String = group

/**
 * Serialization that understands Scala, handed to each client explicitly (ankka's rule): without
 * the Scala module `Option` encodes as `{"empty":false,"defined":true}` and collections do not
 * round-trip, silently.
 */
object FlowSerialization:

  def mapper(): ObjectMapper =
    val m = new ObjectMapper()
    m.registerModule(DefaultScalaModule)
    m.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    m

  def apply(): KubernetesSerialization = new KubernetesSerialization(mapper(), true)

  /**
   * YAML for people and `kubectl apply -f -`. fabric8's own YAML writer cannot represent Scala
   * maps.
   */
  def yamlMapper(): ObjectMapper =
    val m = new ObjectMapper(
      new YAMLFactory()
        .disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER)
        .enable(YAMLGenerator.Feature.MINIMIZE_QUOTES)
        .enable(YAMLGenerator.Feature.ALWAYS_QUOTE_NUMBERS_AS_STRINGS)
    )
    m.registerModule(DefaultScalaModule)
    m.setDefaultPropertyInclusion(
      JsonInclude.Value.construct(JsonInclude.Include.NON_NULL, JsonInclude.Include.NON_NULL)
    ): Unit
    m.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    m

  def toYaml(resource: AnkkaFlow): String = yamlMapper().writeValueAsString(resource)

  def fromYaml(text: String): AnkkaFlow = yamlMapper().readValue(text, classOf[AnkkaFlow])
