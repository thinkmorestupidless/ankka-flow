package com.thinkmorestupidless.ankka.flow.operator

import com.thinkmorestupidless.ankka.flow.crd.FlowSerialization
import io.fabric8.kubernetes.client.KubernetesClientBuilder

object Main:

  def main(args: Array[String]): Unit =
    val settings = Settings.load()
    val client =
      new KubernetesClientBuilder().withKubernetesSerialization(FlowSerialization()).build()
    val kafka = new KafkaExecutor
    val operator = new Operator(
      client,
      settings,
      new PipelineReconciler(client, settings, new Fabric8Executor(client, settings), kafka)
    )
    sys.addShutdownHook {
      operator.close()
      kafka.close()
      client.close()
    }
    operator.start()
    operator.awaitTermination()
