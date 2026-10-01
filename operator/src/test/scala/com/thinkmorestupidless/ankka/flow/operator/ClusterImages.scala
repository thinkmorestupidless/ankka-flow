package com.thinkmorestupidless.ankka.flow.operator

import java.nio.file.{Files, StandardCopyOption}

import com.github.dockerjava.api.exception.NotFoundException
import org.testcontainers.DockerClientFactory
import org.testcontainers.k3s.K3sContainer
import org.testcontainers.utility.MountableFile

/** Imports a locally built image into k3s's containerd, as ankka's suites do. */
object ClusterImages:

  def importInto(k3s: K3sContainer, image: String): Unit =
    val docker = DockerClientFactory.instance().client()
    try docker.inspectImageCmd(image).exec(): Unit
    catch
      case _: NotFoundException =>
        if image.startsWith("apache/kafka") || image.startsWith("neo4j") then
          docker.pullImageCmd(image).start().awaitCompletion(): Unit
        else
          throw new IllegalStateException(
            s"image '$image' is not in the local Docker daemon. `sbt operator/test` builds it; `testOnly` alone does too, through clusterImages."
          )
    val tar = Files.createTempFile("flow-image", ".tar")
    try
      val (name, tag) = image.lastIndexOf(':') match
        case -1 => (image, "latest")
        case at => (image.substring(0, at), image.substring(at + 1))
      val stream = docker.saveImageCmd(name).withTag(tag).exec()
      try Files.copy(stream, tar, StandardCopyOption.REPLACE_EXISTING): Unit
      finally stream.close()
      val inContainer = "/tmp/flow-image.tar"
      k3s.copyFileToContainer(MountableFile.forHostPath(tar), inContainer)
      // `ctr` on k3s's own containerd socket, in the k8s.io namespace kubelet reads (ankka's lesson:
      // the default namespace imports fine and kubelet never sees it).
      val result = k3s.execInContainer(
        "ctr",
        "-a",
        "/run/k3s/containerd/containerd.sock",
        "-n",
        "k8s.io",
        "images",
        "import",
        inContainer
      )
      if result.getExitCode != 0 then
        throw new IllegalStateException(s"importing $image into k3s failed: ${result.getStderr}")
    finally Files.deleteIfExists(tar): Unit
