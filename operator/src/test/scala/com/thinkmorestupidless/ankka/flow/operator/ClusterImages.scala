package com.thinkmorestupidless.ankka.flow.operator

import java.nio.file.Files

import scala.sys.process.*

import org.testcontainers.k3s.K3sContainer
import org.testcontainers.utility.MountableFile

/**
 * Imports images from the local Docker daemon into k3s's containerd, as ankka's suites do. The
 * build's own images must already be there; any other is pulled. Saved for the daemon's own
 * platform: Docker's containerd store keeps a pulled image's index, which `ctr import` cannot read
 * whole. Images that share layers are imported from one archive, so each layer is copied once.
 */
object ClusterImages:

  private def ours(image: String): Boolean =
    image.startsWith("ankka-flow-") || image.startsWith("sample-")

  private lazy val platform: String =
    s"linux/${Seq("docker", "version", "--format", "{{.Server.Arch}}").!!.trim}"

  private def present(image: String): Boolean =
    Seq("docker", "image", "inspect", image).!(ProcessLogger(_ => (), _ => ())) == 0

  def importInto(k3s: K3sContainer, images: String*): Unit =
    images.filterNot(present).foreach { image =>
      if ours(image) then
        throw new IllegalStateException(
          s"image '$image' is not in the local Docker daemon. `sbt operator/test` builds it; `testOnly` alone does too, through clusterImages."
        )
      if Seq("docker", "pull", "-q", "--platform", platform, image).! != 0 then
        throw new IllegalStateException(s"pulling $image failed")
    }
    val tar = Files.createTempFile("flow-images", ".tar")
    try
      val save = Seq("docker", "save", "--platform", platform, "-o", tar.toString) ++ images
      if save.! != 0 then throw new IllegalStateException(s"saving ${images.mkString(", ")} failed")
      val inContainer = "/tmp/flow-images.tar"
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
        throw new IllegalStateException(
          s"importing ${images.mkString(", ")} into k3s failed: ${result.getStderr}"
        )
      k3s.execInContainer("rm", "-f", inContainer): Unit
    finally Files.deleteIfExists(tar): Unit
