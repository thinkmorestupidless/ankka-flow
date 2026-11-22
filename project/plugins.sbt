addSbtPlugin("com.github.sbt" % "sbt-native-packager" % "1.11.7")
addSbtPlugin("org.scalameta"  % "sbt-scalafmt"        % "2.6.2")
// sbt-dynver comes with it: the version is derived from the nearest git tag.
addSbtPlugin("com.github.sbt" % "sbt-ci-release" % "1.12.1")
addSbtPlugin("com.eed3si9n"   % "sbt-buildinfo"  % "0.13.2")

// The protocol (research R1): ScalaPB and grpc-java, not pekko-grpc.
addSbtPlugin("com.thesamet" % "sbt-protoc" % "1.0.6")
libraryDependencies += "com.thesamet.scalapb" %% "compilerplugin" % "0.11.11"

// JUnit 5 and later from sbt, for the Java modules of feature 009 (research V10).
addSbtPlugin("com.github.sbt.junit" % "sbt-jupiter-interface" % "0.20.0")
