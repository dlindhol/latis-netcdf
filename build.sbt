ThisBuild / organization := "io.latis-data"
ThisBuild / scalaVersion := "2.11.8"

val artifactory = "http://web-artifacts.lasp.colorado.edu/artifactory/"

lazy val `latis-netcdf` = (project in file("."))
  .settings(commonSettings)
  .settings(publishSettings)
  .settings(
    name := "latis-netcdf",
    libraryDependencies ++= Seq(
      "io.latis-data" %% "latis" % "2.46.0",
      "edu.ucar"      %  "cdm"   % "4.6.10"
    ),
    resolvers += "Unidata" at "https://artifacts.unidata.ucar.edu/content/repositories/unidata-releases"
  )

lazy val commonSettings = compilerFlags ++ Seq(
  Compile / compile / wartremoverWarnings ++= Warts.allBut(
    Wart.Any,         // false positives
    Wart.Nothing,     // false positives
    Wart.Product,     // false positives
    Wart.Serializable // false positives
  ),
  // Test suite dependencies
  libraryDependencies ++= Seq(
    "junit"        % "junit"           % "4.12" % Test,
    "com.novocode" % "junit-interface" % "0.11" % Test
  ),
  // Resolvers for our Artifactory repos
  resolvers ++= Seq(
    "Artifactory Release" at artifactory + "sbt-release",
    "Artifactory Snapshot" at artifactory + "sbt-snapshot",
    "Artifactory External" at artifactory + "ext-release-local"
  )
)

lazy val compilerFlags = Seq(
  scalacOptions ++= Seq(
    "-deprecation",
    "-encoding", "utf-8",
    "-feature",
  ),
  Compile / compile / scalacOptions ++= Seq(
    "-unchecked",
    "-Xlint",
    "-Ywarn-dead-code",
    "-Ywarn-numeric-widen",
    "-Ywarn-value-discard"
  )
)

lazy val publishSettings = Seq(
  publishTo := {
    if (isSnapshot.value) {
      Some("snapshots" at artifactory + "sbt-snapshot")
    } else {
      Some("releases" at artifactory + "sbt-release")
    }
  },
  credentials ++= Seq(
    Path.userHome / ".artifactorycredentials"
  ).filter(_.exists).map(Credentials(_))
)
