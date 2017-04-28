lazy val `latis-netcdf` = (project in file(".")).
  dependsOn(latis).
  settings(
    name := "latis-netcdf",
    libraryDependencies ++= Seq(
      "edu.ucar" % "cdm" % "4.5.5"
    )
  ).
  settings(commonSettings:_*)

lazy val bench = (project in file("bench")).
  dependsOn(`latis-netcdf`).
  settings(commonSettings:_*).
  enablePlugins(JmhPlugin)

lazy val latis = ProjectRef(file("../latis"), "latis")

lazy val commonSettings = Seq(
  scalaVersion := "2.11.8"
)
