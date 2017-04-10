lazy val `latis-netcdf` = (project in file(".")).
  dependsOn(latis).
  settings(
    name := "latis-netcdf",
    scalaVersion := "2.11.8",
    libraryDependencies ++= Seq(
      "edu.ucar" % "cdm" % "4.5.5"
    )
  )

lazy val latis = ProjectRef(file("../latis"), "latis")
