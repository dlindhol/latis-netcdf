lazy val `latis-netcdf` = (project in file(".")).
  dependsOn(latis).
  settings(
    name := "latis-netcdf",
    libraryDependencies ++= Seq(
      "edu.ucar" % "cdm" % "4.6.10",
      "junit" % "junit" % "4.+" % Test,
      "com.novocode" % "junit-interface" % "0.11" % Test
    ),
    resolvers += "Unidata Artifacts" at
      "https://artifacts.unidata.ucar.edu/content/repositories/unidata-releases/",
    logBuffered in Test := false
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
