import scala.collection.Seq

performMavenCentralSync in ThisBuild := false   // basically just ignores all the sonatype sync parts of things

publishAsOSSProject in ThisBuild := true

homepage in ThisBuild := Some(url("https://github.com/slamdata/quasar-destination-postgres"))

scmInfo in ThisBuild := Some(ScmInfo(
  url("https://github.com/slamdata/quasar-destination-postgres"),
  "scm:git@github.com:slamdata/quasar-destination-postgres.git"))

val DoobieVersion = "0.7.0"

lazy val root = project
  .in(file("."))
  .settings(noPublishSettings)
  .aggregate(core)

lazy val core = project
  .in(file("core"))
  .settings(
    name := "quasar-destination-postgres",

    quasarPluginName := "postgres",

    quasarPluginQuasarVersion := IO.read(file("./quasar-version")).trim,

    quasarPluginDestinationFqcn := Some("quasar.plugin.postgres.PostgresDestinationModule$"),

    /** Specify managed dependencies here instead of with `libraryDependencies`.
      * Do not include quasar libs, they will be included based on the value of
      * `quasarPluginQuasarVersion`.
      */
    quasarPluginDependencies ++= Seq(
      "org.slf4s"    %% "slf4s-api"       % "1.7.25",
      "org.tpolecat" %% "doobie-core"     % DoobieVersion,
      "org.tpolecat" %% "doobie-hikari"   % DoobieVersion,
      "org.tpolecat" %% "doobie-postgres" % DoobieVersion
    ),

    libraryDependencies ++= Seq(
      "com.github.tototoshi" %% "scala-csv" % "1.3.6" % Test,
      "com.slamdata" %% "qdata-core" % "11.0.5" % Test,
      "com.slamdata" %% "quasar-foundation" % quasarPluginQuasarVersion.value % "test->test" classifier "tests",
      "io.argonaut" %% "argonaut-scalaz" % "6.2.3" % Test
    ))
  .enablePlugins(AutomateHeaderPlugin, QuasarPlugin)
