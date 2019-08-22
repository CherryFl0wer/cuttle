val devMode = settingKey[Boolean]("Some build optimization are applied in devMode.")
val writeClasspath = taskKey[File]("Write the project classpath to a file.")

val VERSION = "0.9.3-SNAPSHOT"

lazy val catsCore = "1.6.0"
lazy val circe = "0.11.0"
lazy val doobie = "0.6.0"

addCompilerPlugin("org.spire-math" % "kind-projector" % "0.9.9" cross CrossVersion.binary)
addCompilerPlugin("org.scalamacros" % "paradise" % "2.1.0" cross CrossVersion.full)

lazy val commonSettings = Seq(
  organization := "com.criteo.cuttle",
  version := VERSION,
  scalaVersion := "2.12.8",
  crossScalaVersions := Seq("2.11.11", "2.12.3"),
  scalacOptions ++= Seq(
    "-deprecation",
    "-encoding",
    "UTF-8",
    "-feature",
    "-unchecked",
    "-Xlint",
    "-Yno-adapted-args",
    "-Ywarn-dead-code",
    "-Xfuture",
    "-Ywarn-unused",
    "-Ywarn-unused-import",
    "-Ypartial-unification",
    "-Xmacro-settings:materialize-derivations"
  ),
  devMode := Option(System.getProperty("devMode")).isDefined,
  writeClasspath := {
    val f = file(s"/tmp/classpath_${organization.value}.${name.value}")
    val classpath = (fullClasspath in Test).value
    IO.write(f, classpath.map(_.data).mkString(":"))
    streams.value.log.info(f.getAbsolutePath)
    f
  },
  // test config
  testOptions in IntegrationTest := Seq(Tests.Filter(_ endsWith "ITest"), Tests.Argument("-oF")),
  // Maven config
  credentials += Credentials(
    "Sonatype Nexus Repository Manager",
    "oss.sonatype.org",
    "criteo-oss",
    sys.env.getOrElse("SONATYPE_PASSWORD", "")
  ),
  publishTo := Some(
    if (isSnapshot.value)
      Opts.resolver.sonatypeSnapshots
    else
      Opts.resolver.sonatypeStaging
  )
)

def removeDependencies(groups: String*)(xml: scala.xml.Node) = {
  import scala.xml._
  import scala.xml.transform._
  (new RuleTransformer(
    new RewriteRule {
      override def transform(n: Node): Seq[Node] = n match {
        case dependency @ Elem(_, "dependency", _, _, _*) =>
          if (dependency.child.collectFirst { case e: Elem => e }.exists { e =>
                groups.exists(group => e.toString == s"<groupId>$group</groupId>")
              }) Nil
          else dependency
        case x => x
      }
    }
  ))(xml)
}


lazy val cuttle =
  (project in file("core"))
    .configs(IntegrationTest)
    .settings(commonSettings: _*)
    .settings(Defaults.itSettings: _*)
    .settings(
      libraryDependencies ++= Seq("core", "generic", "parser", "java8")
        .map(module => "io.circe" %% s"circe-$module" % circe),
      libraryDependencies ++= Seq(
        "de.sciss" %% "fingertree" % "1.5.2",
        "org.scala-stm" %% "scala-stm" % "0.8",
        "org.scala-lang" % "scala-reflect" % scalaVersion.value,
        "org.typelevel" %% "cats-core" % catsCore,
        "org.typelevel" %% "cats-effect" % "1.2.0",
        "codes.reactive" %% "scala-time" % "0.4.1",
        "com.zaxxer" % "nuprocess" % "1.1.0",
        "org.postgresql" % "postgresql" % "42.2.5",
        "org.tpolecat" %% "doobie-postgres" % "0.6.0",
        "org.apache.kafka" %% "kafka" % "0.10.2.2",
        "org.scala-lang.modules" %% "scala-async" % "0.9.7",
        "io.circe" %% "circe-optics" % "0.11.0"
      ),
      libraryDependencies ++= Seq(
        "org.tpolecat" %% "doobie-core",
        "org.tpolecat" %% "doobie-hikari"
      ).map(_ % doobie),
      libraryDependencies ++= Seq(
        "org.scalatest" %% "scalatest" % "3.0.1",
        "org.mockito" % "mockito-all" % "1.10.19",
        "org.tpolecat" %% "doobie-scalatest" % doobie
      ).map(_ % "it,test")
    )



lazy val flow =
  (project in file("flow"))
    .settings(commonSettings: _*)
    .settings(
      libraryDependencies ++= Seq(
        "com.ovoenergy" %% "fs2-kafka" % "0.19.9",
        "co.fs2" %% "fs2-reactive-streams" % "1.0.4"
      ))
    .dependsOn(cuttle % "compile->compile;test->test")

lazy val examples =
  (project in file("examples"))
    .settings(commonSettings: _*)
    .settings(
      fork in Test := true,
      connectInput in Test := true,
      javaOptions ++= Seq("-Xmx256m", "-XX:+HeapDumpOnOutOfMemoryError")
    )
    .settings(
      Option(System.getProperty("generateExamples"))
        .map(_ =>
          Seq(
            autoCompilerPlugins := true,
            addCompilerPlugin("com.criteo.socco" %% "socco-plugin" % "0.1.7"),
            scalacOptions := Seq(
              "-P:socco:out:examples/target/html",
              "-P:socco:package_com.criteo.cuttle:https://criteo.github.io/cuttle/api/"
            )
          ))
        .getOrElse(Nil): _*
    )
    .dependsOn(cuttle, flow)

lazy val root =
  (project in file("."))
    .enablePlugins(ScalaUnidocPlugin) 
    .settings(commonSettings: _*)
    .settings(
      scalacOptions in (ScalaUnidoc, unidoc) ++= Seq(
        Seq(
          "-sourcepath",
          baseDirectory.value.getAbsolutePath
        ),
        Opts.doc.title("cuttle"),
        Opts.doc.version(VERSION),
        Opts.doc.sourceUrl("https://github.com/criteo/cuttle/blob/master€{FILE_PATH}.scala"),
        Seq(
          "-doc-root-content",
          (baseDirectory.value / "core/src/main/scala/root.scala").getAbsolutePath
        )
      ).flatten,
      unidocAllAPIMappings in (ScalaUnidoc, unidoc) ++= {
        val allJars = {
          (fullClasspath in cuttle in Compile).value
        }
        Seq(
          allJars
            .flatMap(x => x.metadata.get(moduleID.key).map(m => x.data -> m))
            .collect {
              case (jar, module) if module.name == "scala-library" =>
                jar -> url("https://www.scala-lang.org/api/current/")
              case (jar, module) if module.name.contains("doobie") =>
                jar -> url("https://www.javadoc.io/doc/org.tpolecat/doobie-core_2.12/0.4.1/")
              case (jar, module) if module.name.contains("circe") =>
                jar -> url("http://circe.github.io/circe/api/")
            }
            .toMap
        )
      },
      unidocProjectFilter in (ScalaUnidoc, unidoc) := inProjects(cuttle, flow)
    )
    .aggregate(cuttle, flow)
