organization := "com.inkenkun.x1"

name := "spanner-sandbox"

version := "0.1"

scalaVersion := "2.12.3"

val circeVersion    = "0.8.0"

libraryDependencies ++= Seq(
  "com.google.cloud" % "google-cloud-spanner"       % "0.23.1-beta",
  "io.circe"        %% "circe-generic"              % circeVersion,
  "io.circe"        %% "circe-literal"              % circeVersion,
  "io.circe"        %% "circe-generic-extras"       % circeVersion,
  "io.circe"        %% "circe-parser"               % circeVersion,
  "org.scalatest"   %% "scalatest"                  % "3.0.1"       % "test",
  "org.scalacheck"  %% "scalacheck"                 % "1.13.4"      % "test"
)
