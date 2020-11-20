package be.raffiki

import java.nio.file.Paths

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream._
import akka.stream.scaladsl.GraphDSL.Implicits.port2flow
import akka.stream.scaladsl._
import akka.util.ByteString
import com.typesafe.scalalogging.{LazyLogging, Logger}

import scala.concurrent._
import scala.util.{Failure, Success}

object Main extends App with LazyLogging {
  import be.raffiki.Model._

  implicit val system: ActorSystem = ActorSystem("Dependencies")
  implicit val ec: ExecutionContext = system.dispatcher

  val inputPath = Paths.get("src/main/resources/maven_dependencies.txt")
  val outputPath = Paths.get("src/main/resources/output.txt")

  val lineDelimiter = Framing
    .delimiter(ByteString(System.lineSeparator()),
               maximumFrameLength = 512,
               allowTruncation = true)
    .map(_.utf8String)

  val count: Flow[Row, DependencyCount, NotUsed] =
    Flow[Row]
      .map { row =>
        row.kind match {
          case Compile => DependencyCount(Some(row.library), Count(Compile, 1))
          case Provided =>
            DependencyCount(Some(row.library),
                            providedCount = Count(Provided, 1))
          case Runtime =>
            DependencyCount(Some(row.library), runtimeCount = Count(Runtime, 1))
          case Test =>
            DependencyCount(Some(row.library), testCount = Count(Test, 1))
        }
      }

  val sum: Flow[DependencyCount, DependencyCount, NotUsed] =
    Flow[DependencyCount]
      .fold(DependencyCount(None))(_ add _)
      .filter(_.library.isDefined)

  val counter: Flow[Row, DependencyCount, NotUsed] =
    Flow.fromGraph(GraphDSL.create() { implicit builder =>
      val dispatchRow = builder.add(Balance[Row](2))
      val mergeDependencyCounts = builder.add(Merge[DependencyCount](2))

      dispatchRow.out(0) ~> count.async ~> sum.async ~> mergeDependencyCounts
        .in(0)
      dispatchRow.out(1) ~> count.async ~> sum.async ~> mergeDependencyCounts
        .in(1)

      FlowShape(dispatchRow.in, mergeDependencyCounts.out)
    })

  FileIO
    .fromPath(inputPath)
    .via(lineDelimiter)
    .map(createRow)
    .groupBy(Int.MaxValue, _.library)
    .via(counter)
    .mergeSubstreams
    .map(_.show())
    .map(ByteString(_))
    .runWith(FileIO.toPath(outputPath))
    .onComplete {
      case Success(_) => system.terminate()
      case Failure(exception) => {
        logger.error(s"Error thrown: $exception")
        system.terminate()
      }
    }
}

object Model extends LazyLogging {

  sealed trait Kind {
    def show(): String
  }

  case object Compile extends Kind {
    override def show(): String = "Compile"
  }

  case object Provided extends Kind {
    override def show(): String = "Provided"
  }

  case object Runtime extends Kind {
    override def show(): String = "Runtime"
  }

  case object Test extends Kind {
    override def show(): String = "Test"
  }

  case class Library(groupId: String, artifactId: String, version: String) {
    def show(): String = s"$groupId:$artifactId:$version"
  }

  case class Row(library: Library, dependency: Library, kind: Kind)

  case class Count(kind: Kind, count: Integer) {
    def show(): String = s"${kind.show()}: ${count}"
  }

  case class DependencyCount(library: Option[Library],
                             compileCount: Count = Count(Compile, 0),
                             providedCount: Count = Count(Provided, 0),
                             runtimeCount: Count = Count(Runtime, 0),
                             testCount: Count = Count(Test, 0)) {

    def add(other: DependencyCount) = {
      if (other.library.isEmpty || library.orElse(other.library).isEmpty) {
        logger.error(s"adding empty library with $this")
      }
      DependencyCount(
        library.orElse(other.library),
        Count(Compile, compileCount.count + other.compileCount.count),
        Count(Provided, providedCount.count + other.providedCount.count),
        Count(Runtime, runtimeCount.count + other.runtimeCount.count),
        Count(Test, runtimeCount.count + other.runtimeCount.count)
      )
    }

    def show(): String =
      s"""${library.map(_.show()).getOrElse("")} --> ${compileCount
        .show()} ${providedCount.show()} ${runtimeCount.show()} ${testCount
        .show()}${System.lineSeparator()}"""
  }

  def createRow(line: String): Row = line match {
    case s"$groupId1:$artifactId1:$version1,$groupId2:$artifactId2:$version2,Compile" =>
      Row(Library(groupId1, artifactId1, version1),
          Library(groupId2, artifactId2, version2),
          Compile)
    case s"$groupId1:$artifactId1:$version1,$groupId2:$artifactId2:$version2,Provided" =>
      Row(Library(groupId1, artifactId1, version1),
          Library(groupId2, artifactId2, version2),
          Provided)
    case s"$groupId1:$artifactId1:$version1,$groupId2:$artifactId2:$version2,Runtime" =>
      Row(Library(groupId1, artifactId1, version1),
          Library(groupId2, artifactId2, version2),
          Runtime)
    case s"$groupId1:$artifactId1:$version1,$groupId2:$artifactId2:$version2,Test" =>
      Row(Library(groupId1, artifactId1, version1),
          Library(groupId2, artifactId2, version2),
          Test)
    case line =>
      throw new Exception(
        s"input line $line does not comply to expected format")
  }
}
