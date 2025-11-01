package ragPipeline

import java.nio.file.{Files, Path}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path => HPath}
import ragPipeline.config.AppConfig

object DriverMain {

  private def usage(): Unit = {
    Console.err.println(
      "Usage:\n" +
        "  run -- add-context [--pdfRoot <dir>] [--outDir <dir>] [--reducers N]\n" +
        "  run -- question   [<shardsParentDir>] <question text...>\n"
    )
  }

  /** Ensure required local directories exist before Hadoop writes. */
  private def ensureLocalDirs(): Unit = {
    val inDir: Path  = AppConfig.paths.pdfRoot   // already a java.nio.file.Path
    val outDir: Path = AppConfig.paths.outDir    // already a java.nio.file.Path

    Files.createDirectories(inDir)
    Files.createDirectories(outDir)

    val conf = new Configuration()
    val fs   = FileSystem.get(conf)
    val parentDir = new HPath(outDir.toString)   // Hadoop Path needs String
    if (!fs.exists(parentDir)) fs.mkdirs(parentDir)
  }

  def main(args: Array[String]): Unit = {
    ensureLocalDirs()

    if (args.isEmpty) {
      println("Choose mode: [1] add-context  [2] question")
      print("> ")
      scala.io.StdIn.readLine().trim.toLowerCase match {
        case "1" | "add-context" | "build" =>
          ragPipeline.SetContextDatabase.ContextDatabaseCreation.run(Array.empty)

        case "2" | "question" | "query" =>
          val defaultShards = AppConfig.paths.outDir
          println(s"Using shards directory: $defaultShards")
          print("Question: ")
          val qOpt = Option(scala.io.StdIn.readLine()).map(_.trim).filter(_.nonEmpty)
          qOpt match {
            case Some(q) =>
              if(AppConfig.engine.engine == "spark"){
                ragPipeline.Query.QueryMain.run(Array(defaultShards.toString, q))
              }
              // mapReducer
              else {
                ragPipeline.Query.QueryEngineMapReducer.run(Array(defaultShards.toString, q))
              }
            case None =>
                Console.err.println("No question provided. Exiting gracefully."); return
          }

        case other =>
          Console.err.println(s"Unknown choice: $other"); usage(); sys.exit(1)
      }

    } else {
      args.head.toLowerCase match {
        // ---------------- ADD CONTEXT ----------------
        case "add-context" | "build" =>
          ragPipeline.SetContextDatabase.ContextDatabaseCreation.run(args.tail)

        // ---------------- QUESTION ----------------
        case "question" | "query" =>
          if (args.length == 2) {
            // Case: question <question text>  (use default shards dir)
            val question = args(1)
            val shardsParent = AppConfig.paths.outDir
            println(s"[Config] Using default shards directory: $shardsParent")
            if(AppConfig.engine.engine == "spark"){
              ragPipeline.Query.QueryMain.run(Array(shardsParent.toString, question))
            }
            // engine is mr
            else {
              ragPipeline.Query.QueryEngineMapReducer.run(Array(shardsParent.toString, question))
            }

          }
          else if (args.length >= 3) {
            // Case: question <shardsParentDir> <question...>
            val shardsParent = args(1)
            val question = args.drop(2).mkString(" ").trim
            if(AppConfig.engine.engine == "spark"){
              ragPipeline.Query.QueryMain.run(Array(shardsParent, question))
            }
            // engine is mr
            else {
              ragPipeline.Query.QueryEngineMapReducer.run(Array(shardsParent, question))
            }
          }
          else {
            Console.err.println("question mode requires: <question...>"); usage(); sys.exit(1)
          }

        case other =>
          Console.err.println(s"Unknown subcommand: $other"); usage(); sys.exit(1)
      }
    }
  }


}
