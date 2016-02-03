package dagr.tasks.misc

import java.nio.file.Files

import dagr.core.tasksystem.SimpleInJvmTask
import dagr.tasks.FilePath

/**
  * Simple tasks that deletes one or more extent files.
  */
class DeleteFiles(val paths: FilePath*) extends SimpleInJvmTask {
  withName("Delete_" + paths.size + "_Files")

  override def run(): Unit = paths.foreach(Files.deleteIfExists)
}
