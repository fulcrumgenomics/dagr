package dagr.tasks.picard

import dagr.core.tasksystem.Pipe
import dagr.tasks.DataTypes.SamOrBam
import dagr.tasks.DagrDef
import DagrDef.PathToBam
import htsjdk.samtools.SAMFileHeader.SortOrder

import scala.collection.mutable.ListBuffer

class SortSam(val in: PathToBam, val out: PathToBam, val sortOrder: SortOrder) extends PicardTask with Pipe[SamOrBam,SamOrBam] {
  override protected def addPicardArgs(buffer: ListBuffer[Any]): Unit = {
    buffer += "I=" + in
    buffer += "O=" + out
    buffer += "SO=" + sortOrder
  }
}
