package dagr.tasks.gatk

import dagr.tasks.DagrDef.PathToVcf
import scala.collection.mutable.ListBuffer

class DownsampleVariants(in: PathToVcf, output: PathToVcf, proportion: Double) extends Gatk4Task(walker = "SelectVariants") {

  var downsampleProportion = proportion
  override protected def addWalkerArgs(buffer: ListBuffer[Any]): Unit = {
    buffer.append("-V", in)
    buffer.append("-O", output)
    buffer.append("-factor", downsampleProportion)
  }
}
