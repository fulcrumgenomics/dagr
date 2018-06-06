package dagr.core.custom

import dagr.core.cmdline.DagrCoreMain
import dagr.core.tasksystem.{NoOpTask, Pipeline}

class CustomDagrCoreMain extends DagrCoreMain {
  override protected def pipelineTransformer: Pipeline => Pipeline = {
    super.pipelineTransformer.andThen( originalPipeline => {
      originalPipeline.addDependent(new NoOpTask)
      originalPipeline
      }
    )
  }
}
