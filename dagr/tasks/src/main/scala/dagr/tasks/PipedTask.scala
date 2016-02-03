package dagr.tasks

import dagr.core.tasksystem.ProcessTask

/**
  * ProcessTask that provides help for piping together multiple other ProcessTasks
  * to make a single executable command line.
  */
abstract class PipedTask extends ProcessTask {
  /** Returns the set of tasks that must be piped together. */
  protected def tasks : Seq[ProcessTask]

  /**
    * Abstract method that must be implemented by child classes to return a list or similar traversable
    * list of command line elements (command name and arguments) that form the command line to be run.
    * Individual values will be converted to Strings before being used by calling toString.
    */
  override def args: Seq[Any] = {
    tasks.foldLeft[List[Any]](Nil)((list, task) => list ++ list.headOption.map(x => "|") ++ task.args)
  }
}
