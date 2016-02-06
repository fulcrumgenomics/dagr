/*
 * The MIT License
 *
 * Copyright (c) 2015 Fulcrum Genomics LLC
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package dagr.core.tasksystem

import java.io.{BufferedWriter, FileWriter}
import java.nio.file.Path

import dagr.core.execsystem.{Resource, ResourceSet}

import scala.collection.mutable.ListBuffer
import scala.sys.process._

/** A task that can execute a set of commands in its own process, and does not generate any new tasks.
  */
trait ProcessTask extends UnitTask {
  /**
    * Abstract method that must be implemented by child classes to return a list or similar traversable
    * list of command line elements (command name and arguments) that form the command line to be run.
    * Individual values will be converted to Strings before being used by calling toString.
    */
  def args: Seq[Any]

  /** Returns a string representation of the command to be run by this task.
    *
    * @param setPipefail true if we are to fail if any command in a pipe fails, false if we only check the last command in a pipe or otherwise.
    * @return the command string.
    */
  private def getCommand(setPipefail: Boolean = true): String = {
    if (setPipefail) {
      "set -o pipefail > /dev/null && " + args.mkString(" ")
    }
    else {
      args.mkString(" ")
    }
  }

  /** Write the command to the script and get a process to run.
    *
    * @param script the script where the task's command should be stored.
    * @param logFile the log file where the task's stdout and stderr should be stored.
    * @param setPipefail true if we are to fail if any command in a pipe fails, false if we only check the last command in a pipe or otherwise.
    * @return a process to execute.
    */
  private[core] def getProcessBuilder(script: Path,
                                      logFile: Path,
                                      setPipefail: Boolean = true): ProcessBuilder = {
    val argv: String = getCommand(setPipefail = false)

    logger.debug("Executing call with argv: " + argv)
    logger.debug("Executing script: " + script)

    // write the script
    val writer = new BufferedWriter(new FileWriter(script.toFile))
    writer.write("#/bin/bash\n")
    if (setPipefail) writer.write("set -o pipefail\n")
    writer.write("set -e\n")
    // wrap the commands in a bash function to enable stdout/stderr piping to the log file
    writer.write("# Your command has been wrapped to facilitate piping to a log file\n")
    writer.write("run () {\n")
    writer.write(s"  $argv\n")
    writer.write(s"}\nrun &> $logFile;\n") // NB: this assumes argv is one command with no bash functions already defined
    writer.close()

    // NB: cannot put piping or anything else here as scala.sys.process.Process
    // does not execute in a shell, but instead passes anything after the first whitespace
    // as arguments to the command before that whitespace, which in this case is /bin/bash.
    val command = s"/bin/bash $script"
    logger.debug("Command is: " + command)

    // TODO: use ProgressLogger to capture input and output
    Process.apply(command)
  }

  /**
   *
   * @return the string representation of this task.
   */
  override def toString: String = s"Name: $name"
}
