/*
 * The MIT License
 *
 * Copyright (c) 2015-2016 Fulcrum Genomics LLC
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
package dagr.tasks

import java.nio.file.Path

import dagr.api.models.Memory

import scala.annotation.tailrec
import scala.collection.mutable.ListBuffer

object JarTask {
  /** Looks up the first super class that does not have "\$anon\$" in its name. */
  final def findCommandName(clazz: Class[_], errClassName: Option[String] = None): String = {
    findCommandNameHelper(Option(clazz), errClassName)
  }

  /** Looks up the first super class that does not have "\$anon\$" in its name. */
  @tailrec
  private final def findCommandNameHelper(clazzOption: Option[Class[_]], errClassName: Option[String] = None): String = {
    clazzOption match {
      case None => throw new RuntimeException(s"Could not determine the name of the ${errClassName.getOrElse("task")} class")
      case Some(clazz) =>
        if (!clazz.getName.contains("$anon$")) clazz.getSimpleName
        else findCommandNameHelper(Option(clazz.getSuperclass)) // the call to itself must be the last statement for tailrec to have a chance
    }
  }
}

trait JarTask {

  protected def jarArgs(jarPath: Path,
                        jvmArgs: Traversable[String] = Nil,
                        jvmProperties: collection.Map[String,String] = Map.empty,
                        jvmMemory: Memory,
                        useAdvancedGcOptions: Boolean = true): List[String] = {
    val args= ListBuffer[String]()
    args.append("java")
    args.appendAll(jvmArgs)
    for ((k,v) <- jvmProperties) args.append("-D" + k + "=" + v)
    if (useAdvancedGcOptions) args.append("-XX:GCTimeLimit=50", "-XX:GCHeapFreeLimit=10")
    args.append("-Xmx" + jvmMemory.mb)
    args.append("-jar", jarPath.toString)
    args.toList
  }
}
