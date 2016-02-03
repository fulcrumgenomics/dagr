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
package dagr.core.cmdline.parsing

import dagr.core.cmdline.CommandLineParserInternalException

import scala.collection.mutable

/**
  * Class that encapsulates the information about the arguments to a CommandLineTask and provides
  * various ways to access the data.
  */
class ArgumentLookup(args: ArgumentDefinition*) {
  private val argumentDefinitions = mutable.ListBuffer[ArgumentDefinition]()
  private val byShortName = new mutable.HashMap[String,ArgumentDefinition]()
  private val byLongName  = new mutable.HashMap[String,ArgumentDefinition]()
  private val byName      = new mutable.HashMap[String,ArgumentDefinition]()
  private val byFieldName = new mutable.HashMap[String,ArgumentDefinition]()

  // Add the arguments provided in the constructor
  args.foreach(add)

  /** Adds a new argument definition to the argument lookup. */
  def add(arg: ArgumentDefinition): Unit = {
    // First iterate over the names and ensure that none are taken yet
    arg.getNames.foreach { name =>
      if (byName.contains(name)) throw new CommandLineParserInternalException(s"$name has already been used.  Conflicting arguments are: '${arg.name}' and '${byName.get(name).get.name}'")
      byName(name) = arg
    }

    // Then add it to the other collections
    argumentDefinitions.append(arg)
    byShortName(arg.shortName)  = arg
    byLongName(arg.longName) = arg
    byFieldName(arg.name)  = arg
  }

  /** Returns a view over the list of argument definitions for easy filtering/querying/mapping. */
  def view:Seq[ArgumentDefinition] = this.argumentDefinitions.view

  /** Returns the full set of argument definitions ordered by their indices. */
  def ordered : Seq[ArgumentDefinition] = argumentDefinitions.toList.sortBy(_.index)

  /** Returns the full set of argument names known by the lookup, including all short and long names. */
  def names : Set[String] = byName.keySet.toSet // call to set to return an immutable copy

  /** Returns the ArgumentDefinition, if one exists, for the provided field name. */
  def forField(fieldName: String) : Option[ArgumentDefinition] = this.byFieldName.get(fieldName)

  /** Returns the ArgumentDefinition, if one exists, for the provided argument name. */
  def forArg(argName: String) : Option[ArgumentDefinition] = this.byName.get(argName)

}
