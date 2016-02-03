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
import dagr.core.cmdline._
import dagr.core.util.StringUtil

import scala.collection.mutable.ListBuffer
import scala.collection.{Seq, mutable}

/** Holds information about command line arguments (fields). */
private[parsing] class ArgumentDefinition(val declaringClass: Class[_],
                                          val index: Int,
                                          val name: String,
                                          val annotation: Option[ArgAnnotation],
                                          val defaultValue: Option[Any],
                                          val argumentType: Class[_],
                                          val unitType: Class[_],
                                          val constructableType: Class[_],
                                          val typeName : String
                                         )
{
  val isCollection: Boolean = ParsingUtil.isCollectionClass(argumentType)

  def omitFromCommandLine: Boolean = annotation.isEmpty

  if (annotation.isEmpty && defaultValue.isEmpty) {
    throw new IllegalStateException(s"Must have a default value for arguments without annotations. Name: '$name' Declaring class: '${declaringClass.getSimpleName}'")
  }

  val mutuallyExclusive: mutable.Set[String] = new mutable.HashSet[String]()
  annotation.foreach {_.mutex.foreach(mutuallyExclusive.add) }

  /**
    * The value of this argument. Defaults to the provided default, *or* if no default is given and
    * the argument is an Option then auto-default to None, so that it's not necessary for a task
    * to always have `x: Option[Foo] = None`.
    */
  var value : Option[Any] = if (argumentType == classOf[Option[_]] && defaultValue.isEmpty) Some(None) else defaultValue
  val optional: Boolean = omitFromCommandLine || isFlag || hasValue || (isCollection && annotation.get.minElements() == 0) || argumentType == classOf[Option[_]]

  /** true if the field was set by the user */
  var setByUser: Boolean = false // NB: only true when [[setArgument]] is called

  lazy val isSpecial: Boolean   = annotation.map(_.special()).getOrElse(false)
  lazy val isSensitive: Boolean = annotation.map(_.sensitive()).getOrElse(false)
  lazy val longName: String     = if (annotation.isDefined && annotation.get.name.nonEmpty) annotation.get.name else StringUtil.camelToGnu(name)
  lazy val shortName: String    = annotation.map(_.flag()).getOrElse("")
  lazy val doc: String          = annotation.map(_.doc()).getOrElse("")
  lazy val isCommon: Boolean    = annotation.map(_.common()).getOrElse(false)
  lazy val minElements: Int     = if (isCollection) annotation.map(_.minElements).getOrElse(1) else throw new IllegalStateException("Calling minElements on an argument that is not a collection.")
  lazy val maxElements: Int     = if (isCollection) annotation.map(_.maxElements).getOrElse(Integer.MAX_VALUE) else throw new IllegalStateException("Calling minElements on an argument that is not a collection.")

  def isFlag: Boolean = argumentType == classOf[java.lang.Boolean] || argumentType == classOf[Boolean]

  /** Gets the descriptive name of the Type of the field that should be used in printed output. */
  def getTypeDescription: String = typeName

  /** true if the field has been set, either by a default value or by the user */
  def hasValue: Boolean = {
    this.value.isDefined && (!isCollection || new SomeCollection(this.value.get).nonEmpty)
  }

  /** Sets the value of the argument.  Passing null will cause the value to be set to None. */
  private[parsing] def setFieldValue(value: Any): Unit = this.value = Option(value)

  @SuppressWarnings(Array("unchecked"))
  def setArgument(values: List[String]) {
    if (isFlag && values.isEmpty) {
      setFieldValue(true)  // TODO: shouldn't this pull and convert the first item from the list?
      return
    }
    if (setByUser) throw new IllegalStateException(s"Argument '$name' has already been set")
    if (!isCollection && values.size > 1) {
      throw new UserException(s"Argument '${this.getNames}' cannot be specified more than once.")
    }

    setFieldValue(ParsingUtil.constructFromString(this.argumentType, this.unitType, values:_*))
    this.setByUser = true
  }


  /** Gets the list of names by which this option can be passed on the command line. Will be length 1 or 2. */
  def getNames: List[String] = {
    val names: ListBuffer[String] = new ListBuffer[String]()
    if (!shortName.isEmpty) {
      names += shortName
    }
    if (!longName.isEmpty) {
      names += longName
    }

    names.toList
  }

  /** Utility class to hold the various collection types: [[Seq]], [[Set]], and [[java.util.Collection]] */
  private class SomeCollection(input: Any) {
    import scala.collection.{Seq, Set}

    private var seq: Option[Seq[_]] = None
    private var set: Option[Set[_]] = None
    private var collection: Option[java.util.Collection[_]] = None

    if (classOf[Seq[_]].isAssignableFrom(input.getClass)) seq = Some(input.asInstanceOf[Seq[_]])
    else if (classOf[Set[_]].isAssignableFrom(input.getClass)) set = Some(input.asInstanceOf[Set[_]])
    else if (classOf[java.util.Collection[_]].isAssignableFrom(input.getClass)) collection = Some(input.asInstanceOf[java.util.Collection[_]])
    else throw new IllegalArgumentException(s"Could not determine collection type: ${input.getClass.getCanonicalName}")

    def size: Int = {
      if (seq.isDefined) seq.get.size
      else if (set.isDefined) set.get.size
      else collection.get.size
    }

    def isEmpty: Boolean = {
      if (seq.isDefined) seq.get.isEmpty
      else if (set.isDefined) set.get.isEmpty
      else collection.get.isEmpty
    }

    def nonEmpty: Boolean = !isEmpty

    def getValues: Traversable[_] = {
      import scala.collection.JavaConversions._
      if (seq.isDefined) seq.get.toTraversable
      else if (set.isDefined) set.get.toTraversable
      else collection.get.toTraversable
    }
  }

  /** Validates that the required argument which is a collection is not empty and does not
    * have too many or too few values. */
  def validateCollection(): Unit = {
    val fullName: String = this.longName
    val c: SomeCollection = new SomeCollection(this.value.getOrElse(Nil))
    if (c.isEmpty) {
      throw new MissingArgumentException(s"Argument '$fullName' must be specified at least once.")
    }
    if (this.isCollection) {
      if (c.size < this.minElements) {
        throw new UserException(s"Argument '$fullName' was specified too few times (${c.size} < ${this.minElements})")
      }
      else if (this.maxElements < c.size) {
        throw new UserException(s"Argument '$fullName' was specified too many times (${this.minElements} < ${c.size})")
      }
    }
  }

  /**
    * Helper for pretty printing this option.
    *
    * @param value A value this argument was given
    * @return a string
    *
    */
  private def prettyNameValue(value: Any): String = {
    if (value != null) {
      if (isSensitive) {
        String.format("--%s ***********", longName)
      }
      else {
        String.format("--%s %s", longName, value.toString)
      }
    }
    else {
      ""
    }
  }

  /**
    * Returns a string representation of this argument and it's value(s) which would be valid if copied and pasted
    * back as a command line argument. Will throw an exception if called on an ArgumentDefinition that has not been
    * set.
    */
  def toCommandLineString: String = {
    val value: Any = this.value.get
    if (this.isCollection) {
      val collection = new SomeCollection(value)
      prettyNameValue(collection.getValues.mkString(" "))
    }
    else {
      prettyNameValue(value)
    }
  }
}
