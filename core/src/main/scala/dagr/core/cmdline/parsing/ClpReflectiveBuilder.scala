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
import dagr.core.util._

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.reflect.runtime.{universe => ru}

/**
  * Extends [[ReflectiveBuilder]] to add support for the arguments annotated with the [[Arg]] annotation
  * and to use more specific logic to find the appropriate constructor to use to instantiate a [[CLP]] pipeline.
  *
  * @param clazz the class we wish to reflect upon
  * @tparam T the type for the class
  */
class ClpReflectiveBuilder[T](clazz: Class[T]) extends ReflectiveBuilder[T](clazz) {
  /** Override to use ClpArgument which also optionally holds the @[[dagr.core.cmdline.Arg]] annotation. */
  override type ArgumentDescription = ClpArgument
  /** Override to use the [[ClpArgumentLookup]]. Must be lazy since it is used in `add()` which is called from super's constructor. */
  override lazy val argumentLookup = new ClpArgumentLookup()


  /**
    * Implements the CLP constructor picking logic. Firstly if there is a single public constructor, use that.
    * Otherwise look for a constructor that has arguments with `@Arg` annotations on them.
    * Otherwise look for a constructor annotated with `@CLPConstructor`.
    */
  override protected def pickConstructor(constructors: Seq[ru.MethodSymbol]): ru.MethodSymbol = {
    if (constructors.size == 1) {
      constructors.head
    }
    else {
      val argTypeSymbol = mirror.typeOf[ArgAnnotation]
      val clpCtorSymbol = mirror.typeOf[CLPConstructorAnnotation]

      // Check to see if we can find a constructor with @Arg annotations on it's parameters, or @CLPConstructor on it
      val argOption = constructors.find(c => c.asMethod.paramLists.head.exists(p => p.annotations.exists(a => a.tree.tpe == argTypeSymbol)))
      val clpOption = constructors.find(p => p.annotations.exists(a => a.tree.tpe == clpCtorSymbol))

      argOption.getOrElse(clpOption.getOrElse(throw new IllegalStateException("FML"))).asMethod
    }
  }

  /** Builds an instance of ClpArgument with the provided values and the `@Arg` annotation. */
  override protected def buildArgument(param: ru.Symbol,
                                       declaringClass: Class[_],
                                       index: Int,
                                       name: String,
                                       defaultValue: Option[Any],
                                       argumentType: Class[_],
                                       unitType: Class[_],
                                       constructableType: Class[_],
                                       typeName: String): ArgumentDescription = {
    val jParam = jConstructor.getParameters()(index)
    val annotation = Option(jParam.getAnnotation(classOf[ArgAnnotation]))
    new ArgumentDescription(declaringClass, index, name, defaultValue, argumentType, unitType, constructableType, typeName, annotation)
  }
}

/**
  * Extension to the default ArgumentLookup to support a lot more names that come from the
  * Clp @Arg annotation.
  */
private[parsing] class ClpArgumentLookup(args: ClpArgument*) extends ArgumentLookup[ClpArgument](args:_*) {
  lazy private val byShortName = new mutable.HashMap[String,ClpArgument]()
  lazy private val byLongName  = new mutable.HashMap[String,ClpArgument]()
  lazy private val byName      = new mutable.HashMap[String,ClpArgument]()

  /** Adds a new argument definition to the argument lookup. */
  override def add(arg: ClpArgument): Unit = {
    // First iterate over the names and ensure that none are taken yet
    arg.names.foreach { name =>
      if (byName.contains(name)) throw new CommandLineParserInternalException(s"$name has already been used.  Conflicting arguments are: '${arg.name}' and '${byName.get(name).get.name}'")
      byName(name) = arg
    }

    // Then add it to the other collections
    super.add(arg)
    byShortName(arg.shortName)  = arg
    byLongName(arg.longName) = arg
  }

  /** Returns the full set of argument names known by the lookup, including all short and long names. */
  def names : Set[String] = byName.keySet.toSet // call to set to return an immutable copy

  /** Returns the ArgumentDefinition, if one exists, for the provided argument name. */
  def forArg(argName: String) : Option[ClpArgument] = this.byName.get(argName)
}

/**
  * Extension to [[Argument]] that holds onto the `@Arg` annotation and uses it to define several additional
  * methods.  Also enforces further constraints on the arguments, e.g. arguments without Arg annotations
  * must have default values as there is no other way for them to get values.
  */
private[parsing] class ClpArgument(declaringClass: Class[_],
                                   index: Int,
                                   name: String,
                                   defaultValue: Option[Any],
                                   argumentType: Class[_],
                                   unitType: Class[_],
                                   constructableType: Class[_],
                                   typeName : String,
                                   val annotation: Option[ArgAnnotation]
                                  ) extends Argument(declaringClass, index, name, defaultValue, argumentType, unitType, constructableType, typeName) {

  def omitFromCommandLine: Boolean = annotation.isEmpty

  if (annotation.isEmpty && defaultValue.isEmpty) {
    throw new IllegalStateException(s"Must have a default value for arguments without annotations. Name: '$name' Declaring class: '${declaringClass.getSimpleName}'")
  }

  val mutuallyExclusive: mutable.Set[String] = new mutable.HashSet[String]()
  annotation.foreach {_.mutex.foreach(mutuallyExclusive.add) }
  val optional: Boolean = omitFromCommandLine || isFlag || hasValue || (isCollection && annotation.get.minElements() == 0) || argumentType == classOf[Option[_]]

  /** true if the field was set by the user */
  private[parsing] var isSetByUser: Boolean = false // NB: only true when [[setArgument]] is called, vs. this.value =

  lazy val isSpecial: Boolean   = annotation.map(_.special()).getOrElse(false)
  lazy val isSensitive: Boolean = annotation.map(_.sensitive()).getOrElse(false)
  lazy val longName: String     = if (annotation.isDefined && annotation.get.name.nonEmpty) annotation.get.name else StringUtil.camelToGnu(name)
  lazy val shortName: String    = annotation.map(_.flag()).getOrElse("")
  lazy val doc: String          = annotation.map(_.doc()).getOrElse("")
  lazy val isCommon: Boolean    = annotation.map(_.common()).getOrElse(false)
  lazy val minElements: Int     = if (isCollection) annotation.map(_.minElements).getOrElse(1) else throw new IllegalStateException("Calling minElements on an argument that is not a collection.")
  lazy val maxElements: Int     = if (isCollection) annotation.map(_.maxElements).getOrElse(Integer.MAX_VALUE) else throw new IllegalStateException("Calling minElements on an argument that is not a collection.")

  /** Returns true if the type of the argument is boolean, and can thus be treated as a flag on the command line. */
  def isFlag: Boolean = argumentType == classOf[java.lang.Boolean] || argumentType == classOf[Boolean]

  /**
    * Sets the argument value(s) from an array of String values that appear on the command line. May
    * only be called once, after which repeated calls will throw an exception.
    */
  @SuppressWarnings(Array("unchecked"))
  def setArgument(values: List[String]) : Unit = {
    if (isFlag && values.isEmpty) {
      this.value = true
    }
    else if (isSetByUser) throw new IllegalStateException(s"Argument '$name' has already been set")
    else if (!isCollection && values.size > 1) {
      throw new UserException(s"Argument '${this.names}' cannot be specified more than once.")
    }
    else {
      value = ParsingUtil.constructFromString(this.argumentType, this.unitType, values:_*)
      this.isSetByUser = true
    }
  }

  /** Gets the list of names by which this option can be passed on the command line. Will be length 1 or 2. */
  def names: List[String] = {
    val names: ListBuffer[String] = new ListBuffer[String]()
    if (!shortName.isEmpty) names += shortName
    if (!longName.isEmpty)  names += longName
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

