package dagr.core.util

import java.lang.reflect.{Constructor, InvocationTargetException}

import dagr.core.cmdline._

import scala.collection.mutable
import scala.reflect.runtime.universe._
import scala.reflect.runtime.{universe => ru}

/**
  * Class to manage the process of inspecting another class and it's constructor(s) and then
  * assembling the necessary information to build an instance reflectively.
  *
  * Three classes work together to manage this process; more functionality can be added but
  * will generally require subclassing 2-3 of these classes:
  *   1. ReflectiveBuilder extracts constructor and parameter information and manages building the object
  *   2. ArgumentLookup manages the information about arguments, their names etc.
  *   3. Argument contains detailed information on a single argument
  */
class ReflectiveBuilder[T](val clazz: Class[T]) {
  /** A type which can be narrowed in subclasses. Roughly equivalent to adding a type parameter to the class. */
  type ArgumentDescription <: Argument
  protected val mirror: ru.Mirror = ru.runtimeMirror(clazz.getClassLoader)
  protected val clazzSymbol   = mirror.classSymbol(clazz)
  protected val typeSignature = clazzSymbol.typeSignature
  protected val constructor   = findConstructor
  protected val jConstructor  = toJavaConstructor(constructor)
  lazy val argumentLookup = new ArgumentLookup[ArgumentDescription]()
  extractParameterInformation(constructor)

  /**
    * Finds the appropriate constructor to reflect on. Either the single constructor, or when there are multiple
    * the one annotated with @CLPConstructor, or failing that the first from the list.
    */
  private def findConstructor : MethodSymbol = pickConstructor {
    typeSignature.members.toSeq.filter(p => p.isConstructor && p.owner == clazzSymbol && p.isPublic).map(_.asMethod)
  }

  /** Picks the constructor that should be used. Can be overridden by child classes. */
  protected def pickConstructor(constructors: Seq[MethodSymbol]) : MethodSymbol = {
    if (constructors.size == 1) constructors.head
    else throw new IllegalArgumentException(s"${clazz.getSimpleName} must have 1 public constructor, but has ${constructors.size}.")
  }

  /** Uses some horribly hacky reflection to grab the Java constructor object from the scala one. */
  private def toJavaConstructor(ctor : MethodSymbol) : Constructor[_] = {
    val constructorToJava = mirror.classSymbol(mirror.getClass).typeSignature.member(scala.reflect.runtime.universe.TermName("constructorToJava")).asMethod
    mirror.reflect(mirror: AnyRef).reflectMethod(constructorToJava).apply(ctor).asInstanceOf[Constructor[_]]
  }

  /**
    * Extracts information about the parameters to the provided method (a constructor), their types, their concrete
    * implementation classes, etc.  Populates the information into the instance level [[ArgumentLookup]] object
    * for easy querying.
    *
    * @param constructor a method to be examined - for now always a constructor
    */
  protected def extractParameterInformation(constructor : MethodSymbol): Unit = {
    if (constructor.paramLists.size > 1) throw new IllegalArgumentException("Constructors with more than one parameter list are not supported.")

    // More reflection objects that are needed to extract everything
    val hasCompanion     = clazzSymbol.companion.isModule
    val companionObject  = if (hasCompanion) Some(clazzSymbol.companion.asModule) else None
    val instanceMirror   = if (hasCompanion) Some(mirror reflect (mirror reflectModule companionObject.get).instance) else None
    val companionMembers = if (hasCompanion) Some(instanceMirror.get.symbol.typeSignature) else None

    constructor.paramLists.head.zipWithIndex.foreach { case (param, index) =>
      val name = param.name.toString
      val paramType = param.typeSignature
      if (paramType.typeArgs.size > 1) throw new CommandLineException(s"Parameter $name has multiple generic types and that is not currently supported")

      val paramClass = ReflectionUtil.typeToClass(paramType)
      val paramUnitType = if (paramType.typeArgs.size == 1) paramType.typeArgs.head else paramType
      val paramUnitClass = ReflectionUtil.typeToClass(paramUnitType)
      val paramPrimitiveClass = ReflectionUtil.ifPrimitiveThenWrapper(paramUnitClass)

      val paramTypeName = paramUnitType match {
        case ref: TypeRef => ref.sym.name.toString
        case _ => paramType.typeSymbol.asClass.name.decodedName.toString
      }

      // Sort out the default argument if one exists
      var defaultValue : Option[_] = None
      if (hasCompanion) {
        val defarg = companionMembers.get.member(TermName("$lessinit$greater$default$" + (index + 1)))
        if (defarg != NoSymbol) defaultValue = Option(instanceMirror.get.reflectMethod(defarg.asMethod)())
      }

      val jParam = jConstructor.getParameters()(index)

      this.argumentLookup add buildArgument(
        param=param, declaringClass=clazz, index=index, name=name, defaultValue=defaultValue,
        argumentType = paramClass, unitType=paramUnitClass, constructableType = paramPrimitiveClass, typeName=paramTypeName
      )
    }
  }

  /** Builds an instance of Argument (or sub-class thereof) from the provided values. */
  protected def buildArgument(param: Symbol,
                              declaringClass: Class[_],
                              index: Int,
                              name: String,
                              defaultValue: Option[Any],
                              argumentType: Class[_],
                              unitType: Class[_],
                              constructableType: Class[_],
                              typeName : String) : ArgumentDescription = {
    new Argument(
      declaringClass=clazz, index=index, name=name, defaultValue=defaultValue,
      argumentType = argumentType, unitType=unitType, constructableType = constructableType, typeName=typeName
    ).asInstanceOf[ArgumentDescription]
  }

  /** Constructs a new instance of type T with the parameter values set on the builder (including defaults). */
  def build() : T = {
    val xs = argumentLookup.ordered.filterNot(arg => arg.hasValue).map(arg => arg.name)
    if (xs.nonEmpty) {
      throw new IllegalStateException(s"Arguments not set: ${xs.mkString(", ")}")
    }

    build(argumentLookup.ordered.map(_.value.get))
  }

  /** Constructs a new instance of type T with the parameter values supplied. */
  def build(params: Seq[_]) : T = {
    val ps = params.map(_.asInstanceOf[Object])
    try {
      jConstructor.newInstance(ps: _*).asInstanceOf[T]
    }
    catch {
      case ite: InvocationTargetException => throw ite.getTargetException
    }
  }

  /** Attempts to build an instance using only default values. Will fail if any argument does not have a default. */
  def buildDefault() : T = {
    build(this.argumentLookup.ordered.map(a => a.value.get))
  }
}


/**
  * Class that encapsulates the information about the arguments to a CommandLineTask and provides
  * various ways to access the data.
  */
class ArgumentLookup[ArgType <: Argument](args: ArgType*) {
  protected val argumentDefinitions = mutable.ListBuffer[ArgType]()
  protected val byFieldName = new mutable.HashMap[String,ArgType]()

  // Add the arguments provided in the constructor
  args.foreach(add)

  /** Adds a new argument definition to the argument lookup. */
  def add(arg: ArgType): Unit = {
    // Then add it to the other collections
    argumentDefinitions.append(arg)
    byFieldName(arg.name)  = arg
  }

  /** Returns a view over the list of argument definitions for easy filtering/querying/mapping. */
  def view:Seq[ArgType] = this.argumentDefinitions.view

  /** Returns the full set of argument definitions ordered by their indices. */
  def ordered : Seq[ArgType] = argumentDefinitions.toList.sortBy(_.index)

  /** Returns the ArgumentDefinition, if one exists, for the provided field name. */
  def forField(fieldName: String) : Option[ArgType] = this.byFieldName.get(fieldName)
}


/**
  * Represents a single argument in a constructor.
  *
  * @param declaringClass the class that declares the constructor
  * @param index the 0-based index of this argument in the constructor argument list
  * @param name the name of this argument
  * @param defaultValue the default value if there is one. Note that this can be quite confusing for arguments that
  *                     themselves are Option types. E.g. an option type argument could have a default value of `None`
  *                     which would lead to `defaultValue` being `Some(None)`!
  * @param argumentType the type of the argument in the parameter list
  * @param unitType     either argumentType, or in the case that argumentType represents a container (collection, option)
  *                     the type of the elements
  * @param constructableType a type that can actually be constructed, e.g. in the case of primitive types this would
  *                          be the wrapper type
  * @param typeName the String name that identifies the unit type. May be the simple name of a class, or if a type
  *                 alias is used, the string name of the type alias
  */
class Argument(val declaringClass: Class[_],
               val index: Int,
               val name: String,
               val defaultValue: Option[Any],
               val argumentType: Class[_],
               val unitType: Class[_],
               val constructableType: Class[_],
               val typeName : String) {

  val isCollection: Boolean = ReflectionUtil.isCollectionClass(argumentType)

  /**
    * The value of this argument. Defaults to the provided default, *or* if no default is given and
    * the argument is an Option then auto-default to None, so that it's not necessary for a class
    * to always have `x: Option[Foo] = None`.
    */
  private var _value : Option[Any] = if (argumentType == classOf[Option[_]] && defaultValue.isEmpty) Some(None) else defaultValue

  /** Retrieves the current set value of the argument. */
  def value = this._value

  /** Sets the value of the argument.  Passing null will cause the value to be set to None. */
  def value_=(value: Any): Unit = {
    this._value = Option(value)
  }

  /** Gets the descriptive name of the Type of the field that should be used in printed output. */
  def getTypeDescription: String = typeName

  /** true if the field has been set, either by a default value or by the user */
  def hasValue: Boolean = _value match {
    case None               => false
    case x if isCollection  => !ReflectionUtil.isEmptyCollection(this._value.get)
    case _                  => true
  }
}
