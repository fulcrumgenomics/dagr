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
package dagr.commons.reflect

import java.lang.reflect.{Constructor, InvocationTargetException}
import java.nio.file.Path

import dagr.commons.io.PathUtil
import dagr.commons.CommonsDef._

import scala.annotation.ClassfileAnnotation
import scala.collection.mutable
import scala.reflect._
import scala.reflect.runtime.universe._
import scala.reflect.runtime.{universe => ru}
import scala.util.{Failure, Success, Try}

/** Base class for all exceptions thrown by the reflection util */
class ReflectionException(msg: String) extends RuntimeException(msg) {
  def this(msg: String, e: Exception) = {
    this(msg + ": " + e.getMessage + "\n" + e.getStackTrace.mkString("\n"))
  }
}

/**
  * Provides generic utility methods related to Java and Scala reflection.
  */
object ReflectionUtil {
  /** A runtime mirror for when it's necessary. */
  private val mirror: ru.Mirror = scala.reflect.runtime.currentMirror

  val SpecialEmptyOrNoneToken = ":none:"

  /** A cache of Type objects to their Class objects. */
  private val typeToClassCache = mutable.HashMap[Type, Class[_]]()

  /**
    * Creates a new instance of various java collections.  Will inspect the type given to see
    * if there is a no-arg constructor and will use that if possible.  If there is no constructor
    * it will attempt to determine the appropriate concrete class to instantiate and return that.
    */
  def newJavaCollectionInstance[T <: java.util.Collection[_]](clazz: Class[_ <: T], args: Seq[AnyRef]): T = {
    val collection : java.util.Collection[AnyRef] = findConstructor(clazz) match {
      case Some(ctor) =>
        ctor.newInstance(args: _*).asInstanceOf[java.util.Collection[AnyRef]]
      case None =>
        clazz match {
          case _ if classOf[java.util.Deque[_]].isAssignableFrom(clazz)        => new java.util.ArrayDeque[AnyRef]()
          case _ if classOf[java.util.Queue[_]].isAssignableFrom(clazz)        => new java.util.ArrayDeque[AnyRef]()
          case _ if classOf[java.util.List[_]].isAssignableFrom(clazz)         => new java.util.ArrayList[AnyRef]()
          case _ if classOf[java.util.NavigableSet[_]].isAssignableFrom(clazz) => new java.util.TreeSet[AnyRef]()
          case _ if classOf[java.util.SortedSet[_]].isAssignableFrom(clazz)    => new java.util.TreeSet[AnyRef]()
          case _ if classOf[java.util.Set[_]].isAssignableFrom(clazz)          => new java.util.HashSet[AnyRef]()
          case _ =>  /* treat it as any java.util.Collection */                   new java.util.ArrayList[AnyRef]()
        }
    }

    // add the args
    args.foreach(arg => collection.add(arg))
    // return the collection
    collection.asInstanceOf[T]
  }

  /** Instantiates a new Scala collection of the desired type with the provided arguments. */
  def newScalaCollection[T <: Iterable[_]](clazz: Class[T], args: Seq[Any]): T = {
    val clazzMirror: ru.Mirror = ru.runtimeMirror(clazz.getClassLoader)
    val clazzSymbol = clazzMirror.classSymbol(clazz)  //clazzMirror.classSymbol(tag.runtimeClass)
    val companionObject = clazzSymbol.companion.asModule
    val instanceMirror = clazzMirror reflect (clazzMirror reflectModule companionObject).instance
    val typeSignature = instanceMirror.symbol.typeSignature
    val name = "apply"
    val ctor = typeSignature.member(TermName(name)).asMethod

    if (ctor.isVarargs) instanceMirror.reflectMethod(ctor)(args).asInstanceOf[T]
    else instanceMirror.reflectMethod(ctor)(args:_*).asInstanceOf[T]
  }

  /** Returns true if the class is a subclass of [[Seq]] */
  def isSeqClass(clazz: Class[_]): Boolean = classOf[scala.collection.Seq[_]].isAssignableFrom(clazz)

  /** Returns true if the class is a subclass of [[Set]] */
  def isSetClass(clazz: Class[_]): Boolean = classOf[scala.collection.Set[_]].isAssignableFrom(clazz)

  /** Returns true if the class is a subclass of [[java.util.Collection]] */
  def isJavaCollectionClass(clazz: Class[_]): Boolean = classOf[java.util.Collection[_]].isAssignableFrom(clazz)

  /** Returns true if the class is subclass of a collection class ([[Seq]], [[Set]] or [[java.util.Collection]]) */
  def isCollectionClass(clazz: Class[_]): Boolean = isJavaCollectionClass(clazz) || isSeqClass(clazz) || isSetClass(clazz)

  /** True if the value is a collection class and is empty, false otherwise.  Throws a [[IllegalArgumentException]] if
    * the value is not a supported collection class ([[Seq]], [[Set]] or [[java.util.Collection]]).
    */
  def isEmptyCollection(value: Any): Boolean = {
    value.getClass match {
      case clazz if isJavaCollectionClass(clazz) => value.asInstanceOf[java.util.Collection[_]].isEmpty
      case clazz if isSeqClass(clazz) => value.asInstanceOf[Seq[_]].isEmpty
      case clazz if isSetClass(clazz) => value.asInstanceOf[Set[_]].isEmpty
      case _ => throw new IllegalArgumentException(s"Could not determine collection type of '${value.getClass.getSimpleName}")
    }
  }

  /** Ensures that the wrapper class is used for primitive classes. */
  def ifPrimitiveThenWrapper(typ: Class[_]): Class[_] = typ match {
    case _ if typ eq Byte.getClass          => classOf[java.lang.Byte]
    case _ if typ eq Short.getClass         => classOf[java.lang.Short]
    case _ if typ eq Int.getClass           => classOf[java.lang.Integer]
    case _ if typ eq Long.getClass          => classOf[java.lang.Long]
    case _ if typ eq Float.getClass         => classOf[java.lang.Float]
    case _ if typ eq Double.getClass        => classOf[java.lang.Double]
    case _ if typ eq Boolean.getClass       => classOf[java.lang.Boolean]
    case _ if typ eq java.lang.Byte.TYPE    => classOf[java.lang.Byte]
    case _ if typ eq java.lang.Short.TYPE   => classOf[java.lang.Short]
    case _ if typ eq java.lang.Integer.TYPE => classOf[java.lang.Integer]
    case _ if typ eq java.lang.Long.TYPE    => classOf[java.lang.Long]
    case _ if typ eq java.lang.Float.TYPE   => classOf[java.lang.Float]
    case _ if typ eq java.lang.Double.TYPE  => classOf[java.lang.Double]
    case _ if typ eq java.lang.Boolean.TYPE => classOf[java.lang.Boolean]
    case _ => typ
  }

  /** Attempts to find a Java constructor with the given parameter types, and return it. Returns None if there is none. */
  private def findConstructor[_](typ: Class[_], parameterType: Class[_]*): Option[Constructor[_]] = {
    try { Option(typ.getConstructor(parameterType:_*)) }
    catch { case ex : NoSuchMethodException => None }
  }

  /**
    * Returns true if the symbol (class, field, arg) is annotated with a scala classfile annotation
    * of the provided type, otherwise returns false.
    */
  def hasScalaAnnotation[A <: ClassfileAnnotation](sym: Symbol)(implicit evidence: ru.TypeTag[A]): Boolean = {
    val annotationType = mirror.typeOf[A]
    sym.annotations.exists(a => a.tree.tpe == annotationType)
  }

  /**
    * Returns true if the class of type C the symbol is annotated with a scala classfile annotation
    * of type A, otherwise returns false.
    */
  def hasScalaAnnotation[A <: ClassfileAnnotation, C <: Any](implicit evA: ru.TypeTag[A], evC: ru.TypeTag[C]): Boolean = {
    hasScalaAnnotation[A](mirror.typeOf[C].typeSymbol)
  }

  /**
    * Finds and instantiates a scala classfile annotation of type A, on a class of type C. This method is
    * preferred when, in code, the _type_ of class is known directly. Otherwise see the version which takes
    * an annotated symbol instead of a type.
    * */
  def findScalaAnnotation[A <: ClassfileAnnotation, C <: Any](implicit evA: ru.TypeTag[A], evC: ru.TypeTag[C]): Option[A] = {
    findScalaAnnotation[A](mirror.typeOf[C].typeSymbol)
  }

  /**
    * Finds and hydrates a static scala classfile annotation on the provided symbol. If the symbol isn't annotated
    * with the specified annotation, None is returned. If it is annotated then the annotation value is reified
    * and an instance returned.
    */
  def findScalaAnnotation[A <: ClassfileAnnotation](sym: Symbol)(implicit evidence: ru.TypeTag[A]): Option[A] = {
    val annotationType = mirror.typeOf[A]
    val annotationOption = sym.annotations.find(a => a.tree.tpe == annotationType)
    annotationOption.map(ann => {
      val clz = mirror.runtimeClass(ann.tree.tpe).asInstanceOf[Class[A]]
      val args = ann.tree.children.tail
      val helper = new ReflectiveBuilder[A](clz)

      args.foreach { arg =>
        val name  = arg.children.headOption.map(_.toString) getOrElse unreachable("Annotation param without name")
        val argdef = helper.argumentLookup.forField(name)   getOrElse unreachable("Args must be in lookup")
        var value = reifyAnnotationParameter(arg.children.tail.head)

        if (value.getClass.isArray) {
          value = copy(value.asInstanceOf[Array[Any]], argdef.unitType)
        }
        argdef.value = value
      }

      helper.build()
    })
  }

  /**
    * Hacky method to copy an Array[Any] into a correctly typed Array[T]. It would be great to eliminate the
    * need for this, I'm just not sure how.
    */
  private def copy[T](arr: Array[Any], cl: Class[T])(implicit evidence: ClassTag[T]) : Array[T]= {
    val out = java.lang.reflect.Array.newInstance(cl, arr.length).asInstanceOf[Array[T]]
    arr.indices.foreach(i => out(i) = arr(i).asInstanceOf[T])
    out
  }

  /**
    * Used by [[findScalaAnnotation]] to take an AST representing a `name=value` parameter assignment
    * in a classfile annotation. Ignores the name, and attempts to reify the value from the AST and return it
    * as an instance of the appropriate object
    *
    * @param tree an AST fragment representing a parameter name=value from a classfile annotation
    * @return
    */
  private def reifyAnnotationParameter(tree: Tree): Any = tree.productElement(0) match {
    case Constant(term:TermSymbol)   => termToEnum(term)
    case Constant(typ:Type)          => typeToClass(typ)
    case Constant(constant)          => constant
    case Ident(name) if name.toString == "Array" =>
      // If the thing looks like an array, get the AST for the list of values and reify each one
      tree.productElement(1).asInstanceOf[List[Tree]].map(p => reifyAnnotationParameter(p)).toArray[Any]
    case other: Any =>
      throw new RuntimeException("Don't know how to handle a: " + other)
  }

  /** Takes a TermSymbol representing a java Enum value, and returns the value. */
  private def termToEnum(term: TermSymbol): Any = {
    val enumClass =  if (term.owner.isModuleClass) {
      // This implies that the enum is an inner class, so we have to fetch it's class this weird way
      typeToClass(mirror.reflectClass(term.owner.companion.asClass).symbol.toType)
    }
    else {
      mirror.runtimeClass(term.owner.asClass)
    }
    enumClass.getDeclaredField(term.name.toString).get(null)
  }

  /** Converts a type tag to a class */
  def typeTagToClass[T: TypeTag]: Class[T] = typeToClass(typeTag[T].tpe).asInstanceOf[Class[T]]

  /**
    * Takes a Type symbol that represents a concrete class and returns the Class object associated with it.
    */
  def typeToClass(typ : Type) : Class[_] = {
    this.typeToClassCache.get(typ) match {
      case Some(c) => c
      case None    =>
        val c = {
          if      (typ.typeSymbol.asClass == typeOf[Any].typeSymbol.asClass)      classOf[Any]
          else if (typ.typeSymbol.asClass == typeOf[AnyRef].typeSymbol.asClass)   classOf[AnyRef]
          else if (typ.typeSymbol.asClass == typeOf[AnyVal].typeSymbol.asClass)   classOf[AnyVal]
          else if (typ.typeSymbol.asClass == typeOf[Array[_]].typeSymbol.asClass) classOf[Array[_]]
          else mirror.runtimeClass(typ.typeSymbol.asClass)
        }
        this.typeToClassCache(typ) = c
        c
    }
  }

  /** Gets the annotation of type `T` of the class `clazz` */
  def findJavaAnnotation[T <: java.lang.annotation.Annotation](clazz: Class[_], annotationClazz: Class[T]): Option[T] = {
    Option(clazz.getAnnotation(annotationClazz))
  }

  /**
    * Constructs one or more objects from Strings.
    *
    * @param resultType the type that must be assignable to, from the return of this function (may be a collection or a single-valued type)
    * @param unitType either the same as resultType or when resultType is a collection, the type of elements in the collection
    * @param value one or more strings from which to construct objects.  More than one string should only be given for collections.
    * @return an object constructed from the string.
    */
  def constructFromString(resultType: Class[_], unitType: Class[_], value: String*): Try[Any] = Try {
    if (resultType != unitType && !ReflectionUtil.isCollectionClass(resultType) && !classOf[Option[_]].isAssignableFrom(resultType)) {
      throw new ReflectionException("Don't know how to make a " + resultType.getSimpleName)
    }

    try {
      typedValueFromString(resultType, unitType, value:_*).get
    }
    catch {
      case e: NoSuchMethodException =>
        throw new ReflectionException(s"Cannot find string ctor for '${unitType.getSimpleName}'", e)
      case e: InstantiationException =>
        throw new ReflectionException(s"Abstract class '${unitType.getSimpleName} cannot be used for an argument value type.", e)
      case e: IllegalAccessException =>
        throw new ReflectionException(s"String constructor for argument class ''${unitType.getSimpleName}'' must be public.", e)
      case e: InvocationTargetException =>
        throw new ReflectionException(
          s"Problem constructing '${unitType.getSimpleName}' from the string${plural(value.size)} '" +
            value.toList.mkString(", ") + "'."
        )
    }
  }

  /**
    * Attempts to construct one or more unitType values from Strings, and return them as the resultType. Handles
    * the creation of and packaging into collections as necessary.
    */
  private[reflect] def typedValueFromString(resultType: Class[_], unitType: Class[_], value: String*): Try[Any] = Try {
    if (ReflectionUtil.isCollectionClass(resultType) && resultType != unitType) {
      val typedValues = if (value.length == 1 && value.head.toLowerCase == SpecialEmptyOrNoneToken) {
        Seq.empty
      }
      else {
        value.map(v => buildUnitFromString(unitType, v).get).asInstanceOf[Seq[java.lang.Object]]
      }

      // Condition for the collection type
      if (ReflectionUtil.isJavaCollectionClass(resultType)) {
        try {
          ReflectionUtil.newJavaCollectionInstance(resultType.asInstanceOf[Class[java.util.Collection[AnyRef]]], typedValues)
        }
        catch {
          case e: IllegalArgumentException =>
            throw new ReflectionException(
              s"Collection of type '${resultType.getSimpleName}' cannot be constructed or auto-initialized with a known type."
            )
        }
      }
      else if (ReflectionUtil.isSeqClass(resultType) || ReflectionUtil.isSetClass(resultType)) {
        ReflectionUtil.newScalaCollection(resultType.asInstanceOf[Class[_ <: Iterable[_]]], typedValues)
      }
      else {
        throw new ReflectionException(s"Unknown collection type '${resultType.getSimpleName}'")
      }
    }
    else if (value.size != 1) {
      throw new ReflectionException(s"Expecting a single argument to convert to ${resultType.getSimpleName} but got ${value.size}")
    }
    else if (resultType == classOf[Option[_]] && value.head.toLowerCase == SpecialEmptyOrNoneToken) {
      None
    }
    else {
      buildUnitOrOptionFromString(resultType, unitType, value.head).get
    }
  }

  /**
    * Supports any class with a string constructor, [[Enum]], and [[java.nio.file.Path]].  If the class is an [[Option]],
    * it will either return `None` if `s == null`, or `Some` object wrapping the an object of the wrapped type.
    */
  private def buildUnitOrOptionFromString(resultType: Class[_], unitType: Class[_], value: String): Try[Any] = Try {
    val unit = buildUnitFromString(unitType, value).get
    if (resultType == classOf[Option[_]]) Option(unit)
    else unit
  }

  /** Attempts to construct a single value of unitType from the provided String and return it. */
  private[reflect] def buildUnitFromString(unitType: Class[_], value: String) : Try[Any] = Try {
    val clazz = ifPrimitiveThenWrapper(unitType)
    if (clazz.isEnum) {
      lazy val badArgumentString = s"'$value' is not a valid value for ${clazz.getSimpleName}. " +
        enumOptions(clazz.asInstanceOf[Class[_ <: Enum[_ <: Enum[_]]]]).get
      val maybeEnum = clazz.getEnumConstants.map(_.asInstanceOf[Enum[_]]).find(e => e.name() == value)
      maybeEnum.getOrElse(throw new ReflectionException(badArgumentString))
    }
    else if (clazz == classOf[Path]) {
      PathUtil.pathTo(value)
    }
    else if (clazz == classOf[Option[_]]) {
      // unitType shouldn't be an option, since it's supposed to the type *inside* any containers
      throw new ReflectionException(s"Cannot construct an '${unitType.getSimpleName}' from a string.  Is this an Option[Option[...]]?")
    }
    else if (clazz == classOf[java.lang.Object]) value
    else {
      Try {
        import java.lang.reflect.Constructor
        val ctor: Constructor[_] = clazz.getDeclaredConstructor(classOf[String])
        ctor.setAccessible(true)
        ctor.newInstance(value)
      }.getOrElse {
        val typ             = mirror.classSymbol(clazz).toType
        val companionMirror = mirror.reflectModule(typ.typeSymbol.companion.asModule)
        val companion       = companionMirror.instance
        companion.getClass.getMethod("apply", classOf[String]).invoke(companion, value)
      }
    }
  }

  /**
    * Returns the string "s" if x is greater than 1.
    *
    * @param x Value to test.
    * @return "s" if x is greater than one else "".
    */
  private def plural(x: Int) = if (x > 1) "s" else ""

  /** Gets all the options for an enumeration.
    */
  def enumOptions(clazz: Class[_ <: Enum[_ <: Enum[_]]]): Try[Seq[Enum[_ <: Enum[_]]]] = Try {
    // We assume that clazz is guaranteed to be a Class<? extends Enum>, thus
    // getEnumConstants() won't ever return a null.
    val enumConstants: Array[Enum[_ <: Enum[_]]] = clazz.getEnumConstants.asInstanceOf[Array[Enum[_ <: Enum[_]]]]
    if (enumConstants.length == 0) {
      throw new ReflectionException(s"Bad enum type '${clazz.getName}' with no options")
    }
    enumConstants
  }
}
