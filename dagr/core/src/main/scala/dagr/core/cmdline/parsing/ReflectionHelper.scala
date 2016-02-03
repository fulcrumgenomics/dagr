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

import java.lang.reflect.{InvocationTargetException, Constructor}

import dagr.core.cmdline._
import dagr.core.util.ReflectionUtil

import scala.reflect.runtime.universe._
import scala.reflect.runtime.{universe => ru}

/**
  * Helps with the reflection required in order to find, examine and eventually invoke a constructor
  * on a class with named parameters, with or without defaults, and annotated with @Arg annotations.
  *
  * @param clazz the class we wish to reflect upon
  * @tparam T the type for the class
  */
class ReflectionHelper[T](val clazz: Class[T]) {
  private val mirror: ru.Mirror = ru.runtimeMirror(clazz.getClassLoader)
  private val clazzSymbol   = mirror.classSymbol(clazz)
  private val typeSignature = clazzSymbol.typeSignature
  val argumentLookup = new ArgumentLookup()

  private val constructor   = findConstructor
  private val jConstructor  = toJavaConstructor(constructor)
  mirror.classSymbol(mirror.getClass).typeSignature.member(scala.reflect.runtime.universe.TermName("constructorToJava"))
  extractParameterInformation(constructor)

  /**
    * Finds the appropriate constructor to relfect on. Either the single constructor, or when there are multiple
    * the one annotated with @CLPConstructor, or failing that the first from the list.
    */
  private def findConstructor : MethodSymbol = {
    val constructors = typeSignature.members.toSeq.filter(p => p.isConstructor && p.owner == clazzSymbol)
    if (constructors.size == 1) {
      constructors.head.asMethod
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

  /** Uses some horribly hacky reflection to grab the Java constructor object from the scala one. */
  private def toJavaConstructor(ctor : MethodSymbol) : Constructor[_] = {
    val constructorToJava = mirror.classSymbol(mirror.getClass).typeSignature.member(scala.reflect.runtime.universe.TermName("constructorToJava")).asMethod
    mirror.reflect(mirror: AnyRef).reflectMethod(constructorToJava).apply(ctor).asInstanceOf[Constructor[_]]
  }

  /**
    * Extracts information about the parameters to the provided method (a constructor), their types, their concrete
    * implementation classes, and the [[Arg]] annotations on the parameters.  Populates the information into the
    * instance level [[ArgumentLookup]] object for easy querying.
    *

    * @param ctor a method to be examined - for now always a constructor
    */
  private def extractParameterInformation(ctor : MethodSymbol): Unit = {
    if (ctor.paramLists.size > 1) throw new IllegalArgumentException("Constructors with more than one parameter list are not supported.")

    // More reflection objects that are needed to extract everything
    val hasCompanion = clazzSymbol.companion.isModule
    val companionObject = if (hasCompanion) Some(clazzSymbol.companion.asModule) else None
    val instanceMirror   = if (hasCompanion) Some(mirror reflect (mirror reflectModule companionObject.get).instance) else None
    val companionMembers = if (hasCompanion) Some(instanceMirror.get.symbol.typeSignature) else None

    ctor.paramLists.head.zipWithIndex.foreach { case (param, index) =>
      val name = param.name.toString
      val paramType = param.typeSignature
      if (paramType.typeArgs.size > 1) throw new CommandLineException(s"Parameter $name has multiple generic types and that is not currently supported")

      val paramClass = typeToClass(paramType)
      val paramUnitType = if (paramType.typeArgs.size == 1) paramType.typeArgs.head else paramType
      val paramUnitClass = typeToClass(paramUnitType)
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
      val annotation = Option(jParam.getAnnotation(classOf[ArgAnnotation]))

      this.argumentLookup add new ArgumentDefinition(
        declaringClass=clazz, index=index, name=name, annotation=annotation, defaultValue=defaultValue,
        argumentType = paramClass, unitType=paramUnitClass, constructableType = paramPrimitiveClass, typeName=paramTypeName
      )
    }
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

  /** Attempts to buid an instance using only default values. Will fail if any argument does not have a default. */
  def buildDefault() : T = {
    build(this.argumentLookup.ordered.map(a => a.value.get))
  }

  /** Converts from a Type object to a Class[_]. */
  private def typeToClass(typ : Type) : Class[_] = {
    if (typ.typeSymbol.asClass == typeOf[Any].typeSymbol.asClass) classOf[Any]
    else if (typ.typeSymbol.asClass == typeOf[AnyRef].typeSymbol.asClass) classOf[AnyRef]
    else if (typ.typeSymbol.asClass == typeOf[AnyVal].typeSymbol.asClass) classOf[AnyVal]
    else mirror.runtimeClass(typ.typeSymbol.asClass)
  }
}
