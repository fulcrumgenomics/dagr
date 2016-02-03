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
package dagr.core.util

import java.lang.reflect.Constructor

import scala.reflect.runtime.universe._
import scala.reflect.runtime.{universe => ru}

/**
  * Provides generic utility methods related to Java and Scala reflection.
  */
object ReflectionUtil {
  /**
    * Creates a new instance of various java collections.  Will inspect the type given to see
    * if there is a no-arg constructor and will use that if possible.  If there is no constructor
    * it will attempt to determine the appropriate concrete class to instantiate and return that.
    */
  def newJavaCollectionInstance[T <: java.util.Collection[_]](clazz: Class[_ <: T], args: Seq[AnyRef]): T = {
    val ctor = findConstructor(clazz)

    val collection : java.util.Collection[AnyRef] = if (ctor.isDefined) {
      ctor.get.newInstance(args: _*).asInstanceOf[java.util.Collection[AnyRef]]
    }
    else {
      if(classOf[java.util.Deque[_]].isAssignableFrom(clazz))        new java.util.ArrayDeque[AnyRef]()
      if(classOf[java.util.Queue[_]].isAssignableFrom(clazz))        new java.util.ArrayDeque[AnyRef]()
      if(classOf[java.util.List[_]].isAssignableFrom(clazz))         new java.util.ArrayList[AnyRef]()
      if(classOf[java.util.NavigableSet[_]].isAssignableFrom(clazz)) new java.util.TreeSet[AnyRef]()
      if(classOf[java.util.SortedSet[_]].isAssignableFrom(clazz))    new java.util.TreeSet[AnyRef]()
      if(classOf[java.util.Set[_]].isAssignableFrom(clazz))          new java.util.HashSet[AnyRef]()
      else /* treat it as any java.util.Collection */                new java.util.ArrayList[AnyRef]()
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

  /** Ensures that the wrapper class is used for primitive classes. */
  def ifPrimitiveThenWrapper(`type`: Class[_]): Class[_] = {
    // NB: it is important the primitive class returned has a string constructor value
    if (`type` eq Byte.getClass) return classOf[java.lang.Byte]
    if (`type` eq Short.getClass) return classOf[java.lang.Short]
    if (`type` eq Int.getClass) return classOf[java.lang.Integer]
    if (`type` eq Long.getClass) return classOf[java.lang.Long]
    if (`type` eq Float.getClass) return classOf[java.lang.Float]
    if (`type` eq Double.getClass) return classOf[java.lang.Double]
    if (`type` eq Boolean.getClass) return classOf[java.lang.Boolean]
    if (`type` eq java.lang.Byte.TYPE) return classOf[java.lang.Byte]
    if (`type` eq java.lang.Short.TYPE) return classOf[java.lang.Short]
    if (`type` eq java.lang.Integer.TYPE) return classOf[java.lang.Integer]
    if (`type` eq java.lang.Long.TYPE) return classOf[java.lang.Long]
    if (`type` eq java.lang.Float.TYPE) return classOf[java.lang.Float]
    if (`type` eq java.lang.Double.TYPE) return classOf[java.lang.Double]
    if (`type` eq java.lang.Boolean.TYPE) return classOf[java.lang.Boolean]
    `type`
  }

  /** Attempts to find a Java constructor with the given parameter types, and return it. Returns None if there is none. */
  private def findConstructor[_](typ: Class[_], parameterType: Class[_]*): Option[Constructor[_]] = {
    try { Option(typ.getConstructor(parameterType:_*)) }
    catch { case ex : NoSuchMethodException => None }
  }
}