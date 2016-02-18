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
package dagr.sopt.cmdline.testing.fields

import dagr.commons.util.LogLevel

/** For testing the ability to find and filter classes with the CLP property */

object Fields {
  type PathToSomething = java.nio.file.Path
}
class WithList(var list: List[_])
class WithIntList(var list: List[Int])
class WithJavaCollection(var list: java.util.Collection[_])
class WithJavaSet(var set: java.util.Set[_])
class WithOption(var v: Option[_])
class WithIntOption(var v: Option[Int])
class WithInt(var v: Int)
class WithMap(var map: Map[_, _])
class WithPathToBamOption(var path: Option[Fields.PathToSomething])
class WithString(var s: String)
class WithPathToBam(var path: Fields.PathToSomething)
class WithStringParent(var s: String = "")
class WithStringChild(var t: String) extends WithStringParent
class WithEnum(var verbosity: LogLevel = LogLevel.Info)
class SetClass(var set: Set[_] = Set.empty)
class SeqClass(var seq: Seq[_] = Nil)
