/*
 * The MIT License
 *
 * Copyright (c) 2017 Fulcrum Genomics LLC
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

package dagr.sopt.util

import dagr.commons.util.UnitSpec

class MarkDownProcessorTest extends UnitSpec {
    val markdown =
    """
      |# Heading
      |
      |This is   a paragraph  with   lots of   white     space,
      |that   continues on a new line. And goes on and on and on and on forever and ever and ever.
      |
      |* list item with a really really long description that needs to get word-wrapped and not look stupid or silly.
      |* another list item
      |
      |> block quote with a quite a bit of text such that is needs to go over two lines
      |what with it's lazy continuation on a new line and everything.
      |
      |```info
      |        line of indented code
      |           line of deeper indented code
      |     outdented code
      |```
      |
      |        Paragraph with uneven indent that
      |           should get re-flowed and left-aligned
      |
      |1. numbered item 1
      |1. numbered item 2
      |1. numbered item 3
      |    - bullet item 1
      |    - bullet item 2
      |    - bullet item 3
      |        1. numbered sub-item 1
      |        1. numbered sub-item 2
      |        1. numbered sub-item 3
      |        ~~~shell
      |            code snipped inside an ordered list:
      |               further indented code
      |         outdented code
      |        ~~~
      |
      |A paragraph that contains some `code snippets` and _emphasized_ and __really__ **important** text. And then
      |also a contact link for [Nobody](mailto:nobody@fulcrumgenomics.com), a link to
      |[our website](http://www.fulcrumgenomics.com) and if that's not enough go to http://google.com and ask!
    """.stripMargin.trim

  "MarkDownProcessor" should "convert some stuff" in {
    val processor = new MarkDownProcessor
    val document  = processor.parse(markdown=markdown)
    val lines     = processor.toText(document)
    lines.foreach(System.out.println)

    if (false) {
      System.out.println("-------------------------------------------------------------------")
      System.out.println("-------------------------------------------------------------------")
      System.out.println(processor.toTree(document))
    }

    if (false) {
      System.out.println("-------------------------------------------------------------------")
      System.out.println("----------------------------- HTML --------------------------------")
      System.out.println("-------------------------------------------------------------------")
      System.out.println(processor.toHtml(document))
    }
  }
}
