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

import com.vladsch.flexmark.ast._
import com.vladsch.flexmark.html.HtmlRenderer
import com.vladsch.flexmark.parser.{Parser, ParserEmulationProfile}
import com.vladsch.flexmark.util.options.MutableDataSet

import scala.collection.mutable.ListBuffer
import scala.collection.JavaConversions.iterableAsScalaIterable

/** Represents a chunk of text from MarkDown that is in the process of being laid out in plain text. */
case class Chunk(text: String, wrappable: Boolean, indent: Int, var gutter: Int = 0, var prefix: String = "") {
  def isEmpty: Boolean  = text.isEmpty
  def nonEmpty: Boolean = !isEmpty
  def withGutter(gutter: Int) : Chunk = { this.gutter = gutter; this }
  def withPrefix(prefix: String) : Chunk = { this.prefix = prefix; this }
  override def toString: String = text
}

object Chunk {
  def wrappable(indent:Int,  text: String)    = new Chunk(text=text, wrappable=true, indent=indent)
  def wrappable(indent:Int,  text: String*)   = text.map(t => new Chunk(text=t, wrappable=true, indent=indent))
  def unwrappable(indent: Int, text: String)  = new Chunk(text=text, wrappable=false, indent=indent)
  def unwrappable(indent: Int, text: String*) = text.map(t => new Chunk(text=t, wrappable=false, indent=indent))
  def empty = new Chunk(text="", wrappable=false, indent=0)
}

/**
 * Class for working with MarkDown documents and converting them to HTML or to line-wrapped
 * plain text.
 */
class MarkDownProcessor(lineLength: Int = 80, indentSize: Int = 2) {
  // Re-usable chunks to avoid creating them left and right
  private val NoChunk    = Seq.empty[Chunk]
  private val EmptyChunk = Seq(Chunk.empty)
  private val SpaceChunk = Seq(Chunk.unwrappable(0, " "))

  // A markdown parser
  private val (parser, htmlRenderer) = {
    val options  = new MutableDataSet()
    options.setFrom(ParserEmulationProfile.COMMONMARK)
    (Parser.builder(options).build, HtmlRenderer.builder(options).build)
  }

  /** Parses a MarkDown document into an AST. */
  def parse(markdown: String): Document = {
    parser.parse(markdown).asInstanceOf[Document]
  }

  /** Converts a MarkDown document to text. */
  def toText(document: Node): Seq[String] = {
    val lines = process(document, indent=0)
    lines.flatMap(indentAndWrap)
  }

  /** Converts a MarkDown document to HTML. */
  def toHtml(document: Node): String = this.htmlRenderer.render(document)

  /**
      * Recursive method that does the real work of converting a MarkDown document to text. Navigates
      * the AST recursively collapsing things down into text while maintaining the necessary indents,
      * blank lines after block elements (paragraphs, lists, code blocks, etc.).
      *
      * Handling of line-spacing before/after block elements is slightly tricky.  It is implemented so that
      * block elements (paragraphs, code blocks, lists) generate a sequence of Chunks that end in a blank line.
      * It is then up to the enclosing element to remove the trailing blank line if it is no longer needed.
      * For example a document consisting of 5 paragraphs will generate blank lines after each paragraph,
      * and then remove the last one at the end of the document; lists generate a blank line after the
      * end of the list - but this is removed by the enclosing list if it is a sub-list and not a top-level list.

      * @param node the current node being converted
      * @param indent the number of indents at the current position in the document
      * @param listPosition if inside an ordered list, what is the current list element number
      * @return a sequence of zero or more Chunks representing the content from this point down
      */
  private def process(node: Node, indent:Int, listPosition: Int = 0): Seq[Chunk] = node match {
    case document: Document =>
      withoutTrailingEmptyLine(document.getChildren.toSeq.flatMap(node => process(node, indent)))
    case heading : Heading =>
      val text = heading.getText.toString.trim
      Chunk.unwrappable(indent, text, "=" * text.length, "")
    case para : Paragraph =>
      val lines = para.getChildren.flatMap(c => process(c, indent))
      val text  = lines.mkString
      Chunk.wrappable(indent, text, "")
    case br: SoftLineBreak =>
      if (br.getNext != null && br.getNext.isInstanceOf[SoftLineBreak]) Seq.empty else SpaceChunk
    case list: BulletList =>
      list.getChildren.flatMap(c => process(c, indent+1)).toSeq ++ Seq(Chunk.empty)
    case item: BulletListItem =>
      val children = item.getChildren.toSeq
      val text     = process(children.head, indent)
      val bullet   = Chunk.wrappable(indent, "* " + text.mkString).withGutter(2)
      val subs     = children.tail.flatMap(c => withoutTrailingEmptyLine(process(c, indent + 1)))
      bullet +: subs
    case list: OrderedList =>
      list.getChildren.zipWithIndex.flatMap { case (c, i) => process(c, indent+1, i+1) }.toSeq ++ Seq(Chunk.empty)
    case item: OrderedListItem =>
      val children = item.getChildren.toSeq
      val text     = process(children.head, indent)
      val prefix   = listPosition + ". "
      val bullet   = Chunk.wrappable(indent, prefix + text.mkString).withGutter(prefix.length)
      val subs     = children.tail.flatMap(c => withoutTrailingEmptyLine(process(c, indent + 1)))
      bullet +: subs
    case code : FencedCodeBlock =>
      code.getContentChars.toString.lines.toSeq.flatMap(line => Chunk.unwrappable(indent+1, line)) ++ Seq(Chunk.empty)
    case quote: BlockQuote =>
      val lines = quote.getChildren.flatMap(c => process(c, indent))
      val text  = lines.mkString
      Seq(Chunk.wrappable(indent, text).withPrefix("> ")) ++ EmptyChunk
    case link: Link =>
      Seq(Chunk.wrappable(indent, s"${link.getText} (${link.getUrl})"))
    case delimited: DelimitedNodeImpl =>
      Seq(Chunk.wrappable(indent, delimited.getText.toString))
    case text : Text =>
      Seq(Chunk.wrappable(indent, node.getChars.toString))
    case other =>
      if (other.hasChildren) other.getChildren.flatMap(c => process(c, indent)).toSeq
      else Seq(Chunk.wrappable(indent, other.getChars.toString))
  }

  /**
      * Takes a chunk of text representing a paragraph, list item, line in a code block, etc. and
      * formats it for output by:
      *   - prepending the indenting space
      *   - re-wrapping the text if required (e.g. code block lines don't get wrapped)
      *   - applying any additional gutter (e.g. to follow-on lines in list items)
      */
  private def indentAndWrap(chunk: Chunk): Seq[String] = {
    val indent = " " * (indentSize * chunk.indent)
    val gutter = indent + (" " * chunk.gutter)

    if (chunk.wrappable) {
      val length = this.lineLength - indent.length - chunk.prefix.length
      val lines  = new ListBuffer[String]()
      val words  = chunk.text.split("\\s+").iterator.buffered
      while (words.hasNext) {
        val buffer = new StringBuilder
        buffer.append(chunk.prefix)
        while (words.hasNext && buffer.length + words.head.length < length) buffer.append(words.next()).append(" ")
        val prefix = if (lines.isEmpty) indent else gutter
        lines.append(prefix+ buffer.toString().trim)
        buffer.clear()
      }

      lines
    }
    else {
      Seq(indent + chunk.text)
    }
  }



  /** Removes a trailing empty line from a Seq[Line]. */
  private def withoutTrailingEmptyLine(lines: Seq[Chunk]): Seq[Chunk] = {
    if (lines.isEmpty || lines.last.nonEmpty) lines
    else lines.dropRight(1)
  }

  /** Debugging method to simply print out the tree-structure of a MarkDown document. */
  def toTree(node: Node): String = {
    /** Inner recursive method. */
    def subtree(node: Node, level: Int = 0, buffer: StringBuilder): Unit = {
      val indent = " " * (level * 2)
      buffer.append(indent).append(node.getClass.getSimpleName).append('\n')
      node.getChildren.foreach(child => subtree(child, level + 1, buffer))
    }

    val buffer = new StringBuilder
    subtree(node, 0, buffer)
    buffer.toString()
  }
}

