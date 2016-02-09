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
package dagr

/**
 * Package object to store all the typedefs for various basic types that are
 * used throughout workflows.
 */
package object tasks {
  /////////////////////////////////////////////////////////////////////////////
  // Path-like typedefs that are used to hint at what the Path should be to
  /////////////////////////////////////////////////////////////////////////////

  /** Represents a path to a BAM (or SAM or CRAM) file. */
  type PathToBam = java.nio.file.Path

  /** Represents a path to an intervals file (IntervalList or BED). */
  type PathToIntervals = java.nio.file.Path

  /** Represents a path to a FASTQ file (optionally gzipped). */
  type PathToFastq = java.nio.file.Path

  /** Represents a path to a Reference FASTA file. */
  type PathToFasta = java.nio.file.Path

  /** Represents a path to a VCF/BCF/VCF.gz. */
  type PathToVcf = java.nio.file.Path

  /** Represents a full path including directories, that is intended to be used as a prefix for generating file paths. */
  type PathPrefix = java.nio.file.Path

  /** Represents a path to directory. */
  type DirPath = java.nio.file.Path

  /** Represents a path to a file (not a directory) that doesn't have a more specific type. */
  type FilePath = java.nio.file.Path

  /** A String that represents the prefix or basename of a filename. */
  type FilenamePrefix = String

  ///////////////////////////////////////////////////////////////////
  // Units of memory
  ///////////////////////////////////////////////////////////////////
  type Bytes = BigInt
  type Megabytes = BigInt
  type Gigabytes = BigInt

  /**
    * Object containing phantom types that define streaming/piping data types. These types are never meant
    * to be instantiated, but exist purely to allow type checking of tasks that are strung together
    * using pipes.
    */
  object DataTypes {
    // Developer note: all these classes have private default constructors so that they cannot be instantiated
    class SamOrBam private[DataTypes]()
    class Sam private() extends SamOrBam
    class Bam private() extends SamOrBam
    class Vcf private()
    class Fastq private()
  }
}
