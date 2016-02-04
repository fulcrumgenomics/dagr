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
package dagr.core.cmdline;

import java.lang.annotation.*;


/**
 * Annotates a command line program with various properties, such as usage (short and long),
 * as well as to which pipeline group it belongs.
 *
 * TODO: enforced that any CommandLineProgram has this property defined (use an annotation processor?).
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@Inherited
public @interface CLPAnnotation {
    /**
     * Should return a detailed description of the pipeline that can be down when requesting help on the
     * pipeline.  The first sentence (up to the first period) of the description should summarize the
     * pipeline's purpose in a way that it can be displayed in a list of pipelines.
     */
    String description();

    /** What group does the pipeline belong to, for grouping pipelines at the command line. */
    Class<? extends PipelineGroup> group() default Pipelines.class;

    /** Should this pipeline be hidden from the list shown on the command line. */
    boolean hidden() default false;
}
