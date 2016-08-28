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
package dagr.sopt.cmdline;

import java.lang.annotation.*;

/**
 * Used to annotate which fields of a CommandLineTask are options given at the command line.
 * If a command line call looks like "cmd option=foo x=y bar baz" the CommandLineTask
 * would have annotations on fields to handle the values of option and x. All options
 * must be in the form name=value on the command line. The java type of the option
 * will be inferred from the type of the field or from the generic type of the collection
 * if this option is allowed more than once. The type must be an enum or
 * have a constructor with a single String parameter.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.PARAMETER})
@Inherited
@Documented
public @interface ArgAnnotation {

    /**
     * The full name of the command-line argument.  Full names should be
     * prefixed on the command-line with a double dash (--).  If not specified
     * then default is used, which is to translate the field name into a GNU style
     * option name by breaking words on camel case, joining words back together with
     * hyphens, and converting to lower case (e.g. myThing => my-thing).  The
     * resulting value should be different than {@link #flag()}.
     * @return Selected full name, or "" to use the default.
     */
    String name() default "";

    /**
     * Specified short name of the command.  Single value short names should be
     * prefixed on the command-line with a single dash.  Multi-character short-names
     * will be treated like {@link #name()}. ArgAnnotation values can directly abut
     * single-char short names or be separated from them by a space.  The short name
     * should always have length less than or equal to {@link #name()} if the latter
     * is not empty and is not derived from the field name.  Regardless, they should
     * not have the same value.
     * @return Selected short name, or "" for none.
     */
    String flag() default "";

    /**
     * Documentation for the command-line argument.  Should appear when the
     * --help argument is specified.
     * @return Doc string associated with this command-line argument.
     */
    String doc() default "Undocumented option";

    /**
     * Array of option names that cannot be used in conjunction with this one.
     * If 2 options are mutually exclusive and are both required (i.e. are not Option types,
     * don't have a default value or are a Collection with minElements > 0) it will be
     * interpreted as one OR the other is required and an exception will only be thrown if
     * neither are specified.
     */
    String[] mutex() default {};

    /**
     * Is this an Option common to all command line programs.  If it is then it will only
     * be displayed in usage info when H or STDHELP is used to display usage.
     */
    boolean common() default false;

    /**
     * Does this option have special treatment in the argument parsing system.
     * Some examples are arguments_file and help, which have special behavior in the parser.
     * This is intended for documenting these options.
     */
    boolean special() default false;

    /**
     * Are the contents of this argument private and should be kept out of logs.
     * Examples of sensitive arguments are encryption and api keys.
     */
    boolean sensitive() default false;

    /**
     * If the argument is a collection, then the minimum number of arguments required.  This is ignored
     * for non-collection arguments.
     * */
    int minElements() default 1;

    /**
     * If the argument is a collection, then the maximum number of arguments required.  This is ignored
     * for non-collection arguments.
     * */
    int maxElements() default Integer.MAX_VALUE;

}
