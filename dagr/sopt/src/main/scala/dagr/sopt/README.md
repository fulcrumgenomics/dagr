# Dagr's Option Parsing API

## Option Naming

### Short Names

Formats supported:

- `-<name><value>`
- `-<name>=<value>`
- `-<name> <value>`

Name must be a single character:`[A-Za-z0-9?]`

### Long names

Formats supported:

- `--<name>=<value>`
- `--<name> <value>`

Name must be `[A-Za-z0-9?][-A-Za-Z0-9?]*`

The following is not supported:

- `--<name><value>`

due to the abbreviations support described below.

## Prefixes
Prefixes of option names are supported, as long as the a prefix is not ambiguous, and either a ` ` or `=` delimiter is used.

These are equivalent:

- `--foobar <value>`
- `--fooba  <value>`
- `--foob  <value>`
- `--foo  <value>`

But if we have another option:

- `--footar <value>`

a [DuplicateOptionNameException](https://github.com/fulcrumgenomics/dagr/blob/master/src/main/scala/dagr/sopt/OptionParsingExceptions.scala#L48) will be thrown.

## Option Types

### Flag options

Flag options specify a boolean (true or false) value.  The value for the option can be omitted, and it will set
the option value to true.  The following are all equivalent:

- `-f`
- `-ftrue`
- `-f=true`
- `-fT`
- `-f=T`

We also allow `Yes` and `Y` to also specify `true`, and `No` and `n` to also specify `false`.  Both are case
insensitive.  We allow the following: `[T|True|F|False|Yes|Y|No|N]` with case insensitivity.

### Single-value options

Single value

### Multi-value options

Multiple values can specified via multiple invocations:

- `--<name>=<value> --<name>=<value> --<name>=<value>...`
- `--<name> <value> --<name> <value> --<name> <value>...`

or multiple values one after each other:

- `--<name>=<value> <value> <value>...`
- `--<name> <value> <value> <value>...`

### Caveats

- Both flag and single-value options may only be specified once on the command line.
- A multiple value option may be specified more than once.
- A single value option must have a value after its option name.
- A multiple value option must have one or more values after its option name.

