# User Defined Directive (UDD)

User Defined Directive (UDD) allow users to develop, deploy and use
data processing directives within the data preparation tool.

# Syntax

```
    [1] #pragma version 2.0
    [2] parse-as-csv :body ',' true
    [3] #pragma load-directives text-reverse,text-sanatization
    [4] !text-reverse :text
    [5] !text-sanatization :description
```

More description of the above lines. 

  * `[1]` Specifies the version of directive grammar.
  * `[3]` Dynamically loads the two UDDs as CDAP Plugins. 
  * `[4]` Uses the directive. `!` specifies the directive as external or user defined.

## Example implementation of directive

Following is a sample implementation of the plugin.

```
  @Plugin(type = UDD.Type)
  @Name("text-reverse")
  @Description("Reverses a string value of a column)
  public final class TextReverse implements UDD {
    private final ColumnName columnArgs;

    @Override
    public UsageDefinition define() {
      UsageDefinition.Builder builder = UsageDefintion.builder();
      builder.define("col", TokenType.COLUMN_NAME);
      return builder.build();
    }

    @Override
    public void initialize(Arguments args) throws DirectiveParseException {
      columnArgs = args.value("col");
    }

    @Override
    public List<Row> execute(List<Row> rows, RecipeContext context)
      throws RecipeExecutionException, ErrorRowException {
      for(Row row : rows) {
        ...
      }
    }
  }
```

## Related documentation

  * Information about Grammar [here](grammar/grammar-info.md)

