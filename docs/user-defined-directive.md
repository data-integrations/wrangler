# User Defined Directive (UDD)

**U**ser **D**efined **D**irective (UDD) are easier and simpler way for users to build and integrate custom directives with wrangler. UDD framework allow users to develop, deploy and use data processing directives within the data preparation tool.

Building a custom directive involves implementing three simple methods :
  * **D** -- `define()` -- Define how the framework should interpret the arguments. 
  * **I** -- `initialise()` -- Invoked by the framework to initialise the custom directive with arguments parsed. 
  * **E** -- `execute()` -- Execute and apply your business logic for transforming the `Row`.
    
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

## How to write directive.

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
      builder.define("text", TokenType.COLUMN_NAME);
      return builder.build();
    }

    @Override
    public void initialise(Arguments args) throws DirectiveParseException {
      columnArgs = args.value("text");
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

Following is detailed explaination for the above code.

  * `@Plugin` annotation tells the framework the type of plugin this class represents.
  * `@Name` annotation provides the name of the plugin. For this type, the directive name and plugin name are the same.
  * `@Description` annotation provides a short description of the directive.
  * `UsageDefition define() { }` Defines the arguments that are expected by the directive.
  * `void initialise(Arguments args) { }` Invoked before configuring a directive with arguments parsed by the framework based on the `define()` methods `UsageDefintion`.
  * `execute(...) { }` Every `Row` from previous directive execution is passed to this plugin to execute.

## Extracting Loadable Directives

Sample code to show how loadable directives can be extracted from the recipe.

```
    ...
    String[] recipe = new String[] {
      "#pragma version 2.0;",
      "#pragma load-directives text-reverse, text-exchange;",
      "rename col1 col2",
      "parse-as-csv body , true",
      "!text-reverse :text;",
      "!test prop: { a='b', b=1.0, c=true};",
      "#pragma load-directives test-change,text-exchange, test1,test2,test3,test4;"
    };

    Compiler compiler = new RecipeCompiler();
    CompiledUnit compiled = compiler.compile(new MigrateToV2(recipe).migrate());
    Set<String> loadableDirectives = compiled.getLoadableDirectives();
    ...
```

## Parsing Recipe into list of executable directives

This block of code shows how one can parse recipe into a list of
directives that are executable in the `RecipePipeline`.

```
    ...
    String[] recipe = new String[] {
      "#pragma version 2.0;",
      "#pragma load-directives text-reverse, text-exchange;",
      "rename col1 col2",
      "parse-as-csv body , true"
    };

    CompositeDirectiveRegistry registry = new CompositeDirectiveRegistry(
      new SystemDirectiveRegistry()
    );

    RecipeParser parser = new GrammarBasedParser(new MigrateToV2(recipe).migrate(), registry);
    parser.initialize(null);
    List<Directive> directives = parser.parse();
    ...
```

## Related documentation

  * Information about Grammar [here](grammar/grammar-info.md)
  * Migrating directives from version 1.0 to version 2.0 [here](directive-migration.md)
  * Various `TokenType` supported by system [here](../api/src/main/java/co/cask/wrangler/api/parser/TokenType.java)
