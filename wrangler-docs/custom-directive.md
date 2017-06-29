# UDD or Custom Directives (UDD)

**U**ser **D**efined **D**irective (UDD) or Custom Directives are easier and simpler way
for users to build and integrate custom directives with wrangler.
UDD framework allow users to develop, deploy and use data processing directives
within the data preparation tool.

Building a custom directive involves implementing three simple methods :
  * **D** -- `define()` -- Define how the framework should interpret the arguments. 
  * **I** -- `initialize()` -- Invoked by the framework to initialise the custom directive with arguments parsed. 
  * **E** -- `execute()` -- Execute and apply your business logic for transforming the `Row`.

# Steps to Build a directive

  * Clone the [example repository from github](https://github.com/hydrator/example-directive)

  ```javascript
  git clone git@github.com:hydrator/example-directive
  ```

  * Implementing three interfaces `define()`, `initialize()` and `execute()`.
  * Build a JAR (`mvn clean package`)
  * Deploy the JAR as a plugin into CDAP through UI or CLI or REST API
  * Use the plugin as follows:

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

## Construct of a directive.

Following is a sample implementation of the plugin that extends the interface [UDD](../api/src/main/java/co/cask/wrangler/api/UDD.java)

```
  @Plugin(type = Directive.Type)
  @Name("text-reverse")
  @Description("Reverses a string value of a column)
  public final class TextReverse implements Directive {
    private final ColumnName columnArgs;

    @Override
    public UsageDefinition define() {
      UsageDefinition.Builder builder = UsageDefintion.builder();
      builder.define("text", TokenType.COLUMN_NAME);
      return builder.build();
    }

    @Override
    public void initialize(Arguments args) throws DirectiveParseException {
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
  * `void initialize(Arguments args) { }` Invoked before configuring a directive with arguments parsed by the framework based on the `define()` methods `UsageDefintion`.
  * `execute(...) { }` Every `Row` from previous directive execution is passed to this plugin to execute.

## Migrating from Old Syntax to New Syntax

The recipe containing old syntax of invoking directives will automagically get
 transformed into new directives syntax. But, any new directives or custom directives
 have to specify and use new syntax.

### What's different in new syntax

There are some major difference in new syntax for invoking directives, all
 are listed below.

 * Semicolon(`;`) denotes a terminator for a single directive. E.g. Old : `parse-as-csv body , true` New : `parse-as-csv :body ',' true ;`
 * Column names are represented with a prefixed-colon. E.g. Old : `body`, New : `:body`
 * Text arguments are represented within quotes -- single or double. E.g. Old: `;`, New : `';'`
 * Expressions or conditions are now enclosed with a construct `exp: { condition or expression }`
 * Optional arguments are truly optional now.

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
