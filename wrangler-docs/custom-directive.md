# Introduction

CDAP provides extensive support for user defined directives (UDDs) as a way to specify custom processing for dataprep. CDAP UDDs can currently be implemented in Java.

The most extensive support is provided for Java functions. Java functions are also more efficient because they are implemented in the same language as CDAP and DataPrep and because additional interfaces and integrations with other CDAP subsystems are supported.

User Defined Directives, also known as UDD, allow you to create custom functions to transform records within CDAP DataPrep or a.k.a Wrangler. CDAP comes with a comprehensive library of functions. There are however some omissions, and some specific cases for which UDDs are the solution.

UDDs, similar to User-defined Functions (UDFs) have a long history of usefulness in SQL-derived languages and other data processing and query systems.  While the framework can be rich in their expressiveness, there's just no way they can anticipate all the things a developer wants to do.  Thus, the custom UDF has become commonplace in our data manipulation toolbox. In order to support customization or extension, CDAP now has the ability to build your own functions for manipulating data through UDDs.

Developing CDAP DataPrep UDDs by no means rocket science, and is an effective way of solving problems that could either be downright impossible, or does not meet your requirements or very akward to solve. 

**U**ser **D**efined **D**irective (UDD) or Custom Directives are easier and simpler way for users to build and integrate custom directives with wrangler. UDD framework allow users to develop, deploy and use data processing directives
within the data preparation tool.

Building a custom directive involves implementing three simple methods :
  * **D** -- `define()` -- Define how the framework should interpret the arguments. 
  * **I** -- `initialize()` -- Invoked by the framework to initialize the custom directive with arguments parsed. 
  * **E** -- `execute()` -- Execute and apply your business logic for transforming the `Row`.
  
## Steps to Build a directive

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

## Developing UDD

There is one simple interface for developing your customized directive. The simple interface `co.cask.wrangler.api.Directive` can be used for developing user defined directive.

### Simple API

Building a UDD with the simpler UDD API involves nothing more than writing a class with three function (evaluate) and few annotations. Here is an example:

```
@Plugin(type = UDD.Type)
@Name(SimpleUDD.NAME)
@Description("My first simple user defined directive")
public SimpleUDD implements Directive {
  public static final String NAME = "my-simple-udd";
  
  public UsageDefinition define() {
    ...
  }
  
  public void initialize(Arguments args) throws DirectiveParseException {
    ...
  }
  
  public List<Row> execute(List<Row> rows, RecipeContext context) throws RecipeException, ErrorRowException {
    ...
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

### Testing a simple UDD

Because the UDD is simple three functions class, you can test it with regular testing tools, like JUnit.

```
public class SimpleUDDTest {

  @Test
  public void testSimpleUDD() throws Exception {
    TestRecipe recipe = new TestRecipe();
    recipe("parse-as-csv :body ',';");
    recipe("drop :body;");
    recipe("rename :body_1 :simpledata;");
    recipe("!my-simple-udd ...");
    
    TestRows rows = new TestRows();
    rows.add(new Row("body", "root,joltie,mars avenue"));
    RecipePipeline pipeline = TestingRig.pipeline(RowHash.class, recipe);
    List<Row> actual = pipeline.execute(rows.toList());
  }
}
```

### Building a UDD Plugin

There is nothing much to be done here, this example repository includes a maven POM file that is pre-configured for building the directive JAR. All that a developer does it build the project using the following command. 

```
  mvn clean package
```

This would generate two files

  * Artifact - `my-simple-udd-1.0-SNAPSHOT.jar`
  * Artifact Configuration `my-simple-udd-1.0-SNAPSHOT.json`
  
### Deploying Plugin

There are multiple ways the custom directive can be deployed to CDAP. The two popular ways are through using CDAP CLI (command line interface) and CDAP UI.

#### CDAP CLI

In order to deploy the directive through CLI. Start the CDAP CLI and use the `load artifact` command to load the plugin artifact into CDAP. 

```
$ $CDAP_HOME/bin/cdap cli
cdap > load artifact my-simple-udd-1.0-SNAPSHOT.jar config-file my-simple-udd-1.0-SNAPSHOT.json
```

#### CDAP UI
![alt text](https://github.com/hydrator/example-directive/blob/develop/docs/directive-plugin.gif "Logo Title Text 1")

## Example

I am going to walk through the creation of a user defined directive(udd) called `text-reverse` that takes one argument: Column Name -- it's the name of the column in a `Row` that needs to be reversed. The resulting row will have the Column Name specified in the input have reversed string of characters.

```
 !text-reverse :address
 !text-reverse :id
```

Here is the implementation of the above UDD. 

```
@Plugin(type = UDD.Type)
@Name(TextReverse.NAME)
@Description("Reverses the column value")
public final class TextReverse implements UDD {
  public static final String NAME = "text-reverse";
  private String column;
  
  public UsageDefinition define() {
    UsageDefinition.Builder builder = UsageDefinition.builder(NAME);
    builder.define("column", TokenType.COLUMN_NAME);
    return builder.build();
  }
  
  public void initialize(Arguments args) throws DirectiveParseException {
    this.column = ((ColumnName) args.value("column").value();
  }
  
  public List<Row> execute(List<Row> rows, RecipeContext context) throws RecipeException, ErrorRowException {
    for(Row row : rows) {
      int idx = row.find(column);
      if (idx != -1) {
        Object object = row.getValue(idx);
        if (object instanceof String) {
          String value = (String) object;
          row.setValue(idx, new StringBuffer(value).reverse().toString());
        }
      }
    }
    return rows;
  }
}
```

### Code Walk Through

#### Annontations

Following annotations are required for the plugin. If any of these are missing, the plugin or the directive will not be loaded. 

* `@Plugin` defines the type of plugin it is. For all UDDs it's set to `UDD.Type`.
* `@Name` defines the name of the plugin and as well as the directive name. 
* `@Description` provides a short description for the plugin and as well as for the directive. 

#### Call Pattern

The call pattern of UDD is the following :

* **DEFINE** : During configure time either in the CDAP Pipeline Transform or Data Prep Service, the `define()` method is invoked only once to retrieve the information of the usage. The usage defines the specification of the arguments that this directive is going to accept. In our example of `text-reverse`, the directive accepts only one argument and that is of type `TokenType.COLUMN_NAME`.
* **INITIALIZE** : During the initialization just before pumping in `Row`s through the directive, the `initialize()` method is invoked. This method is passed the arguments that are parsed by the system. It also provides the apportunity for the UDD writer to validate and throw exception if the value is not as expected.
* **EXECUTE** : Once the pipeline has been setup, the `Row` is passed into the `execute()` method to transform. 

### Testing

Following is the JUnit class that couldn't be any simpler. 

```
  @Test
  public void testBasicReverse() throws Exception {
    TestRecipe recipe = new TestRecipe();
    recipe.add("parse-as-csv :body ',';");
    recipe.add("set-headers :a,:b,:c;");
    recipe.add("text-reverse :b");

    TestRows rows = new TestRows();
    rows.add(new Row("body", "root,joltie,mars avenue"));
    rows.add(new Row("body", "joltie,root,venus blvd"));

    RecipePipeline pipeline = TestingRig.pipeline(TextReverse.class, recipe);
    List<Row> actual = pipeline.execute(rows.toList());

    Assert.assertEquals(2, actual.size());
    Assert.assertEquals("eitloj", actual.get(0).getValue("b"));
    Assert.assertEquals("toor", actual.get(1).getValue("b"));
  }
```

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
 
## Macros and Directives

There are use-cases where a macro is specified in a pipeline for the directive in a Wrangle transform. In that we wouldn't want to fail for not recognizing it's a macro and as well be able to register the dynamically loadable directives (plugins) during that phase. Failing to register the directives in the configure phase would make the directive unusable during initialize causing the entire pipeline to fail. So, the approach that is being taken is as follows

  * The `RecipeCompiler` recognizes the macros specified and skips them during the `configure` phase of a Wrangler Transform.
  * Compilation will generate a list of all loadable directives. These are all the directives that have been specified with `pragma load-directives` statement.
  * The dynamic loadable directives are then registered in the pipeline to be used.
  * A 64-bit unqiue id is generated for each directive that is loaded dynamically and the information is passed through the plugin properties into `initialize()`.
  * In `initialize()` the properties are retrieved and appropriate plugin id for the plugin name in the context of pipeline is extracted. Using that plugin id an instance of the class is created. 
  
All-in-All **Macros can be freely specified** in the Wrangler Transform plugin for the directive configuration.  

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
