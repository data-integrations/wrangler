# Introduction

CDAP provides extensive support for user defined directives (UDDs) as a way to specify custom processing for dataprep. CDAP UDDs can currently be implemented in Java.

The most extensive support is provided for Java functions. Java functions are also more efficient because they are implemented in the same language as CDAP and DataPrep and because additional interfaces and integrations with other CDAP subsystems are supported.

User Defined Directives, also known as UDD, allow you to create custom functions to transform records within CDAP DataPrep or a.k.a Wrangler. CDAP comes with a comprehensive library of functions. There are however some omissions, and some specific cases for which UDDs are the solution.

UDDs, similar to User-defined Functions (UDFs) have a long history of usefulness in SQL-derived languages and other data processing and query systems.  While the framework can be rich in their expressiveness, there's just no way they can anticipate all the things a developer wants to do.  Thus, the custom UDF has become commonplace in our data manipulation toolbox. In order to support customization or extension, CDAP now has the ability to build your own functions for manipulating data through UDDs.

Developing CDAP DataPrep UDDs by no means rocket science, and is an effective way of solving problems that could either be downright impossible, or does not meet your requirements or very awkward to solve. 

**U**ser **D**efined **D**irective (UDD) or Custom Directives are easier and simpler way for users to build and integrate custom directives with wrangler. UDD framework allow users to develop, deploy and use data processing directives
within the data preparation tool.

Building a custom directive involves implementing four simple methods :
  * **D** -- `define()` -- Define how the framework should interpret the arguments. 
  * **I** -- `initialize()` -- Invoked by the framework to initialize the custom directive with arguments parsed. 
  * **E** -- `execute()` -- Execute and apply your business logic for transforming the `Row`.
  * **D** -- `destroy()` -- Invoke by the framework to destroy any resources held by the directive. 
  
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
    [1] parse-as-csv :body ',' true
    [2] #pragma load-directives text-reverse,text-sanitization;
    [3] text-reverse :text
    [4] text-sanitization :description
```

More description of the above lines. 

  * `[2]` Dynamically loads the two UDDs as CDAP Plugins. 
  * `[3]` Uses the directive. 

## Developing UDD

There is one simple interface for developing your customized directive. The simple interface `io.cdap.wrangler.api.Directive` can be used for developing user defined directive.

### Simple API

Building a UDD with the simpler UDD API involves nothing more than writing a class with four function (evaluate) and few annotations. Here is an example:

```
@Plugin(type = UDD.Type)
@Name(SimpleUDD.NAME)
@Categories(categories = {"example", "simple"})
@Description("My first simple user defined directive")
public SimpleUDD implements Directive {
  public static final String NAME = "my-simple-udd";
  
  public UsageDefinition define() {
    ...
  }
  
  public void initialize(Arguments args) throws DirectiveParseException {
    ...
  }
  
  public List<Row> execute(List<Row> rows, ExecutorContext context) throws RecipeException, ErrorRowException {
    ...
  }
  
  public void destroy() {
    ...
  }
}
```

Following is detailed explaination for the above code.

  * `@Plugin` annotation tells the framework the type of plugin this class represents.
  * `@Name` annotation provides the name of the plugin. For this type, the directive name and plugin name are the same.
  * `@Description` annotation provides a short description of the directive.
  * `@Categories` annotation provides the category this directive belongs to.
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
 text-reverse :address
 text-reverse :id
```

Here is the implementation of the above UDD. 

```
@Plugin(type = UDD.Type)
@Name(TextReverse.NAME)
@Categories(categories = {"text-manipulation"})
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
  
  public List<Row> execute(List<Row> rows, ExecutorContext context) throws RecipeException, ErrorRowException {
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
  
  public void destroy() {
    // no-op
  }
}
```

### Code Walk Through

#### Annontations

Following annotations are required for the plugin. If any of these are missing, the plugin or the directive will not be loaded. 

* `@Plugin` defines the type of plugin it is. For all UDDs it's set to `UDD.Type`.
* `@Name` defines the name of the plugin and as well as the directive name. 
* `@Categories` defines one or more categories the directive belongs to.
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
 
## Macros and Wrangler Transform

Macros are be freely specified. One caveat while specifying macros is that the `#pragma load-directives` cannot be part of the macro. They should be specified in the plugin configuration itself. The reason is that we have to register the user defined directives prior to initializing and executing the transform. Macros are dereferenced only at the initialization and execution state. A configuration could look something like this.

```
#pragma load-directives my-udd-1, my-udd2;
${macroed_recipe}
#pragma load-directives my-udd3
${another_recipe}
```

## Lifecycle of Directive Methods

As you seen by now that the `Directive` interface exposes three methods `define()`, `initialize()` and `execute()`. These methods are invoked by the framework at different stages of processing. They also differ by different context in which they run like in `Transform` or in `Service`. So, let's talk about how and when they are called in different context. 

### Transform Context

A Transform is a CDAP plugin that is part of the pipeline configured and UDD or Custom Directives are embedded within such a transform. Each transform implements a few interfaces like `configurePipeline()`, `initialize()` and `transform()`. 

* When a pipeline is deployed to CDAP, during the deployment process, the `configurePipeline()` method is invoked by the CDAP Framework. In this method the plugin developer can validate the configurations, schema and such. In the case of UDD, the recipe containing the UDD is compiled and all the loadable directives are extracted. At this point none of the methods of UDD are invoked. Only the UDDs defined in the recipe are registered to be used in the pipeline deployed.
* When the pipeline is started, the plugins `initialize()` method is invoked. During this stage of the plugin, all the UDDs are loaded and initialized. At this point all the directives (user and system) are invoked -- at this point the `configure()` is called to get the definition of arguments for each UDD. Each directive within the recipe is parsed and then the respective UDD `initialize()` is invoked with the arguments parsed. These two methods are invoked only once before the start. If there are multiple instances of a directive being used within the recipe, this method is called the same number of times as the instance of directive in the recipe. 
* When the pipeline starts processing, each `StructuredRecord` into the transform invokes the UDD's `execute()` method. 

## Precedence of directive loading

Directives are loaded into the directive registry from the system and also from the user artifacts. So, now there are multiple cases where there might be conflicts, this section will describe how those conflicts are handled and what the users should expect in terms of behavior of the system when there are conflicts. Following are the scenarios when there could be conflict

* SYSTEM has a directive `x` pre-loaded, but a USER defines the same directive `x`. 
* User 1 has a USER directive `y`, and User 2 also has a different version or a completely different directive, but has the same name `y`. 
* SYSTEM has two directives with the same name `z`. 
* USER directive `k` has two different artifacts. 

## Field-level Lineage

Field-level lineage allows users to see which directives were applied to a specific column of data in a given timeframe. They can see how a column of data was generated and which other columns were produced from this column as well as how its values were manipulated.

### Labels

Every column involved in a directive must have one and only one associated label. These labels are: `{READ, ADD, DROP, RENAME, MODIFY}`
* **READ**: When the values of a column impact one or more other columns it is labeled as a READ column.
	* Ex1. `copy <source> <destination>`. In this case since the values of the entries of the source column are read in order to produce the destination column, the source column should be labeled as READ.
	* Ex2. `filter-row-if-matched <column> <regex>`. In this case since the values of the entries of the supplied column are read in order to filter the rows in the dataset, column should be labeled as READ. This is the case even though the supplied column is modified since its values are read.
* **ADD**: When a column is generated by the directive, this column is labeled as an ADD column.
	* Ex1. `copy <source> <destination>`. In this case since the destination is a new column that is generated by this directive, it should be labeled as ADD.
* **DROP**: When a column is dropped as a result of the directive, this column is labeled as a DROP column.
	* Ex1. `drop <column>[,<column>*]`. In this case since all the columns listed are dropped by this directive, all the listed columns should be labled as DROP columns.
* **RENAME**: When the name of a column is changed to another name, both the old and new name are labeled as RENAME columns. Note that neither column is labeled as ADD or DROP since no column is added or dropped, but instead a column's name is being replaced in place.
	* Ex1. `rename <old> <new>`. In this case since the name old is being replaced with the name new, both old and new should be labeled as RENAME. This is because one column's name is being changed/renamed from old to new.
	* EX2. `swap <column1> <column2>`. In this case since both the name column1 and the name column2 are simply being replaced with the other, both column1 and column2 should be labeled as RENAME. No records are being added or lost by this directive.
* **MODIFY**: When the values of a column's entries are potentially changed, but not read and impacting other columns, it should be labeled as a MODIFY column.
	* Ex1. `lowercase <column>`. In this case since the column doesn't impact any other column, and its values are potentially modified it should be labeled as MODIFY.
* Bonus: Rather then having to label every column if the columns are all READ, ADD, **or** MODIFY columns, the following can be used to replace the column name: `{"all columns", "all columns minus _ _ _ _", "all columns formatted %s_%d"}`. The first represents a case where all columns present in the dataset at the end of the directive can all be labeled the same. The second represents the case where all columns except for a space separated list of columns present in the dataset at the end of the directive can all be labeled the same. The third represents the case where all columns present at the end of the directive which follow the format string, supporting %s and %d, can all be labeled the same. Again this only works for READ, ADD, **or** MODIFY.
	* Ex1. `split-to-columns <column> <regex>`. In this case since all the newly produced columns will have names formatted `column_%d`, `all columns formatted column_%d` can be labeled ADD, rather than each individual new column.
	* Ex2. `parse-as-csv <column> <delimiter>`. In this case since all the columns present at the end of this directive will have been produced by this directive except for column itself, `all columns minus column` can be labeled ADD, rather than each individual new column.
	* Ex3. Custom directive: `lowercase-all`. This custom directive changes all the record values to lowercase. In this case all columns present at the end of this directive will have been modified by this directive, so `all columns` can be labeled MODIFY, rather than each individual column.



## Related documentation

  * Information about Grammar [here](grammar/grammar-info.md)
  * Custom Directive Implementation Internals [here](udd-internal.md)
  * Migrating directives from version 1.0 to version 2.0 [here](directive-migration.md)
  * Various `TokenType` supported by system [here](../api/src/main/java/io/cdap/wrangler/api/parser/TokenType.java)

