# UDD Internals
 
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
