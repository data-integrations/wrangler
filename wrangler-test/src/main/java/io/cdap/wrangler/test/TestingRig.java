package io.cdap.wrangler.test;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.wrangler.api.Directive;
import io.cdap.wrangler.api.DirectiveLoadException;
import io.cdap.wrangler.api.DirectiveParseException;
import io.cdap.wrangler.api.RecipeException;
import io.cdap.wrangler.api.RecipeParser;
import io.cdap.wrangler.api.RecipePipeline;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.executor.RecipePipelineExecutor;
import io.cdap.wrangler.parser.GrammarBasedParser;
import io.cdap.wrangler.parser.MigrateToV2;
import io.cdap.wrangler.proto.Contexts;
import io.cdap.wrangler.registry.CompositeDirectiveRegistry;
import io.cdap.wrangler.registry.SystemDirectiveRegistry;
import io.cdap.wrangler.test.api.TestRecipe;
import io.cdap.wrangler.test.api.TestRows;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public final class TestingRig {

  private TestingRig() { }

  public static void main(String[] args) throws Exception {
    List<String> directives = Arrays.asList(
      "aggregate-stats :size_col :duration_col total_size_mb total_time_sec"
    );

    List<String> input = Arrays.asList(
      "size_col,duration_col",
      "10MB,1s",
      "512KB,500ms",
      "2MB,2.5s"
    );

    List<String> output = execute(directives, input);

    for (String line : output) {
      System.out.println(line);
    }
  }

  public static List<String> execute(List<String> directives, List<String> inputLines) throws Exception {
    // Build TestRecipe
    TestRecipe recipe = new TestRecipe();
    for (String directive : directives) {
      recipe.add(directive);
    }

    // Parse header and rows
    String headerLine = inputLines.get(0);
    String[] headers = headerLine.split(",");
    TestRows testRows = new TestRows();

    for (int i = 1; i < inputLines.size(); i++) {
      String[] values = inputLines.get(i).split(",");
      Row row = new Row();
      for (int j = 0; j < headers.length; j++) {
        row.add(headers[j].trim(), values[j].trim());
      }
      testRows.add(row);
    }

    // Run through pipeline
    RecipePipeline pipeline = pipeline(io.cdap.directives.aggregates.AggregateStats.class, recipe);
    List<Row> outputRows = pipeline.execute(testRows.toList());

    // Convert output to CSV lines
    List<String> output = new ArrayList<>();
    if (!outputRows.isEmpty()) {
      // Add header
// Use the first row to get column names
Row first = outputRows.get(0);
int colCount = first.width();
List<String> columnNames = new ArrayList<>();
for (int i = 0; i < colCount; i++) {
  columnNames.add(first.getColumn(i));
}

// Add header
output.add(String.join(",", columnNames));

// Add each row’s values
for (Row row : outputRows) {
  List<String> values = new ArrayList<>();
  for (String col : columnNames) {
    values.add(String.valueOf(row.getValue(col)));
  }
  output.add(String.join(",", values));
}

      
    }

    return output;
  }

  public static RecipePipeline pipeline(Class<? extends Directive> directive, TestRecipe recipe)
    throws RecipeException, DirectiveParseException, DirectiveLoadException {
    verify(directive);
    List<String> packages = new ArrayList<>();
    packages.add(directive.getPackage().getName());

    CompositeDirectiveRegistry registry = new CompositeDirectiveRegistry(
      new SystemDirectiveRegistry(packages)
    );

    String migrate = new MigrateToV2(recipe.toArray()).migrate();
    RecipeParser parser = new GrammarBasedParser(Contexts.SYSTEM, migrate, registry);
    return new RecipePipelineExecutor(parser, null);
  }

  private static void verify(Class<? extends Directive> directive) {
    String classz = directive.getCanonicalName();
    Plugin plugin = directive.getAnnotation(Plugin.class);
    if (plugin == null || !plugin.type().equalsIgnoreCase(Directive.TYPE)) {
      throw new IllegalArgumentException(
        String.format("Class '%s' @Plugin annotation is not of type '%s'", classz, Directive.TYPE)
      );
    }

    Name name = directive.getAnnotation(Name.class);
    if (name == null) {
      throw new IllegalArgumentException(
        String.format("Class '%s' is missing @Name annotation.", classz)
      );
    }

    Description description = directive.getAnnotation(Description.class);
    if (description == null) {
      throw new IllegalArgumentException(
        String.format("Class '%s' is missing @Description annotation.", classz)
      );
    }
  }
}
