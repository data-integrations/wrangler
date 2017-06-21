/*
 * Copyright © 2016 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.wrangler;

import co.cask.cdap.api.data.schema.Schema;
import co.cask.wrangler.api.Record;
import co.cask.wrangler.executor.ParallelPipelineExecutor;
import co.cask.wrangler.executor.PipelineExecutor;
import co.cask.wrangler.parser.DirectivesBaseVisitor;
import co.cask.wrangler.parser.DirectivesLexer;
import co.cask.wrangler.parser.DirectivesParser;
import co.cask.wrangler.parser.DirectivesVisitor;
import co.cask.wrangler.parser.TextDirectives;
import co.cask.wrangler.steps.transformation.functions.DDL;
import com.google.common.io.Resources;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.StringReader;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * The test in this file is ignored as we use this only in cases when someone reports an issue with the file.
 */
public class InputFileTest {

  @Ignore
  @Test
  public void testWithFile() throws Exception {
    Path path = Paths.get("<path to file>");
    byte[] data = Files.readAllBytes(path);

    String[] directives = new String[] {
      "parse-as-csv body , true",
      "drop body",
      "drop Cabin",
      "drop Embarked",
      "fill-null-or-empty Age 0",
      "filter-row-if-true Fare < 8.06"
    };

    TextDirectives txtDirectives = new TextDirectives(directives);

    String lines = new String(data);
    List<Record> records1 = new ArrayList<>();
    List<Record> records2 = new ArrayList<>();
    for (String line : lines.split("\n")) {
      records1.add(new Record("body", line));
      records2.add(new Record("body", line));
    }

    long start = System.currentTimeMillis();
    PipelineExecutor executor1 = new PipelineExecutor();
    executor1.configure(txtDirectives, null);
    List<Record> results1 = executor1.execute(records1);
    long end = System.currentTimeMillis();
    System.out.println(
      String.format("Sequential : Records %d, Duration %d", results1.size(), end - start)
    );

    start = System.currentTimeMillis();
    ParallelPipelineExecutor executor2 = new ParallelPipelineExecutor();
    executor2.configure(txtDirectives, null);
    List<Record> results2 = executor2.execute(records2);
    end = System.currentTimeMillis();
    System.out.println(
      String.format("Parallel : Records %d, Duration %d", results2.size(), end - start)
    );


    Assert.assertTrue(true);
  }

  @Ignore
  @Test
  public void testSchemaParsing() throws Exception {
    URL schemaURL = getClass().getClassLoader().getResource("schema.avsc");
    Assert.assertNotNull(schemaURL);

    // Takes the avro schema and converts it to json.
    Schema schema = DDL.parse(Resources.toString(schemaURL, StandardCharsets.UTF_8));

    // Now we select the path : 'GetReservationRS.Reservation.PassengerReservation.Segments.Segment'
    schema = DDL.select(schema, "GetReservationRS.Reservation.PassengerReservation.Segments.Segment[0]");

    // Now, we drop 'Product'
    schema = DDL.drop(schema, "Product", "Air", "Vehicle", "Hotel", "General");

    Assert.assertNotNull(schema);
  }

  @Test
  public void testLexer() throws Exception {
    String[] directives = new String[] {
      "set-column :abc, :edf;",
      "send-to-error exp:{ window < 10 } ;",
      "parse-as-simple-date :col 'yyyy-mm-dd' :col 'test' :col2,:col4,:col9 10 exp:{test < 10};",
      "send-to-error exp:{ if(window < 10) { true } else {false} };",
      "!udd1 :col1 :col2 'test';"
    };

    String[] ruleNames = DirectivesLexer.ruleNames;
    for (int i = 0; i < directives.length; ++i) {
      CharStream stream = new ANTLRInputStream(new StringReader(directives[i]));
      DirectivesLexer lexer = new DirectivesLexer(stream);
      DirectivesParser parser = new DirectivesParser(new CommonTokenStream(lexer));
      ParseTree tree = parser.directives();
      parser.setBuildParseTree(false);
      DirectivesVisitor visitor = new MyVisitor();
      visitor.visit(tree);
    }

    Assert.assertTrue(true);
  }



  private final class ParsedDirective {

  }

  private final class MyVisitor extends DirectivesBaseVisitor<ParsedDirective> {
    /**
     * {@inheritDoc}
     * <p>
     * <p>The default implementation returns the result of calling
     * {@link #visitChildren} on {@code ctx}.</p>
     *
     * @param ctx
     */
    @Override
    public ParsedDirective visitDirective(DirectivesParser.DirectiveContext ctx) {
      System.out.println("\nDirective " + ctx.getText());
      return super.visitDirective(ctx);
    }

    /**
     * {@inheritDoc}
     * <p>
     * <p>The default implementation returns the result of calling
     * {@link #visitChildren} on {@code ctx}.</p>
     *
     * @param ctx
     */
    @Override
    public ParsedDirective visitNumberrange(DirectivesParser.NumberrangeContext ctx) {
      return super.visitNumberrange(ctx);
    }

    /**
     * {@inheritDoc}
     * <p>
     * <p>The default implementation returns the result of calling
     * {@link #visitChildren} on {@code ctx}.</p>
     *
     * @param ctx
     */
    @Override
    public ParsedDirective visitEcommand(DirectivesParser.EcommandContext ctx) {
      System.out.println("ECommand : " + ctx.Identifier().getText());
      return super.visitEcommand(ctx);
    }

    /**
     * {@inheritDoc}
     * <p>
     * <p>The default implementation returns the result of calling
     * {@link #visitChildren} on {@code ctx}.</p>
     *
     * @param ctx
     */
    @Override
    public ParsedDirective visitColumn(DirectivesParser.ColumnContext ctx) {
      System.out.println("Column : " + ctx.Column().getText());
      return new ParsedDirective();
    }

    /**
     * {@inheritDoc}
     * <p>
     * <p>The default implementation returns the result of calling
     * {@link #visitChildren} on {@code ctx}.</p>
     *
     * @param ctx
     */
    @Override
    public ParsedDirective visitText(DirectivesParser.TextContext ctx) {
      System.out.println("Text : " + ctx.String().getText());
      return super.visitText(ctx);
    }

    /**
     * {@inheritDoc}
     * <p>
     * <p>The default implementation returns the result of calling
     * {@link #visitChildren} on {@code ctx}.</p>
     *
     * @param ctx
     */
    @Override
    public ParsedDirective visitNumber(DirectivesParser.NumberContext ctx) {
      System.out.println("Number : " + ctx.Number().getText());
      return super.visitNumber(ctx);
    }

    /**
     * {@inheritDoc}
     * <p>
     * <p>The default implementation returns the result of calling
     * {@link #visitChildren} on {@code ctx}.</p>
     *
     * @param ctx
     */
    @Override
    public ParsedDirective visitBool(DirectivesParser.BoolContext ctx) {
      System.out.println("Boolean : " + ctx.Bool().getText());
      return super.visitBool(ctx);
    }

    /**
     * {@inheritDoc}
     * <p>
     * <p>The default implementation returns the result of calling
     * {@link #visitChildren} on {@code ctx}.</p>
     *
     * @param ctx
     */
    @Override
    public ParsedDirective visitCondition(DirectivesParser.ConditionContext ctx) {
      int childCount = ctx.getChildCount();
      StringBuilder sb = new StringBuilder();
      for (int i = 1; i < childCount - 1; ++i) {
        ParseTree child = ctx.getChild(i);
        sb.append(child.getText()).append(" ");
      }
      System.out.println("Condition " + sb.toString());
      return new ParsedDirective();
    }

    /**
     * {@inheritDoc}
     * <p>
     * <p>The default implementation returns the result of calling
     * {@link #visitChildren} on {@code ctx}.</p>
     *
     * @param ctx
     */
    @Override
    public ParsedDirective visitCommand(DirectivesParser.CommandContext ctx) {
      System.out.println("Command : " + ctx.getText());
      return super.visitCommand(ctx);
    }

    /**
     * {@inheritDoc}
     * <p>
     * <p>The default implementation returns the result of calling
     * {@link #visitChildren} on {@code ctx}.</p>
     *
     * @param ctx
     */
    @Override
    public ParsedDirective visitCollist(DirectivesParser.CollistContext ctx) {
      List<TerminalNode> columns = ctx.Column();
      System.out.println("Column List");
      for (TerminalNode column : columns) {
        System.out.println("  - " + column);
      }
      return new ParsedDirective();
    }

    /**
     * {@inheritDoc}
     * <p>
     * <p>The default implementation returns the result of calling
     * {@link #visitChildren} on {@code ctx}.</p>
     *
     * @param ctx
     */
    @Override
    public ParsedDirective visitNumberlist(DirectivesParser.NumberlistContext ctx) {
      System.out.println("Number List : " + ctx.getText());
      return super.visitNumberlist(ctx);
    }

    /**
     * {@inheritDoc}
     * <p>
     * <p>The default implementation returns the result of calling
     * {@link #visitChildren} on {@code ctx}.</p>
     *
     * @param ctx
     */
    @Override
    public ParsedDirective visitBoollist(DirectivesParser.BoollistContext ctx) {
      System.out.println("Boolean List : " + ctx.getText());
      return super.visitBoollist(ctx);
    }

    /**
     * {@inheritDoc}
     * <p>
     * <p>The default implementation returns the result of calling
     * {@link #visitChildren} on {@code ctx}.</p>
     *
     * @param ctx
     */
    @Override
    public ParsedDirective visitStringlist(DirectivesParser.StringlistContext ctx) {
      System.out.println("String List : " + ctx.getText());
      return super.visitStringlist(ctx);
    }

  }
}
