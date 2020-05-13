package io.cdap.mapper;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.format.StructuredRecordStringConverter;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

/**
 * Tests exprression mapper.
 */
public class ExpressionEvaluatorTest {

  private String readTestFile(String filename) throws IOException {
    URL eMapUrl = getClass().getClassLoader().getResource(filename);
    StringWriter writer = new StringWriter();
    IOUtils.copy(eMapUrl.openStream(), writer, Charset.forName("UTF-8"));
    return writer.toString();
  }

  private void testSingleInOutWithMaps(String testIn, String testOut,
                                                   String testInData, String testMaps,
                                                   String expectedData) throws Exception {
    Schema inputSchema = Schema.parseJson(readTestFile(testIn));
    Schema outputSchema = Schema.parseJson(readTestFile(testOut));
    StructuredRecord record
      = StructuredRecordStringConverter.fromJsonString(readTestFile(testInData), inputSchema);

    ExpressionEvaluator em = new ExpressionEvaluator(readTestFile(testMaps));
    ExpressionEvaluator.Compiled compiled = em.compile();
    compiled.addOutput("out", outputSchema);
    compiled.addInput("in", record);
    ExpressionEvaluator.Result result = compiled.execute();
    Assert.assertNotNull(result);
    StructuredRecord expected = StructuredRecordStringConverter.fromJsonString(
      readTestFile(expectedData),
      result.getSchema("out")
    );
    StructuredRecord actual = result.getRecord("out");
    Assert.assertEquals(
      StructuredRecordStringConverter.toJsonString(expected),
      StructuredRecordStringConverter.toJsonString(actual)
    );
  }

  @Test
  public void testBasic() throws Exception {

    testSingleInOutWithMaps(
      "mapper/basic/in.schema.json",
      "mapper/basic/out.schema.json",
      "mapper/basic/data.json",
      "mapper/basic/basic.1.emap",
      "mapper/basic/basic.1.emap.expected"
    );

    testSingleInOutWithMaps(
      "mapper/basic/in.schema.json",
      "mapper/basic/out.schema.json",
      "mapper/basic/data.json",
      "mapper/basic/basic.2.emap",
      "mapper/basic/basic.2.emap.expected"
    );

    testSingleInOutWithMaps(
      "mapper/basic/in.schema.json",
      "mapper/basic/out.schema.json",
      "mapper/basic/data.json",
      "mapper/basic/basic.3.emap",
      "mapper/basic/basic.3.emap.expected"
    );

    testSingleInOutWithMaps(
      "mapper/basic/in.schema.json",
      "mapper/basic/out.schema.json",
      "mapper/basic/data.json",
      "mapper/basic/basic.4.emap",
      "mapper/basic/basic.4.emap.expected"
    );

  }

  private void testGenerateData() throws Exception {
    String inputSchemaString = readTestFile("mapper/basic/in.schema.json");
    Schema schema = Schema.parseJson(inputSchemaString);
    StructuredRecord.Builder mainRecord =
      StructuredRecord.builder(schema)
        .set("a", "test").set("b", "sub-test").set("c", 1l)
        .set("d", 12).set("e", new BigDecimal("12.345").unscaledValue().toByteArray()).set("f", 4.56d);

    Schema rec1 = schema.getField("rec1").getSchema();
    StructuredRecord.Builder rec1Record =
      StructuredRecord.builder(rec1).set("a", "rec1-test")
        .set("b", "rec1-sub-test").set("c", "rec1-c");

    Schema rec2 = rec1.getField("rec2").getSchema().getNonNullable();
    StructuredRecord.Builder rec2Record =
      StructuredRecord.builder(rec2).set("a", "rec2-test-a")
        .set("b", "rec2-test-b").set("c", "rec2-test-c");

    Schema rec3 = schema.getField("rec3").getSchema().getNonNullable().getComponentSchema().getNonNullable();
    List<Object> list = new ArrayList<>();
    list.add(StructuredRecord.builder(rec3).set("a", "rec3-array-a-1").set("b", 5l).build());
    list.add(StructuredRecord.builder(rec3).set("a", "rec3-array-a-2").set("b", 6l).build());

    rec1Record.set("rec2", rec2Record.build());
    mainRecord.set("rec1", rec1Record.build());
    mainRecord.set("rec3", list);

    StructuredRecord build = mainRecord.build();
    String s = StructuredRecordStringConverter.toJsonString(build);
    System.out.println(s);
  }
}