package io.cdap.directives;

import io.cdap.wrangler.TestingRig;
import io.cdap.wrangler.api.Row;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class NullHandlingTest {

  private static final String CSV_PARSER_DIRECTIVE = "parse-as-csv body , false";
  private static final String FIXED_LENGTH_PARSER_DIRECTIVE = "parse-as-fixed-length body 2,2,1,1,3,4";
  private static final String HL7_PARSER_DIRECTIVE =  "parse-as-hl7 body";
  private static final String JS_PARSER_DIRECTIVE =  "parse-as-json body";
  private static final String JS_PATH_PARSER_DIRECTIVE =  "set-column body json:Parse(body)"; // do not understand what it is
  private static final String AVRO_PARSER_DIRECTIVE =  "parse-as-avro body"; // skipping for now, since this needs to connect to schema registry
  private static final String AVRO_FILE_PARSER_DIRECTIVE =  "parse-as-avro-file body";
  private static final String DATE_PARSER_DIRECTIVE =  "parse-as-date :body 'US/Eastern'";
  private static final String DATE_TIME_PARSER_DIRECTIVE =  "parse-as-datetime :body \"MM/dd/yyyy HH:mm\"";
  private static final String EXCEL_PARSER_DIRECTIVE =  "parse-as-excel :body '0'";
  private static final String LOG_PARSER_DIRECTIVE =  "parse-as-log :body '%h %l %u %t \"%r\" %>s %b'";
  private static final String PROTOBUF_PARSER_DIRECTIVE =  "parse-as-hl7 body"; // needs schema registry connection
  private static final String SIMPLE_DATE_PARSER_DIRECTIVE =  "parse-as-simple-date :body \"h:mm a\"";
  private static final String TIMESTAMP_PARSER_DIRECTIVE =  "parse-timestamp :body";
  private static final String CURRENCY_PARSER_DIRECTIVE =  "parse-as-currency :body :dst 'en_US'";
  private static final String XML_TO_CSV_PARSER_DIRECTIVE =  "parse-xml-to-json :body";


  @Test
  public void ParsersTest() {
    List<Row> rows = Arrays.asList(new Row("body", null));

    run(CSV_PARSER_DIRECTIVE, rows);
    run(FIXED_LENGTH_PARSER_DIRECTIVE, rows);
    run(HL7_PARSER_DIRECTIVE, rows);
    run(JS_PARSER_DIRECTIVE, rows);
    // run(JS_PATH_PARSER_DIRECTIVE, rows);
    // run(AVRO_PARSER_DIRECTIVE, rows);
    run(AVRO_FILE_PARSER_DIRECTIVE, rows);
    run(DATE_PARSER_DIRECTIVE, rows);
    run(DATE_TIME_PARSER_DIRECTIVE, rows);
    run(EXCEL_PARSER_DIRECTIVE, rows);
    run(LOG_PARSER_DIRECTIVE, rows);
    // run(PROTOBUF_PARSER_DIRECTIVE, rows);
    run(SIMPLE_DATE_PARSER_DIRECTIVE, rows);
    run(TIMESTAMP_PARSER_DIRECTIVE, rows);
    run(CURRENCY_PARSER_DIRECTIVE, rows);
    run(XML_TO_CSV_PARSER_DIRECTIVE, rows);
  }

  private void run(String directive, List<Row> rows) {
    String[] directives = new String[] {directive};
    try {
      TestingRig.execute(directives, rows);
    } catch (Exception e) {
      //System.out.println("Error in handling NULL value for " + directive.split(" ")[0]);
      System.out.println(e.getMessage());
      System.out.println("---");
    }
  }

  @Test
  public void JsParserTest() {
    List<Row> rows = Arrays.asList(
      new Row("body", null),
      new Row("body", "{\"name\" : \"sai\"}")
    );
    String[] directives = new String[] {JS_PARSER_DIRECTIVE};
    try {
      rows = TestingRig.execute(directives, rows);
      System.out.println(rows.size());
    } catch (Exception e) {
      //System.out.println("Error in handling NULL value for " + directive.split(" ")[0]);
      System.out.println(e.getMessage());
      System.out.println("---");
    }
  }

  @Test
  public void ExcelParserTest() {
    List<Row> rows = Arrays.asList(new Row("body", null));
    String[] directives = new String[] {EXCEL_PARSER_DIRECTIVE};
    try {
      rows = TestingRig.execute(directives, rows);
      System.out.println(rows.size());
    } catch (Exception e) {
      //System.out.println("Error in handling NULL value for " + directive.split(" ")[0]);
      System.out.println(e.getMessage());
      System.out.println("---");
    }
  }

}
