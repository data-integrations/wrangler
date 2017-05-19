package co.cask.wrangler.steps.parser;

import co.cask.wrangler.api.Record;
import co.cask.wrangler.executor.PipelineExecutor;
import co.cask.wrangler.executor.TextDirectives;
import org.junit.Assert;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * Tests {@link ParseAvroFile}
 */
public class ParseAvroFileTest {

  @Test
  public void testParseAsAvroFile() throws Exception {
    Path path = Paths.get("/Users/nitin/Downloads/1495172716487.avro");
    byte[] data = Files.readAllBytes(path);

    String[] directives = new String[] {
      "parse-as-avro-file body",
      "drop body"
    };

    List<Record> records = new ArrayList<>();
    records.add(new Record("body", data));

    PipelineExecutor executor = new PipelineExecutor();
    executor.configure(new TextDirectives(directives), null);
    List<Record> results = executor.execute(records);
    Assert.assertTrue(results.size() > 0);
  }

}