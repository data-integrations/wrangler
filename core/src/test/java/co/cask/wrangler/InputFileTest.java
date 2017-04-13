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
import co.cask.wrangler.internal.ParallelPipelineExecutor;
import co.cask.wrangler.internal.PipelineExecutor;
import co.cask.wrangler.internal.TextDirectives;
import co.cask.wrangler.steps.transformation.functions.DDL;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

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
    Path path = Paths.get("/Users/nitin/Downloads/schema_from_avro.avsc");
    byte[] avroSchemaBytes = Files.readAllBytes(path);
    String avroSchemaString = new String(avroSchemaBytes);

    // Takes the avro schema and converts it to json.
    Schema schema = DDL.parse(avroSchemaString);

    // Now we select the path : 'GetReservationRS.Reservation.PassengerReservation.Segments.Segment'
    schema = DDL.select(schema, "GetReservationRS.Reservation.PassengerReservation.Segments.Segment[0]");

    // Now, we drop 'Product'
    schema = DDL.drop(schema, "Product", "Air", "Vehicle", "Hotel", "General");

    Assert.assertTrue(true);
  }

}
