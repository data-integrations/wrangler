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

import co.cask.wrangler.api.Record;
import co.cask.wrangler.internal.ParallelPipelineExecutor;
import co.cask.wrangler.internal.PipelineExecutor;
import co.cask.wrangler.internal.TextDirectives;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.spi.json.GsonJsonProvider;
import com.jayway.jsonpath.spi.json.JsonOrgJsonProvider;
import com.jayway.jsonpath.spi.json.JsonProvider;
import com.jayway.jsonpath.spi.mapper.GsonMappingProvider;
import com.jayway.jsonpath.spi.mapper.JsonOrgMappingProvider;
import com.jayway.jsonpath.spi.mapper.MappingProvider;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

/**
 * The test in this file is ignored as we use this only in cases when someone reports an issue with the file.
 */
public class InputFileTest {

  public static final Configuration GSON_CONFIGURATION = Configuration
    .builder()
    .mappingProvider(new GsonMappingProvider())
    .jsonProvider(new GsonJsonProvider())
    .build();

  public static final Configuration JSON_ORG_CONFIGURATION = Configuration
    .builder()
    .mappingProvider(new JsonOrgMappingProvider())
    .jsonProvider(new JsonOrgJsonProvider())
    .build();

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

  /**
   * This will be used for later use, for now we use default provider.
   */
  private static class Config implements Configuration.Defaults {
    private final JsonProvider jsonProvider = new GsonJsonProvider();
    private final MappingProvider mappingProvider = new GsonMappingProvider();

    @Override
    public JsonProvider jsonProvider() {
      return jsonProvider;
    }

    @Override
    public MappingProvider mappingProvider() {
      return mappingProvider;
    }

    @Override
    public Set<Option> options() {
      return EnumSet.noneOf(Option.class);
    }
  }
}
