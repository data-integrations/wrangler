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

package co.cask.wrangler.steps.parser;

import co.cask.wrangler.api.AbstractStep;
import co.cask.wrangler.api.PipelineContext;
import co.cask.wrangler.api.Record;
import co.cask.wrangler.api.StepException;
import co.cask.wrangler.api.Usage;

import java.net.URL;
import java.util.List;

/**
 * A AVRO Parser Stage for parsing the {@link Record} provided based on configuration.
 */
@Usage(
  directive = "parse-as-avro",
  usage = "parse-as-avro <column> <schema-id>",
  description = "Parses a column as AVRO record based on the schema id."
)
public class AvroParser extends AbstractStep {
  private final String col;
  private final String id;
  private boolean schemaRetrieved = false;
  private String schema;

  public AvroParser(int lineno, String detail, String col, String id) {
    super(lineno, detail);
    this.col = col;
    this.id = id;
  }

  /**
   * Parses a give column in a {@link Record} as a CSV Record.
   *
   * @param records Input {@link Record} to be wrangled by this step.
   * @param context Specifies the context of the pipeline.
   * @return New Record containing multiple columns based on CSV parsing.
   */
  @Override
  public List<Record> execute(List<Record> records, PipelineContext context)
    throws StepException {

    if (!schemaRetrieved) {
      schema = getAVROSchema(context);
    }

    for (Record record : records) {
      int idx = record.find(col);
    }
    return records;
  }

  private String getAVROSchema(PipelineContext context) {
    URL url = context.getService("dataprep", "service");
    String path = url.toString();
  }
}
