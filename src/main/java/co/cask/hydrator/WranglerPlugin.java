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

package co.cask.hydrator;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.api.TransformContext;
import co.cask.wrangler.Row;
import co.cask.wrangler.Stages.CsvParser;
import co.cask.wrangler.Stages.Lower;
import co.cask.wrangler.Stages.Types;
import co.cask.wrangler.WrangleStep;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Wrangler - A interactive tool for data data cleansing and transformation.
 *
 * This plugin is an implementation of the transformation that are performed in the
 * backend for operationalizing all the interactive wrangling that is being performed
 * by the user.
 */
@Plugin(type = "transform")
@Name("Wrangler")
@Description("Wrangler - A interactive tool for data cleansing and transformation.")
public class WranglerPlugin<StructuredRecord> extends Transform<StructuredRecord, StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(WranglerPlugin.class);

  // Stores a list of preconfigured wrangle stage objects to execute during transformation.
  private List<WrangleStep> stages = new ArrayList<>();

  @Override
  public void initialize(TransformContext context) throws Exception {
    super.initialize(context);

    // Setup the stages as per the frontend configuration.
    //stages.add(new CsvParser(',', "col", null));
    stages.add(new co.cask.wrangler.Stages.Name(new ArrayList<String>()));
    stages.add(new Types(new ArrayList<String>()));
    stages.add(new Lower("col1"));
  }

  @Override
  public void transform(StructuredRecord input, Emitter<StructuredRecord> emitter) throws Exception {
    Row row = new Row();
    for (WrangleStep stage : stages) {
      row = stage.execute(row);
    }
  }



  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    super.configurePipeline(pipelineConfigurer);
  }
}

