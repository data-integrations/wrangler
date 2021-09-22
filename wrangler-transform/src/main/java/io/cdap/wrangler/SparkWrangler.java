/*
 * Copyright © 2021 Cask Data, Inc.
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
package io.cdap.wrangler;

import io.cdap.cdap.api.annotation.RuntimeImplementation;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.spark.sql.DataFrames;
import io.cdap.cdap.etl.api.batch.SparkCompute;
import io.cdap.cdap.etl.api.batch.SparkExecutionPluginContext;
import io.cdap.wrangler.Wrangler.Config;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

/**
 * Wrangler implementation with Spark SQL. Currently it has only precondition support
 */
@RuntimeImplementation(pluginClass = Wrangler.class, order = Wrangler.ORDER_SPARK)
public class SparkWrangler extends SparkCompute<StructuredRecord, StructuredRecord> {

  private final Wrangler.Config config;

  public SparkWrangler(Config config) {
    this.config = config;
    if (!Config.EL_SQL.equalsIgnoreCase(config.getExpressionLanguage())) {
      //Fall back
      throw new IllegalStateException("This implementation runs for SQL language");
    }
    if (config.getDirectives() != null && !config.getDirectives().trim().isEmpty()) {
      throw new IllegalStateException("We only run this for empty directives list");
    }
  }

  @Override
  public JavaRDD<StructuredRecord> transform(SparkExecutionPluginContext context,
      JavaRDD<StructuredRecord> input) throws Exception {
    SparkSession session = SparkSession.builder().sparkContext(context.getSparkContext().sc())
        .getOrCreate();
    StructType inputSchema = DataFrames.toDataType(context.getInputSchema());
    Schema outputSchema = context.getOutputSchema();
    JavaRDD<Row> rowRDD = input.map(record -> DataFrames.toRow(record, inputSchema));
    Dataset<Row> df = session.createDataFrame(rowRDD, inputSchema);
    Dataset<Row> result = df.filter(config.getPrecondition());
    return result.javaRDD()
        .map(r -> DataFrames.fromRow(r, outputSchema));
  }
}
