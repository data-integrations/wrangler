/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.wrangler.steps.transformation;

import co.cask.wrangler.api.AbstractStep;
import co.cask.wrangler.api.PipelineContext;
import co.cask.wrangler.api.Record;
import co.cask.wrangler.api.StepException;
import co.cask.wrangler.api.Usage;
import org.simmetrics.StringMetric;
import org.simmetrics.metrics.StringMetrics;

import java.util.List;

/**
 * Step for implementing the directive for measuring the metrics between two sequence of characters.
 */
@Usage(
  directive = "text-metric",
  usage = "text-metric <method> <column1> <column2> <destination>",
  description = "Calculates the metric for comparing two string values."
)
public class TextMetricMeasure extends AbstractStep {
  private final String column1;
  private final String column2;
  private final String destination;
  private final StringMetric metric;

  public TextMetricMeasure(int lineno, String directive, String method, String column1, String column2,
                           String destination) {
    super(lineno, directive);
    this.column1 = column1;
    this.column2 = column2;
    this.destination = destination;

    // defaults to : cosineSimilarity
    switch(method.toLowerCase()) {
      case "euclidean":
        metric = StringMetrics.euclideanDistance();
        break;

      case "block-metric":
        metric = StringMetrics.blockDistance();
        break;

      case "identity":
        metric = StringMetrics.identity();
        break;

      case "block":
        metric = StringMetrics.blockDistance();
        break;

      case "dice":
        metric = StringMetrics.dice();
        break;

      case "longest-common-subsequence":
        metric = StringMetrics.longestCommonSubsequence();
        break;

      case "longest-common-substring":
        metric = StringMetrics.longestCommonSubstring();
        break;

      case "overlap-cofficient":
        metric = StringMetrics.overlapCoefficient();
        break;

      case "jaccard":
        metric = StringMetrics.jaccard();
        break;

      case "damerau-levenshtein":
        metric = StringMetrics.damerauLevenshtein();
        break;

      case "generalized-jaccard":
        metric = StringMetrics.generalizedJaccard();
        break;

      case "jaro":
        metric = StringMetrics.jaro();
        break;

      case "simon-white":
        metric = StringMetrics.simonWhite();
        break;

      case "levenshtein":
        metric = StringMetrics.levenshtein();
        break;

      case "cosine":
      default:
        metric = StringMetrics.cosineSimilarity();
        break;
    }
  }

  /**
   * Executes a wrangle step on single {@link Record} and return an array of wrangled {@link Record}.
   *
   * @param records List of input {@link Record} to be wrangled by this step.
   * @param context {@link PipelineContext} passed to each step.
   * @return Wrangled List of {@link Record}.
   */
  @Override
  public List<Record> execute(List<Record> records, PipelineContext context) throws StepException {
    for (Record record : records) {
      int idx1 = record.find(column1);
      int idx2 = record.find(column2);

      if (idx1 == -1 || idx2 == -1) {
        record.add(destination, 0.0f);
        continue;
      }

      Object object1 = record.getValue(idx1);
      Object object2 = record.getValue(idx2);

      if (object1 == null || object2 == null) {
        record.add(destination, 0.0f);
        continue;
      }

      if (!(object1 instanceof String) || !(object2 instanceof String)) {
        record.add(destination, 0.0f);
        continue;
      }

      if (((String) object1).isEmpty() || ((String) object2).isEmpty()) {
        record.add(destination, 0.0f);
        continue;
      }

      record.add(destination, metric.compare((String) object1, (String) object2));
    }

    return records;
  }
}
