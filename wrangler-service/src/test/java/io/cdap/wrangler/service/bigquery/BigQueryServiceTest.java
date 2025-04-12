/*
 * Copyright © 2018-2019 Cask Data, Inc.
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

package io.cdap.wrangler.service.bigquery;

import com.google.cloud.bigquery.DatasetId;
import io.cdap.wrangler.proto.connection.ConnectionMeta;
import io.cdap.wrangler.proto.connection.ConnectionType;
import io.cdap.wrangler.service.gcp.GCPUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

/**
 * Tests for {@link BigQueryHandler}.
 */
public class BigQueryServiceTest {

  @Test
  public void testDatasetWhitelistParsing() {
    ConnectionMeta connection = ConnectionMeta.builder()
      .setName("test")
      .setType(ConnectionType.BIGQUERY)
      .putProperty(GCPUtils.PROJECT_ID, "pX")
      // [p0,d0], [p1,d1], 'p2:' is invalid and should be ignored, [pX,d2], [pX,d3]
      .putProperty("datasetWhitelist", "p0:d0 , p1:d1 , p2: , d2 , :d3")
      .build();

    Set<DatasetId> expected = new HashSet<>();
    expected.add(DatasetId.of("p0", "d0"));
    expected.add(DatasetId.of("p1", "d1"));
    expected.add(DatasetId.of("pX", "d2"));
    expected.add(DatasetId.of("pX", "d3"));
    Set<DatasetId> actual = BigQueryHandler.getDatasetWhitelist(connection);
    Assert.assertEquals(expected, actual);
  }
}
