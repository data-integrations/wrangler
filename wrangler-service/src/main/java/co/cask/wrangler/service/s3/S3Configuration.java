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

package co.cask.wrangler.service.s3;

import co.cask.wrangler.dataset.connections.Connection;
import com.amazonaws.auth.AWSCredentials;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Map;

/**
 * Credentials of the authorized user connecting to AWS and AWS region connecting to
 */
public class S3Configuration implements AWSCredentials {
  private static final List<String> CONFIG_FIELDS = ImmutableList.of("accessKeyId", "accessSecretKey", "region");
  private final String accessKeyId;
  private final String accessSecretKey;
  private final String region;

  S3Configuration(Connection connection) {
    Map<String, Object> properties = connection.getAllProps();

    if(properties == null || properties.size() == 0) {
      throw new IllegalArgumentException("S3 properties are not defined. Check connection setting.");
    }

    for (String property : CONFIG_FIELDS) {
      if (!properties.containsKey(property)) {
        throw new IllegalArgumentException("Missing configuration in connection for property " + property);
      }
    }
    accessKeyId = String.valueOf(properties.get("accessKeyId"));
    accessSecretKey = String.valueOf(properties.get("accessSecretKey"));
    region = String.valueOf(properties.get("region"));
  }

  @Override
  public String getAWSAccessKeyId() {
    return accessKeyId;
  }

  @Override
  public String getAWSSecretKey() {
    return accessSecretKey;
  }

  public String getRegion() {
    return region;
  }
}
