/*
 * Copyright © 2017-2019 Cask Data, Inc.
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

package io.cdap.wrangler.service.s3;

import com.amazonaws.auth.AWSCredentials;
import com.google.common.collect.ImmutableList;
import io.cdap.wrangler.proto.connection.ConnectionMeta;

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

  S3Configuration(Map<String, String> properties) {
    if (properties == null || properties.size() == 0) {
      throw new IllegalArgumentException("S3 properties are not defined. Check connection setting.");
    }

    for (String property : CONFIG_FIELDS) {
      if (!properties.containsKey(property)) {
        throw new IllegalArgumentException("Missing configuration in connection for property " + property);
      }
    }
    accessKeyId = properties.get("accessKeyId");
    accessSecretKey = properties.get("accessSecretKey");
    region = properties.get("region");
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
