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

/**
 * Amazon Credentials extracted from the {@link Connection}.
 */
public class WebCredential implements AWSCredentials {
  private final String accessKey;
  private final String secretAccessKey;

  public WebCredential(Connection connection) {
    this.accessKey = connection.getProp("accessKey");
    this.secretAccessKey = connection.getProp("secretAccessKey");
    if (this.accessKey == null || this.secretAccessKey == null) {
      throw new IllegalArgumentException("AWS access key or secrete access key not specified.");
    }
  }

  @Override
  public String getAWSAccessKeyId() {
    return accessKey;
  }

  @Override
  public String getAWSSecretKey() {
    return secretAccessKey;
  }
}
