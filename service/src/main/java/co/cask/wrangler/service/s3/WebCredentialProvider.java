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
import com.amazonaws.auth.AWSCredentialsProvider;

/**
 * {@link Connection} S3 credential provider.
 */
public class WebCredentialProvider implements AWSCredentialsProvider {
  private final Connection connection;

  public WebCredentialProvider(Connection connection) {
    this.connection = connection;
  }

  @Override
  public AWSCredentials getCredentials() {
    return new WebCredential(connection);
  }

  @Override
  public void refresh() {
    // Nothing to be done here.
  }
}
