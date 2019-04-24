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

package io.cdap.wrangler.service.adls;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Map;

/**
 * Credentials of the authorized user connecting to ADLS Gen1
 */
public class ADLSConfiguration {
  private static final List<String> CONFIG_FIELDS = ImmutableList.of("clientID", "clientSecret",
          "refreshURL", "accountFQDN");
  private final String clientID;
  private final String clientSecret;
  private final String refreshURL;
  private final String accountFQDN;

  ADLSConfiguration(Map<String, String> properties) {

    if (properties == null || properties.size() == 0) {
      throw new IllegalArgumentException("ADLS properties are not defined. Check connection setting.");
    }

    for (String property : CONFIG_FIELDS) {
      if (!properties.containsKey(property)) {
        throw new IllegalArgumentException("Missing configuration in connection for property " + property);
      }
    }

    clientID = properties.get("clientID");
    clientSecret = properties.get("clientSecret");
    refreshURL = properties.get("refreshURL");
    accountFQDN = properties.get("accountFQDN");
  }


  public String getADLSClientID() {
    return clientID;
  }

  public String getClientKey() {
    return clientSecret;
  }

  public String getEndpointURL() {
    return refreshURL;
  }

  public String getAccountFQDN() {
    return accountFQDN;
  }
}
