/*
 *  Copyright © 2017-2019 Cask Data, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *  use this file except in compliance with the License. You may obtain a copy of
 *  the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations under
 *  the License.
 */

package io.cdap.wrangler.api;

import io.cdap.cdap.etl.api.LookupProvider;
import io.cdap.cdap.etl.api.StageMetrics;
import io.cdap.wrangler.api.annotations.PublicEvolving;

import java.io.Serializable;
import java.net.URL;
import java.util.Map;

/**
 * Pipeline Context for passing contextual information to the pipeline being executed.
 */
@PublicEvolving
public interface ExecutorContext extends LookupProvider, Serializable {
  /**
   * Specifies the environment in which wrangler is running.
   */
  enum Environment {
    SERVICE,
    TRANSFORM,
    MICROSERVICE,
    TESTING
  };

  /**
   * @return Environment this context is prepared for.
   */
  Environment getEnvironment();

  /**
   * @return namespace that the program is being executed in
   */
  String getNamespace();

  /**
   * @return Measurements handler.
   */
  StageMetrics getMetrics();

  /**
   * @return Context name.
   */
  String getContextName();

  /**
   * @return Properties associated with run and pipeline.
   */
  Map<String, String> getProperties();

  /**
   * Returns a valid service url.
   *
   * @param applicationId id of the application to which a service url.
   * @param serviceId id of the service within application.
   * @return URL if service exists, else null.
   */
  URL getService(String applicationId, String serviceId);

  /**
   * @return A transient store.
   */
  TransientStore getTransientStore();

  default boolean isSchemaManagementEnabled() {
    return false;
  }
}
