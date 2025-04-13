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
 * Pipeline execution context that provides access to runtime environment,
 * metrics, and configuration properties. This context is passed to directives
 * during their execution phase.
 */
@PublicEvolving
public interface ExecutorContext extends LookupProvider, Serializable {
  /**
   * Defines the runtime environment in which the wrangler is executing.
   * This affects available features and execution behavior.
   */
  enum Environment {
    /** Running as a service with interactive capabilities. */
    SERVICE,
    /** Running as a data pipeline transform. */
    TRANSFORM,
    /** Running as an independent microservice. */
    MICROSERVICE,
    /** Running in test mode with mock capabilities. */
    TESTING
  }

  /**
   * Gets the runtime environment for this context.
   *
   * @return The {@link Environment} this context is configured for
   */
  Environment getEnvironment();

  /**
   * Gets the namespace in which the program is executing.
   * The namespace provides isolation between different executions.
   *
   * @return Name of the current namespace
   */
  String getNamespace();

  /**
   * Gets the metrics collection interface.
   * Use this to record performance and operational metrics.
   *
   * @return StageMetrics instance for recording measurements
   */
  StageMetrics getMetrics();

  /**
   * Gets the unique name of this context.
   * This name can be used for logging and metrics.
   *
   * @return Unique identifier for this context
   */
  String getContextName();

  /**
   * Gets configuration properties for this execution.
   * These properties are set during pipeline or service configuration.
   *
   * @return Map of configuration properties
   */
  Map<String, String> getProperties();

  /**
   * Gets the URL for a specified service.
   * This allows directives to interact with other CDAP services.
   *
   * @param applicationId ID of the application containing the service
   * @param serviceId ID of the service to locate
   * @return URL of the service if it exists, null otherwise
   */
  URL getService(String applicationId, String serviceId);

  /**
   * Gets the temporary storage interface.
   * Use this to store data that needs to persist across directive executions
   * but does not need long-term storage.
   *
   * @return TransientStore instance for temporary data storage
   */
  TransientStore getTransientStore();

  /**
   * Checks if schema management features are enabled.
   * When enabled, directives can modify and validate data schemas.
   *
   * @return true if schema management is enabled, false otherwise
   */
  default boolean isSchemaManagementEnabled() {
    return false;
  }
}
