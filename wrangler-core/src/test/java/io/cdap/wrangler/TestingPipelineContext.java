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

package io.cdap.wrangler;

import io.cdap.cdap.etl.api.Lookup;
import io.cdap.cdap.etl.api.StageMetrics;
import io.cdap.directives.aggregates.DefaultTransientStore;
import io.cdap.wrangler.api.Executor;
import io.cdap.wrangler.api.ExecutorContext;
import io.cdap.wrangler.api.TransientStore;
import io.cdap.wrangler.proto.Contexts;
import org.mockito.Mockito;

import java.net.URL;
import java.util.HashMap;
import java.util.Map;

/**
 * This class {@link TestingPipelineContext} is a runtime context that is provided for each
 * {@link Executor} execution.
 */
public class TestingPipelineContext implements ExecutorContext {
  private final StageMetrics metrics;
  private final String name;
  private final TransientStore store;
  private final Map<String, String> properties;
  private boolean schemaManagementEnabled;

  public TestingPipelineContext() {
    name = "testing";
    store = new DefaultTransientStore();
    properties = new HashMap<>();

    metrics = Mockito.mock(StageMetrics.class);
    Mockito.doNothing().when(metrics).count(Mockito.anyString(), Mockito.anyInt());

    schemaManagementEnabled = false;
  }

  /**
   * @return Environment this context is prepared for.
   */
  @Override
  public Environment getEnvironment() {
    return Environment.TESTING;
  }

  @Override
  public String getNamespace() {
    return Contexts.SYSTEM;
  }

  /**
   * @return Measurements context.
   */
  @Override
  public StageMetrics getMetrics() {
    return metrics;
  }

  /**
   * @return Context name.
   */
  @Override
  public String getContextName() {
    return name;
  }

  /**
   * @return Properties associated with run and pipeline.
   */
  @Override
  public Map<String, String> getProperties() {
    return properties;
  }

  /**
   * Returns a valid service url.
   *
   * @param applicationId id of the application to which a service url.
   * @param serviceId     id of the service within application.
   * @return URL if service exists, else null.
   */
  @Override
  public URL getService(String applicationId, String serviceId) {
    return null;
  }

  @Override
  public TransientStore getTransientStore() {
    return store;
  }

  public TestingPipelineContext setSchemaManagementEnabled() {
    this.schemaManagementEnabled = true;
    return this;
  }

  /**
   * Provides a handle to dataset for lookup.
   *
   * @param s name of the dataset.
   * @param map properties associated with dataset.
   * @return handle to dataset for lookup.
   */
  @Override
  public <T> Lookup<T> provide(String s, Map<String, String> map) {
    return null;
  }

  @Override
  public boolean isSchemaManagementEnabled() {
    return schemaManagementEnabled;
  }
}
