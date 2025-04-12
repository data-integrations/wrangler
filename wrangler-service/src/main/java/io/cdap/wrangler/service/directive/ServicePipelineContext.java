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

package io.cdap.wrangler.service.directive;

import io.cdap.cdap.api.service.http.HttpServiceContext;
import io.cdap.cdap.api.service.worker.SystemAppTaskContext;
import io.cdap.cdap.etl.api.Lookup;
import io.cdap.cdap.etl.api.StageMetrics;
import io.cdap.cdap.etl.common.DatasetContextLookupProvider;
import io.cdap.cdap.etl.common.NoopMetrics;
import io.cdap.cdap.features.Feature;
import io.cdap.wrangler.api.ExecutorContext;
import io.cdap.wrangler.api.TransientStore;

import java.net.URL;
import java.util.Collections;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Implementation of {@PipelineContext}, for use in Service.
 */
class ServicePipelineContext implements ExecutorContext {
  // this is different than serviceContext.getNamespace(), as it's the namespace of the workspace, not the namespace
  // that we're executing in
  private final String namespace;
  private final Environment environment;
  @Nullable
  private final HttpServiceContext serviceContext;
  private final DatasetContextLookupProvider lookupProvider;
  private final TransientStore store;
  @Nullable
  private final SystemAppTaskContext systemAppTaskContext;

  ServicePipelineContext(String namespace, Environment environment, HttpServiceContext serviceContext,
                         TransientStore store) {
    this(namespace, environment, serviceContext, null, store);
  }

  ServicePipelineContext(String namespace, Environment environment, SystemAppTaskContext systemAppTaskContext,
                         TransientStore store) {
    this(namespace, environment, null, systemAppTaskContext, store);
  }

  private ServicePipelineContext(String namespace, Environment environment,
                                 @Nullable HttpServiceContext serviceContext,
                                 @Nullable SystemAppTaskContext systemAppTaskContext,
                                 TransientStore store) {
    this.namespace = namespace;
    this.environment = environment;
    this.serviceContext = serviceContext;
    this.systemAppTaskContext = systemAppTaskContext;
    this.lookupProvider = new DatasetContextLookupProvider(serviceContext);
    this.store = store;
  }

  @Override
  public String getNamespace() {
    return namespace;
  }

  /**
   * @return Environment this context is prepared for.
   */
  @Override
  public Environment getEnvironment() {
    return environment;
  }

  /**
   * @return Measurements handler.
   */
  @Override
  public StageMetrics getMetrics() {
    return NoopMetrics.INSTANCE;
  }

  /**
   * @return Context name.
   */
  @Override
  public String getContextName() {
    if (systemAppTaskContext != null) {
      return systemAppTaskContext.getServiceName();
    }
    return serviceContext.getSpecification().getName();
  }

  /**
   * @return
   */
  @Override
  public Map<String, String> getProperties() {
    return Collections.emptyMap();
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
    if (systemAppTaskContext != null) {
      return systemAppTaskContext.getServiceURL(applicationId, serviceId);
    }
    return serviceContext.getServiceURL(applicationId, serviceId);
  }

  @Override
  public TransientStore getTransientStore() {
    return store;
  }

  /**
   * @return Properties associated with run and pipeline.
   */
  @Override
  public <T> Lookup<T> provide(String s, Map<String, String> map) {
    return lookupProvider.provide(s, map);
  }

  @Override
  public boolean isSchemaManagementEnabled() {
    if (systemAppTaskContext != null) {
      return Feature.WRANGLER_SCHEMA_MANAGEMENT.isEnabled(systemAppTaskContext);
    }
    return Feature.WRANGLER_SCHEMA_MANAGEMENT.isEnabled(serviceContext);
  }
}
