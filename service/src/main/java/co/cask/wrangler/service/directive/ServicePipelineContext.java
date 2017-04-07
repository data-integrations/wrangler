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

package co.cask.wrangler.service.directive;

import co.cask.cdap.api.service.http.HttpServiceContext;
import co.cask.cdap.etl.api.Lookup;
import co.cask.cdap.etl.api.StageMetrics;
import co.cask.cdap.etl.common.DatasetContextLookupProvider;
import co.cask.cdap.etl.common.NoopMetrics;
import co.cask.wrangler.api.PipelineContext;

import java.util.Collections;
import java.util.Map;

/**
 * Implementation of {@PipelineContext}, for use in Service.
 */
class ServicePipelineContext implements PipelineContext {
  private PipelineContext.Environment environment;
  private final HttpServiceContext serviceContext;
  private final DatasetContextLookupProvider lookupProvider;

  public ServicePipelineContext(Environment environment, HttpServiceContext serviceContext) {
    this.environment = environment;
    this.serviceContext = serviceContext;
    this.lookupProvider = new DatasetContextLookupProvider(serviceContext);
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
   * @return Properties associated with run and pipeline.
   */
  @Override
  public <T> Lookup<T> provide(String s, Map<String, String> map) {
    return lookupProvider.provide(s, map);
  }
}
