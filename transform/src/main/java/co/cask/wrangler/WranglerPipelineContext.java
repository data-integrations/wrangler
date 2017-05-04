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

package co.cask.wrangler;

import co.cask.cdap.etl.api.Lookup;
import co.cask.cdap.etl.api.StageMetrics;
import co.cask.cdap.etl.api.TransformContext;
import co.cask.wrangler.api.PipelineContext;

import java.util.Map;

/**
 * Class description here.
 */
class WranglerPipelineContext implements PipelineContext {
  private final Environment environment;
  private final TransformContext context;
  private StageMetrics metrics;
  private String name;
  private Map<String, String> properties;

  WranglerPipelineContext(Environment environment, TransformContext context) {
    this.environment = environment;
    this.metrics = context.getMetrics();
    this.name = context.getStageName();
    this.properties = context.getPluginProperties().getProperties();
    this.context = context;
  }

  /**
   * @return Environment this context is prepared for.
   */
  @Override
  public Environment getEnvironment() {
    return environment;
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
   *
   * @param s
   * @param map
   * @param <T>
   * @return
   */
  @Override
  public <T> Lookup<T> provide(String s, Map<String, String> map) {
    return context.provide(s, map);
  }
}
