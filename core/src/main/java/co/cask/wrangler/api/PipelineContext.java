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

package co.cask.wrangler.api;

import co.cask.cdap.etl.api.LookupProvider;
import co.cask.cdap.etl.api.StageMetrics;
import co.cask.wrangler.api.annotations.PublicEvolving;

import java.io.Serializable;
import java.util.Map;

/**
 * Pipeline Context for passing contextual information to the pipeline being executed.
 */
@PublicEvolving
public interface PipelineContext extends LookupProvider, Serializable {
  public enum Environment {
    SERVICE,
    TRANSFORM
  };

  /**
   * @return Environment this context is prepared for.
   */
  public Environment getEnvironment();

  /**
   * @return Measurements handler.
   */
  public StageMetrics getMetrics();

  /**
   * @return Context name.
   */
  public String getContextName();

  /**
   * @return Properties associated with run and pipeline.
   */
  public Map<String, String> getProperties();
}
