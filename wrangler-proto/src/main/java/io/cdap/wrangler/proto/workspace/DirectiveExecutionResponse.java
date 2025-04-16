/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.wrangler.proto.workspace;

import io.cdap.wrangler.proto.ServiceResponse;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * The result of executing directives on a workspace.
 */
public class DirectiveExecutionResponse extends ServiceResponse<Map<String, Object>> {
  private final Collection<String> header;
  private final Map<String, String> types;
  private final List<String> directives;

  public DirectiveExecutionResponse(Collection<Map<String, Object>> values, Collection<String> header,
                                    Map<String, String> types, List<String> directives) {
    super(values);
    this.header = header;
    this.types = types;
    this.directives = directives;
  }
}
