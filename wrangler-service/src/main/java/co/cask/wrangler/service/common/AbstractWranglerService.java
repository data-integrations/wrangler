/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.wrangler.service.common;

import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceContext;
import co.cask.wrangler.DataPrep;
import co.cask.wrangler.dataset.connections.ConnectionStore;
import co.cask.wrangler.dataset.workspace.DataType;
import co.cask.wrangler.dataset.workspace.WorkspaceDataset;
import co.cask.wrangler.dataset.workspace.WorkspaceException;
import co.cask.wrangler.proto.Recipe;
import co.cask.wrangler.proto.RequestV1;
import com.google.gson.Gson;

import java.util.List;
import javax.annotation.Nullable;

import static co.cask.wrangler.service.directive.DirectivesService.WORKSPACE_DATASET;

public class AbstractWranglerService extends AbstractHttpServiceHandler {
  private static final Gson GSON = new Gson();
  protected ConnectionStore store;

  @UseDataSet(DataPrep.CONNECTIONS_DATASET)
  private Table connectionTable;

  @UseDataSet(WORKSPACE_DATASET)
  protected WorkspaceDataset ws;

  @Override
  public void initialize(HttpServiceContext context) throws Exception {
    super.initialize(context);
    store = new ConnectionStore(connectionTable);
  }

  /**
   * Return whether the header needs to be copied when creating the pipeline source for the specified workspace.
   * This just amounts to checking whether parse-as-csv with the first line as a header is used as a directive.
   */
  protected boolean shouldCopyHeader(@Nullable String workspaceId) throws WorkspaceException {
    if (workspaceId == null) {
      return false;
    }
    String data = ws.getData(workspaceId, WorkspaceDataset.REQUEST_COL, DataType.TEXT);
    if (data == null) {
      return false;
    }

    RequestV1 workspaceReq = GSON.fromJson(data, RequestV1.class);
    Recipe recipe = workspaceReq.getRecipe();
    List<String> directives = recipe.getDirectives();
    // yes this is really hacky, but there doesn't seem to be a good way to get the actual directive classes
    return directives.stream()
      .map(String::trim)
      .anyMatch(directive -> directive.startsWith("parse-as-csv") && directive.endsWith("true"));
  }
}
