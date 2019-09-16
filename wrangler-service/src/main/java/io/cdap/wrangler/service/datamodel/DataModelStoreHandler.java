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
package io.cdap.wrangler.service.datamodel;

import io.cdap.cdap.api.NamespaceSummary;
import io.cdap.cdap.api.annotation.TransactionControl;
import io.cdap.cdap.api.annotation.TransactionPolicy;
import io.cdap.cdap.api.service.http.HttpServiceRequest;
import io.cdap.cdap.api.service.http.HttpServiceResponder;
import io.cdap.cdap.api.service.http.SystemHttpServiceContext;
import io.cdap.cdap.spi.data.StructuredTableContext;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import io.cdap.wrangler.dataset.datamodel.DataModelConfig;
import io.cdap.wrangler.dataset.datamodel.DataModelStore;
import io.cdap.wrangler.dataset.datamodel.PackageDataModelLoader;
import io.cdap.wrangler.dataset.workspace.ConfigStore;
import io.cdap.wrangler.dataset.workspace.ConfigType;
import io.cdap.wrangler.proto.Contexts;
import io.cdap.wrangler.proto.Namespace;
import io.cdap.wrangler.proto.NamespacedId;
import io.cdap.wrangler.proto.NotFoundException;
import io.cdap.wrangler.proto.ServiceResponse;
import io.cdap.wrangler.proto.datamodel.JsonSchemaDataModelDescriptor;
import io.cdap.wrangler.service.common.AbstractWranglerHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * The REST APIs for managing and interacting with the data models in the system.
 */
public class DataModelStoreHandler extends AbstractWranglerHandler {
  private static final Logger LOG = LoggerFactory.getLogger(DataModelStoreHandler.class);
  private static final PackageDataModelLoader DATA_MODEL_LOADER = new PackageDataModelLoader();


  /**
   * Populates the DataModelStore with data model definitions defined in the package.
   * @param context the HTTP service runtime context
   * @throws Exception
   */
  @Override
  public void initialize(SystemHttpServiceContext context) throws Exception {
    super.initialize(context);
    try {
      NamespaceSummary namespaceSummary;
      String ctxNamespace = context.getNamespace();
      if (Contexts.SYSTEM.equals(ctxNamespace)) {
        namespaceSummary = new NamespaceSummary(Contexts.SYSTEM, "", 0L);
      } else {
        namespaceSummary = getContext().getAdmin().getNamespaceSummary(ctxNamespace);
      }
      Namespace namespace = new Namespace(namespaceSummary.getName(), namespaceSummary.getGeneration());

      TransactionRunners.run(getContext(), ctx -> {
        ConfigStore configStore = ConfigStore.get(ctx);
        DataModelConfig dataModelConfig = configStore.getConfig(ConfigType.DATA_MODELS, DataModelConfig.class);
        if (dataModelConfig == null || !dataModelConfig.getIsDataModelsLoaded()) {
          DataModelStore store = DataModelStore.get(ctx);
          for (JsonSchemaDataModelDescriptor descriptor : DATA_MODEL_LOADER.load(namespace)) {
            store.addOrUpdateDataModel(descriptor);
          }
          configStore.<DataModelConfig>updateConfig(ConfigType.DATA_MODELS, new DataModelConfig(true));
        }
      });
    } catch (Exception e) {
      LOG.warn("Exception while loading default schemas ", e);
    }
  }

  /**
   * Returns a listing of the meta data information for all of the data models. By default excludes
   * the data model definitions. If the data model definition is required, the data model specific
   * end point should be used.
   *
   * @param request Handler for incoming request.
   * @param responder Responder for data going out.
   * @param namespace Namespace of request.
   */
  @GET
  @Path("contexts/{context}/datamodels")
  @TransactionPolicy(value = TransactionControl.EXPLICIT)
  public void getDataModels(HttpServiceRequest request, HttpServiceResponder responder,
                        @PathParam("context") String namespace) {
    respond(request, responder, namespace, ns -> TransactionRunners.run(getContext(), context -> {
      DataModelStore store = DataModelStore.get(context);
      return new ServiceResponse<>(store.listDataModels(ns));
    }));
  }

  /**
   * Returns the data model definition.
   *
   * @param request Handler for incoming request.
   * @param responder Responder for data going out.
   * @param namespace Namespace of request.
   * @param id The id of the standard returned by the .../schemas listing.
   */
  @GET
  @Path("contexts/{context}/datamodels/{id}")
  @TransactionPolicy(value = TransactionControl.EXPLICIT)
  public void getDataModel(HttpServiceRequest request, HttpServiceResponder responder,
                        @PathParam("context") String namespace, @PathParam("id") String id) {
    respond(request, responder, namespace, ns -> TransactionRunners.run(getContext(), context -> {
      NamespacedId nId = new NamespacedId(ns, id);
      DataModelStore store = getStore(context, nId);
      return new ServiceResponse<>(store.readDataModelDescriptor(nId));
    }));
  }

  /**
   * Returns all of the models defined within the data model.
   *
   * @param request Handler for incoming request.
   * @param responder Responder for data going out.
   * @param namespace Namespace of request.
   * @param id The id of the data model returned by the data models listing.
   */
  @GET
  @Path("contexts/{context}/datamodels/{id}/models")
  @TransactionPolicy(value = TransactionControl.EXPLICIT)
  public void getModels(HttpServiceRequest request, HttpServiceResponder responder,
                                @PathParam("context") String namespace, @PathParam("id") String id) {
    respond(request, responder, namespace, ns -> TransactionRunners.run(getContext(), context -> {
      NamespacedId nId = new NamespacedId(ns, id);
      DataModelStore store = getStore(context, nId);
      return new ServiceResponse<>(store.listModels(nId));
    }));
  }

  /**
   * Returns the definition of a model.
   *
   * @param request Handler for incoming request.
   * @param responder Responder for data going out.
   * @param namespace Namespace of request.
   * @param id The id of the data model returned by the data models listing.
   * @param name The name of the entity.
   */
  @GET
  @Path("contexts/{context}/datamodels/{id}/models/{name}")
  @TransactionPolicy(value = TransactionControl.EXPLICIT)
  public void getModel(HttpServiceRequest request, HttpServiceResponder responder,
                                        @PathParam("context") String namespace,
                                        @PathParam("id") String id,
                                        @PathParam("name") String name) {
    respond(request, responder, namespace, ns -> TransactionRunners.run(getContext(), context -> {
      NamespacedId nId = new NamespacedId(ns, id);
      DataModelStore store = getStore(context, nId);
      Object model = store.getModel(nId, name);
      if (model == null) {
        throw new NotFoundException(String.format("Entity name %s not found.", name));
      }
      return new ServiceResponse<>(model);
    }));
  }

  private DataModelStore getStore(StructuredTableContext context, NamespacedId id)
      throws NotFoundException, IOException {
    DataModelStore store = DataModelStore.get(context);
    if (!store.hasDataModel(id)) {
      throw new NotFoundException(String.format("Standard schema id %s not found.", id));
    }
    return store;
  }
}
