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

package co.cask.wrangler.service.schema;

import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.service.http.HttpServiceContext;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import co.cask.wrangler.dataset.schema.SchemaDescriptor;
import co.cask.wrangler.dataset.schema.SchemaRegistry;
import co.cask.wrangler.proto.BadRequestException;
import co.cask.wrangler.proto.NamespacedId;
import co.cask.wrangler.proto.NotFoundException;
import co.cask.wrangler.proto.ServiceResponse;
import co.cask.wrangler.proto.schema.SchemaDescriptorType;
import co.cask.wrangler.proto.schema.SchemaEntryVersion;
import co.cask.wrangler.service.common.AbstractWranglerHandler;
import com.google.gson.Gson;

import java.nio.ByteBuffer;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

/**
 * This class {@link SchemaRegistryHandler} provides schema management service.
 */
public class SchemaRegistryHandler extends AbstractWranglerHandler {
  private static final Gson GSON = new Gson();

  @UseDataSet(SchemaRegistry.DATASET_NAME)
  private Table table;

  private SchemaRegistry registry;

  /**
   * An implementation of HttpServiceHandler#initialize(HttpServiceContext). Stores the context
   * so that it can be used later.
   *
   * @param context the HTTP service runtime context
   * @throws Exception
   */
  @Override
  public void initialize(HttpServiceContext context) throws Exception {
    super.initialize(context);
    registry = new SchemaRegistry(table);
  }

  @PUT
  @Path("schemas")
  public void create(HttpServiceRequest request, HttpServiceResponder responder,
                     @QueryParam("id") String id, @QueryParam("name") String name,
                     @QueryParam("description") String description, @QueryParam("type") String type) {
    create(request, responder, getContext().getNamespace(), id, name, description, type);
  }

  /**
   * Creates an entry for Schema with id, name, description and type of schema.
   * if the 'id' already exists, then it overwrites the data with new information.
   * This responds with HTTP - OK (200) or Internal Error (500).
   *
   * @param request HTTP request handler.
   * @param responder HTTP response handler.
   * @param id of schema to be created.
   * @param name for the schema id being created.
   * @param description associated with schema.
   * @param type of schema.
   */
  @PUT
  @Path("contexts/{context}/schemas")
  public void create(HttpServiceRequest request, HttpServiceResponder responder, @PathParam("context") String namespace,
                     @QueryParam("id") String id, @QueryParam("name") String name,
                     @QueryParam("description") String description, @QueryParam("type") String type) {
    respond(request, responder, namespace, () -> {
      if (id == null || id.isEmpty()) {
        throw new BadRequestException("Schema id must be specified.");
      }
      if (name == null || name.isEmpty()) {
        throw new BadRequestException("Schema name must be specified.");
      }
      SchemaDescriptorType descriptorType = GSON.fromJson(type, SchemaDescriptorType.class);
      if (descriptorType == null) {
        throw new BadRequestException(String.format("Schema type '%s' is invalid.", type));
      }
      if (description == null || description.isEmpty()) {
        throw new BadRequestException("Schema description must be specified.");
      }
      SchemaDescriptor descriptor = new SchemaDescriptor(NamespacedId.of(namespace, id),
                                                         name, description, descriptorType);
      registry.write(descriptor);
      return new ServiceResponse<Void>(String.format("Successfully created schema entry with id '%s', name '%s'",
                                                     id, name));
    });
  }

  @POST
  @Path("schemas/{id}")
  public void upload(HttpServiceRequest request, HttpServiceResponder responder,
                     @PathParam("id") String id) {
    upload(request, responder, getContext().getNamespace(), id);
  }

  /**
   * Uploads a schema to be associated with the schema id.
   * This API will automatically increment the schema version. Upon adding the schema to associated id in
   * schema registry, it returns the version to which the uploaded schema was added to.
   *
   * Following is the response when it's HTTP OK(200)
   * {
   *   "status" : "OK",
   *   "message" : "Success",
   *   "count" : 1,
   *   "values" : [
   *      {
   *        "id" : <id-passed-in-REST-call>
   *        "version" : <version-schema-written-to>
   *      }
   *   ]
   * }
   *
   * On any issues, returns error with proper error message and Internal Server error (500).
   *
   * @param request HTTP request handler.
   * @param responder HTTP response handler.
   * @param id of the schema being uploaded.
   */
  @POST
  @Path("contexts/{context}/schemas/{id}")
  public void upload(HttpServiceRequest request, HttpServiceResponder responder, @PathParam("context") String namespace,
                     @PathParam("id") String id) {
    respond(request, responder, namespace, () -> {
      byte[] bytes = null;
      ByteBuffer content = request.getContent();
      if (content != null && content.hasRemaining()) {
        bytes = new byte[content.remaining()];
        content.get(bytes);
      }

      if (bytes == null) {
        throw new BadRequestException("No schema was provided in the request body");
      }

      NamespacedId namespacedId = NamespacedId.of(namespace, id);
      long version = registry.add(namespacedId, bytes);
      return new ServiceResponse<>(new SchemaEntryVersion(namespacedId, version));
    });
  }

  @DELETE
  @Path("schemas/{id}")
  public void delete(HttpServiceRequest request, HttpServiceResponder responder,
                     @PathParam("id") String id) {
    delete(request, responder, getContext().getNamespace(), id);
  }

  /**
   * Deletes the entire entry from the registry.
   *
   * Everything related to the schema id is deleted completely.
   * All versions of schema are also deleted.
   *
   * @param request HTTP request handler.
   * @param responder HTTP response handler.
   * @param id of the schema to be deleted.
   */
  @DELETE
  @Path("contexts/{context}/schemas/{id}")
  public void delete(HttpServiceRequest request, HttpServiceResponder responder, @PathParam("context") String namespace,
                     @PathParam("id") String id) {
    respond(request, responder, namespace, () -> {
      NamespacedId namespacedId = NamespacedId.of(getContext().getNamespace(), id);
      if (registry.hasSchema(namespacedId)) {
        throw new NotFoundException("Id " + id + " not found.");
      }
      registry.delete(namespacedId);
      return new ServiceResponse<Void>("Successfully deleted schema " + id);
    });
  }

  @DELETE
  @Path("schemas/{id}/versions/{version}")
  public void delete(HttpServiceRequest request, HttpServiceResponder responder,
                     @PathParam("id") String id, @PathParam("version") long version) {
    delete(request, responder, getContext().getNamespace(), id, version);
  }

  /**
   * Deletes a version of schema from the registry for a given schema id.
   *
   * @param request HTTP request handler.
   * @param responder HTTP response handler.
   * @param id of the schema
   * @param version version of the schema.
   */
  @DELETE
  @Path("contexts/{context}/schemas/{id}/versions/{version}")
  public void delete(HttpServiceRequest request, HttpServiceResponder responder, @PathParam("context") String namespace,
                     @PathParam("id") String id, @PathParam("version") long version) {
    respond(request, responder, namespace, () -> {
      registry.remove(NamespacedId.of(namespace, id), version);
      return new ServiceResponse<Void>("Successfully deleted version '" + version + "' of schema " + id);
    });
  }

  @GET
  @Path("schemas/{id}/versions/{version}")
  public void get(HttpServiceRequest request, HttpServiceResponder responder,
                  @PathParam("id") String id, @PathParam("version") long version) {
    get(request, responder, getContext().getNamespace(), id, version);
  }

  /**
   * Returns information of schema, including schema requested along with versions available and other metadata.
   *
   * @param request HTTP request handler.
   * @param responder HTTP response handler.
   * @param id of the schema.
   * @param version of the schema.
   */
  @GET
  @Path("contexts/{context}/schemas/{id}/versions/{version}")
  public void get(HttpServiceRequest request, HttpServiceResponder responder, @PathParam("context") String namespace,
                  @PathParam("id") String id, @PathParam("version") long version) {
    respond(request, responder, namespace,
            () -> new ServiceResponse<>(registry.getEntry(NamespacedId.of(namespace, id), version)));
  }


  @GET
  @Path("schemas/{id}")
  public void get(HttpServiceRequest request, HttpServiceResponder responder,
                  @PathParam("id") String id) {
    get(request, responder, getContext().getNamespace(), id);
  }

  /**
   * Returns information of schema, including schema requested along with versions available and other metadata.
   * This call will automatically detect the currect active version of schema.
   *
   * @param request HTTP request handler.
   * @param responder HTTP response handler.
   * @param id of the schema.
   */
  @GET
  @Path("contexts/{context}/schemas/{id}")
  public void get(HttpServiceRequest request, HttpServiceResponder responder,
                  @PathParam("context") String namespace, @PathParam("id") String id) {
    respond(request, responder, namespace,
            () -> new ServiceResponse<>(registry.getEntry(NamespacedId.of(namespace, id))));
  }

  @GET
  @Path("schemas/{id}/versions")
  public void versions(HttpServiceRequest request, HttpServiceResponder responder, @PathParam("id") String id) {
    versions(request, responder, getContext().getNamespace(), id);
  }

  /**
   * Returns list of versions for a give schema id.
   *
   * @param request HTTP request handler.
   * @param responder HTTP response handler.
   * @param id of the schema.
   */
  @GET
  @Path("contexts/{context}/schemas/{id}/versions")
  public void versions(HttpServiceRequest request, HttpServiceResponder responder,
                       @PathParam("context") String namespace, @PathParam("id") String id) {
    respond(request, responder, namespace,
            () -> new ServiceResponse<>(registry.getVersions(NamespacedId.of(namespace, id))));
  }
}
