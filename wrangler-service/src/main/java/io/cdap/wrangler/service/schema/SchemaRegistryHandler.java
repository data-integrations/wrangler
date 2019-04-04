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

package io.cdap.wrangler.service.schema;

import com.google.gson.Gson;
import io.cdap.cdap.api.annotation.TransactionControl;
import io.cdap.cdap.api.annotation.TransactionPolicy;
import io.cdap.cdap.api.service.http.HttpServiceRequest;
import io.cdap.cdap.api.service.http.HttpServiceResponder;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import io.cdap.wrangler.dataset.schema.SchemaDescriptor;
import io.cdap.wrangler.dataset.schema.SchemaRegistry;
import io.cdap.wrangler.proto.BadRequestException;
import io.cdap.wrangler.proto.NamespacedId;
import io.cdap.wrangler.proto.NotFoundException;
import io.cdap.wrangler.proto.ServiceResponse;
import io.cdap.wrangler.proto.schema.SchemaDescriptorType;
import io.cdap.wrangler.proto.schema.SchemaEntryVersion;
import io.cdap.wrangler.service.common.AbstractWranglerHandler;

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
  @TransactionPolicy(value = TransactionControl.EXPLICIT)
  public void create(HttpServiceRequest request, HttpServiceResponder responder, @PathParam("context") String namespace,
                     @QueryParam("id") String id, @QueryParam("name") String name,
                     @QueryParam("description") String description, @QueryParam("type") String type) {
    respond(request, responder, namespace, ns -> {
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
      SchemaDescriptor descriptor = new SchemaDescriptor(new NamespacedId(ns, id),
                                                         name, description, descriptorType);
      TransactionRunners.run(getContext(), context -> {
        SchemaRegistry registry = SchemaRegistry.get(context);
        registry.write(descriptor);
      });
      return new ServiceResponse<Void>(String.format("Successfully created schema entry with id '%s', name '%s'",
                                                     id, name));
    });
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
  @TransactionPolicy(value = TransactionControl.EXPLICIT)
  public void upload(HttpServiceRequest request, HttpServiceResponder responder, @PathParam("context") String namespace,
                     @PathParam("id") String id) {
    respond(request, responder, namespace, ns -> {
      byte[] bytes;
      ByteBuffer content = request.getContent();
      if (content != null && content.hasRemaining()) {
        bytes = new byte[content.remaining()];
        content.get(bytes);
      } else {
        bytes = null;
      }

      if (bytes == null) {
        throw new BadRequestException("No schema was provided in the request body");
      }

      NamespacedId namespacedId = new NamespacedId(ns, id);
      long version = TransactionRunners.run(getContext(), context -> {
        SchemaRegistry registry = SchemaRegistry.get(context);
        return registry.add(namespacedId, bytes);
      });
      return new ServiceResponse<>(new SchemaEntryVersion(namespacedId, version));
    });
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
  @TransactionPolicy(value = TransactionControl.EXPLICIT)
  public void delete(HttpServiceRequest request, HttpServiceResponder responder, @PathParam("context") String namespace,
                     @PathParam("id") String id) {
    respond(request, responder, namespace, ns -> {
      NamespacedId namespacedId = new NamespacedId(ns, id);
      TransactionRunners.run(getContext(), context -> {
        SchemaRegistry registry = SchemaRegistry.get(context);
        if (registry.hasSchema(namespacedId)) {
          throw new NotFoundException("Id " + id + " not found.");
        }
        registry.delete(namespacedId);
      });
      return new ServiceResponse<Void>("Successfully deleted schema " + id);
    });
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
  @TransactionPolicy(value = TransactionControl.EXPLICIT)
  public void delete(HttpServiceRequest request, HttpServiceResponder responder, @PathParam("context") String namespace,
                     @PathParam("id") String id, @PathParam("version") long version) {
    respond(request, responder, namespace, ns -> {
      TransactionRunners.run(getContext(), context -> {
        SchemaRegistry registry = SchemaRegistry.get(context);
        registry.remove(new NamespacedId(ns, id), version);
      });
      return new ServiceResponse<Void>("Successfully deleted version '" + version + "' of schema " + id);
    });
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
  @TransactionPolicy(value = TransactionControl.EXPLICIT)
  public void get(HttpServiceRequest request, HttpServiceResponder responder, @PathParam("context") String namespace,
                  @PathParam("id") String id, @PathParam("version") long version) {
    respond(request, responder, namespace, ns -> TransactionRunners.run(getContext(), context -> {
      SchemaRegistry registry = SchemaRegistry.get(context);
      return new ServiceResponse<>(registry.getEntry(new NamespacedId(ns, id), version));
    }));
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
  @TransactionPolicy(value = TransactionControl.EXPLICIT)
  public void get(HttpServiceRequest request, HttpServiceResponder responder,
                  @PathParam("context") String namespace, @PathParam("id") String id) {
    respond(request, responder, namespace, ns -> TransactionRunners.run(getContext(), context -> {
      SchemaRegistry registry = SchemaRegistry.get(context);
      return new ServiceResponse<>(registry.getEntry(new NamespacedId(ns, id)));
    }));
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
  @TransactionPolicy(value = TransactionControl.EXPLICIT)
  public void versions(HttpServiceRequest request, HttpServiceResponder responder,
                       @PathParam("context") String namespace, @PathParam("id") String id) {
    respond(request, responder, namespace, ns -> TransactionRunners.run(getContext(), context -> {
      SchemaRegistry registry = SchemaRegistry.get(context);
      return new ServiceResponse<>(registry.getVersions(new NamespacedId(ns, id)));
    }));
  }
}
