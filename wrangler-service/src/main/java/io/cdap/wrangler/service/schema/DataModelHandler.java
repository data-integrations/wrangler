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
package io.cdap.wrangler.service.schema;

import io.cdap.cdap.api.annotation.TransactionControl;
import io.cdap.cdap.api.annotation.TransactionPolicy;
import io.cdap.cdap.api.service.http.HttpServiceRequest;
import io.cdap.cdap.api.service.http.HttpServiceResponder;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import io.cdap.wrangler.RequestExtractor;
import io.cdap.wrangler.datamodel.DataModelGlossary;
import io.cdap.wrangler.proto.BadRequestException;
import io.cdap.wrangler.proto.Contexts;
import io.cdap.wrangler.proto.Namespace;
import io.cdap.wrangler.proto.NamespacedId;
import io.cdap.wrangler.proto.NotFoundException;
import io.cdap.wrangler.proto.ServiceResponse;
import io.cdap.wrangler.proto.schema.DataModelSchemaEntry;
import io.cdap.wrangler.proto.schema.DataModelUrlRequest;
import io.cdap.wrangler.service.common.AbstractWranglerHandler;

import org.apache.avro.Schema;

import java.util.ArrayList;
import java.util.List;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * The {@link DataModelHandler} class provides data model schema listings APIs.
 */
public class DataModelHandler extends AbstractWranglerHandler {


  /**
   * Uploads a one or more data model schemas into the internal data model glossary.
   * The full name and revision of each data model schema form a unique identify.
   *
   * Returns:
   *  200 - if data model successfully added.
   *  400 - if the request is missing a url parameter in the body.
   *  400 - if the server was unable to load the data model at the specified url.
   *
   * @param request the HTTP request handler.
   * @param responder the HTTP response handler.
   * @param namespace the context of the request.
   */
  @POST
  @Path("contexts/{context}/datamodels/schemas")
  @TransactionPolicy(value = TransactionControl.EXPLICIT)
  public void upload(HttpServiceRequest request, HttpServiceResponder responder,
                     @PathParam("context") String namespace) {
    respond(request, responder, namespace, ns -> {
      RequestExtractor handler = new RequestExtractor(request);
      DataModelUrlRequest dataModelUrl = handler.getContent("UTF-8", DataModelUrlRequest.class);
      if (dataModelUrl == null || dataModelUrl.getUrl() == null) {
        throw new BadRequestException("Request body is missing required url field.");
      }
      if (!DataModelGlossary.initialize(dataModelUrl.getUrl())) {
        throw new BadRequestException("Unable to load data models.");
      }
      return new ServiceResponse<>(dataModelUrl);
    });
  }


  /**
   * Returns a list of all data models available. By default excludes the data model schemas and
   * only returns the meta data due to potentially large size of the schemas.
   *
   * Returns:
   *  200 - if successful.
   *  404 - if there no data models loaded.
   *
   * @param request the HTTP request handler.
   * @param responder the HTTP response handler.
   * @param namespace the context of the request.
   */
  @GET
  @Path("contexts/{context}/datamodels/schemas")
  @TransactionPolicy(value = TransactionControl.EXPLICIT)
  public void listSchemas(HttpServiceRequest request, HttpServiceResponder responder,
                          @PathParam("context") String namespace) {
    respond(request, responder, namespace, ns -> TransactionRunners.run(getContext(), context -> {
      if (DataModelGlossary.getGlossary() == null) {
        throw new NotFoundException("There is no data model initialized.");
      }
      Namespace defaultNamespace = new Namespace(Contexts.SYSTEM, 0L);
      List<DataModelSchemaEntry> results = new ArrayList<>();
      for (Schema schema : DataModelGlossary.getGlossary().getAll()) {
        results.add(new DataModelSchemaEntry(new NamespacedId(defaultNamespace, schema.getFullName()),
                                             schema.getName(),
                                             schema.getDoc(),
                                             Long.parseLong(schema.getProp("_revision"))
        ));
      }
      return new ServiceResponse<>(results);
    }));
  }

  /**
   * Retrieves the schema of a specific revision of a data model.
   *
   * Returns:
   *  200 - if the appropriate schema version has been located.
   *  404 - if no schema matching the id or version specified exists.
   *
   * @param request the HTTP request handler.
   * @param responder the HTTP response handler.
   * @param namespace the context of the request.
   * @param id of the data model.
   * @param revision of the data model.
   */
  @GET
  @Path("contexts/{context}/datamodels/schemas/{id}/revisions/{revision}")
  @TransactionPolicy(value = TransactionControl.EXPLICIT)
  public void listSchema(HttpServiceRequest request, HttpServiceResponder responder,
                         @PathParam("context") String namespace,
                         @PathParam("id") String id, @PathParam("revision") long revision) {
    respond(request, responder, namespace, ns -> TransactionRunners.run(getContext(), context -> {
      if (DataModelGlossary.getGlossary() == null) {
        throw new NotFoundException("There is no data model initialized.");
      }
      Schema schema = DataModelGlossary.getGlossary().get(id, revision);
      if (schema == null) {
        throw new NotFoundException(String.format("unable to find schema %s revision %d", id, revision));
      }
      return new ServiceResponse<>(schema.toString());
    }));
  }
}
