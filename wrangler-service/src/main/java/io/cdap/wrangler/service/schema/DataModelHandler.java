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
import io.cdap.wrangler.proto.Contexts;
import io.cdap.wrangler.proto.Namespace;
import io.cdap.wrangler.proto.NamespacedId;
import io.cdap.wrangler.proto.NotFoundException;
import io.cdap.wrangler.proto.ServiceResponse;
import io.cdap.wrangler.proto.schema.DataModelSchemaEntry;
import io.cdap.wrangler.service.common.AbstractWranglerHandler;
import io.cdap.wrangler.utils.AvroSchemaGlossary;
import io.cdap.wrangler.utils.PackagedSchemaLoader;

import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * The {@link DataModelHandler} class provides data model schema listings APIs.
 */
public class DataModelHandler extends AbstractWranglerHandler {

  private static final Logger LOG = LoggerFactory.getLogger(DataModelHandler.class);
  private static final AvroSchemaGlossary GLOSSARY;

  static {
    PackagedSchemaLoader loader = new PackagedSchemaLoader(
      DataModelHandler.class.getClassLoader(), "schemas", "manifest.json");
    GLOSSARY = new AvroSchemaGlossary(loader);
    if (!GLOSSARY.configure()) {
      LOG.error("unable to load system data model schemas");
    }
  }

  @GET
  @Path("v2/contexts/{context}/schemas")
  @TransactionPolicy(value = TransactionControl.EXPLICIT)
  public void listSchemas(HttpServiceRequest request, HttpServiceResponder responder,
                          @PathParam("context") String namespace) {
    respond(request, responder, namespace, ns -> TransactionRunners.run(getContext(), context -> {
      Namespace defaultNamespace = new Namespace(Contexts.SYSTEM, 0L);
      List<DataModelSchemaEntry> results = new ArrayList<>();
      for (Schema schema : GLOSSARY.getAll()) {
        results.add(new DataModelSchemaEntry(new NamespacedId(defaultNamespace, schema.getFullName()),
                                             schema.getName(),
                                             schema.getDoc(),
                                             Long.parseLong(schema.getProp("_revision"))
        ));
      }
      return new ServiceResponse<>(results);
    }));
  }

  @GET
  @Path("v2/contexts/{context}/schemas/{id}/revisions/{revision}")
  @TransactionPolicy(value = TransactionControl.EXPLICIT)
  public void listSchema(HttpServiceRequest request, HttpServiceResponder responder,
                         @PathParam("context") String namespace,
                         @PathParam("id") String id, @PathParam("revision") long revision) {
    respond(request, responder, namespace, ns -> TransactionRunners.run(getContext(), context -> {
      Schema schema = GLOSSARY.get(id, revision);
      if (schema == null) {
        throw new NotFoundException(String.format("unable to find schema %s revision %d", id, revision));
      }
      return new ServiceResponse<>(schema.toString());
    }));
  }
}
