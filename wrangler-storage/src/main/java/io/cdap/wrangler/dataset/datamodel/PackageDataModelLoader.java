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
package io.cdap.wrangler.dataset.datamodel;

import com.google.gson.Gson;
import com.google.gson.JsonParser;
import io.cdap.wrangler.proto.Namespace;
import io.cdap.wrangler.proto.NamespacedId;
import io.cdap.wrangler.proto.datamodel.JsonSchemaDataModel;
import io.cdap.wrangler.proto.datamodel.JsonSchemaDataModelDescriptor;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Loads Json schema data model definitions stored as resources within the package.
 */
public class PackageDataModelLoader implements JsonSchemaLoader {

  private static final String LOADER_TYPE = "package";
  private static final String MANIFEST_FILE = "datamodels/manifest.txt";

  public PackageDataModelLoader() {}

  @Override
  public String getLoaderType() {
    return PackageDataModelLoader.LOADER_TYPE;
  }

  @Override
  public Collection<JsonSchemaDataModelDescriptor> load(Namespace namespace) throws IOException {
    List<JsonSchemaDataModelDescriptor> descriptors = new ArrayList<>();
    Gson gson = new Gson();
    InputStream manifest = PackageDataModelLoader.class.getClassLoader()
        .getResourceAsStream(PackageDataModelLoader.MANIFEST_FILE);
    try (BufferedReader br = new BufferedReader(new InputStreamReader(manifest))) {
      while (br.ready()) {
        String schemaId = br.readLine();
        InputStream schemaStream = PackageDataModelLoader.class.getClassLoader()
            .getResourceAsStream(String.format("datamodels/%s.json", schemaId));
        if (schemaStream != null) {
          JsonSchemaDataModelDescriptor descriptor = JsonSchemaDataModelDescriptor.builder()
              .setId(new NamespacedId(namespace, schemaId))
              .setJsonSchemaDataModel(gson.fromJson(new BufferedReader(new InputStreamReader(schemaStream)),
                                           JsonSchemaDataModel.class))
              .build();
          descriptors.add(descriptor);
        }
      }
    }
    return descriptors;
  }
}
