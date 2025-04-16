/*
 *  Copyright © 2017-2019 Cask Data, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *  use this file except in compliance with the License. You may obtain a copy of
 *  the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations under
 *  the License.
 */

package io.cdap.wrangler.service.explorer;

import com.google.api.gax.paging.Page;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import io.cdap.wrangler.service.gcp.GCPUtils;
import io.cdap.wrangler.service.gcs.GCSHandler;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.net.HttpURLConnection;
import java.util.Iterator;

/**
 * Tests {@link GCSHandler}
 */
@Ignore
public class GCSServiceTest {

  @Ignore
  private JsonObject list(String path) throws Exception {
    String key = "/tmp/cask-dev-clusters-729251f6adf7.json";
    ServiceAccountCredentials credentials = GCPUtils.loadLocalFile(key);

    Storage storage = StorageOptions.newBuilder()
      .setProjectId("cask-dev-clusters")
      .setCredentials(credentials)
      .build()
      .getService();

    String bucketName = "";
    String prefix = null;
    int bucketStart = path.indexOf("/");
    if (bucketStart != -1) {
      int bucketEnd = path.indexOf("/", bucketStart + 1);
      if (bucketEnd != -1) {
        bucketName = path.substring(bucketStart + 1, bucketEnd);
        if ((bucketEnd + 1) != path.length()) {
          prefix = path.substring(bucketEnd + 1);
        }
      } else {
        bucketName = path.substring(bucketStart + 1);
      }
    }

    if (bucketName.isEmpty() && prefix == null) {
      Page<Bucket> list = storage.list();
      Iterator<Bucket> iterator = list.getValues().iterator();
      JsonObject response = new JsonObject();
      response.addProperty("status", HttpURLConnection.HTTP_OK);
      response.addProperty("message", "OK");
      JsonArray values = new JsonArray();
      while (iterator.hasNext()) {
        com.google.cloud.storage.Bucket bucket = iterator.next();
        JsonObject object = new JsonObject();
        object.addProperty("name", bucket.getName());
        object.addProperty("created", bucket.getCreateTime());
        object.addProperty("etag", bucket.getEtag());
        object.addProperty("generated-id", bucket.getGeneratedId());
        object.addProperty("meta-generation", bucket.getMetageneration());
        values.add(object);
      }
      response.addProperty("count", values.size());
      response.add("values", values);
      return response;
    }

    Page<Blob> list = null;
    if (prefix == null) {
      list = storage.list(bucketName, Storage.BlobListOption.currentDirectory());
    } else {
      list = storage.list(bucketName, Storage.BlobListOption.currentDirectory(),
                          Storage.BlobListOption.prefix(prefix));
    }

    Iterator<Blob> iterator = list.iterateAll().iterator();
    JsonArray values = new JsonArray();
    while (iterator.hasNext()) {
      JsonObject object = new JsonObject();
      Blob blob = iterator.next();

      object.addProperty("bucket", blob.getBucket());
      object.addProperty("name", blob.getName().replaceFirst(prefix, ""));
      object.addProperty("path", blob.getName());
      object.addProperty("generation", blob.getGeneration());
      object.addProperty("created", blob.getCreateTime());
      object.addProperty("md5", blob.getMd5());
      object.addProperty("size", blob.getSize());
      object.addProperty("content-type", blob.getContentType());
      values.add(object);
    }
    JsonObject response = new JsonObject();
    response.addProperty("status", HttpURLConnection.HTTP_OK);
    response.addProperty("message", "OK");
    response.addProperty("count", values.size());
    response.add("values", values);
    return response;
  }

  @Test
  @Ignore
  public void testExploreGCS() throws Exception {
    list("/cdap/audio/raw");
    Assert.assertTrue(true);
  }

}
