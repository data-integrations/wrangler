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

package co.cask.wrangler.service.filesystem;

import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import com.j256.simplemagic.ContentInfo;
import com.j256.simplemagic.ContentInfoUtil;
import org.apache.commons.io.FilenameUtils;
import org.apache.twill.filesystem.Location;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;

import static co.cask.wrangler.service.directive.DirectivesService.error;
import static co.cask.wrangler.service.directive.DirectivesService.sendJson;

/**
 * A {@link FSBrowserService} is a HTTP Service handler for exploring the filesystem.
 * It provides capabilities for listing file(s) and directories. It also provides metadata.
 */
public class FSBrowserService extends AbstractHttpServiceHandler {
  private static final Logger LOG = LoggerFactory.getLogger(FSBrowserService.class);

  /**
   * Lists the content of the path specified using the {@Location}.
   *
   * @param request HTTP Request Handler
   * @param responder HTTP Response Handler
   * @param path to the location in the filesystem
   * @throws Exception
   */
  @Path("explorer")
  @GET
  public void list(HttpServiceRequest request, HttpServiceResponder responder,
                   @QueryParam("path") String path) throws Exception {

    try {
      JSONObject response = new JSONObject();
      JSONArray values = new JSONArray();
      // Trick in getting the location.
      Location base = getLocation(path);
      // Get the list of all the files.
      List<Location> locations = base.list();
      // Iterate through each file.
      for(Location location : locations) {
        JSONObject object = locationInfo(location);
        // If it's a directory, inspect the contents further attempting to detect the type.
        String type = guessLocationType(location, 1);
        object.put("type", type);
        values.put(object);
      }
      response.put("status", HttpURLConnection.HTTP_OK);
      response.put("message", "Success");
      response.put("count", values.length());
      response.put("values", values);
      sendJson(responder, HttpURLConnection.HTTP_OK, response.toString());
    } catch (IOException e) {
      error(responder, "Error exploring directory '" + path + "'. " + e.getMessage());
    } catch (URISyntaxException e) {
      error(responder, "Error detecting proper URI for the directory '" + path + "'. " + e.getMessage());
    }
  }

  /**
   * Inspects the location to detect the type of the file.
   *
   * @param path of the file or path to be investigated.
   * @param lookahead Specifies a look a head parameter.
   * @throws IOException
   */
  private String guessLocationType(Location path, int lookahead) throws IOException {
    if (path.exists() && !path.isDirectory()) {
      return detectFileType(path);
    } else {
      if (lookahead >= 0 && path.exists()) {
        Multiset<String> types = HashMultiset.create();
        for (Location location : path.list()) {
          String type = guessLocationType(location, lookahead - 1);
          types.add(type);
        }
        String topType = FileTypes.UNKNOWN;
        for (Multiset.Entry<String> top : types.entrySet()) {
          if(topType.equalsIgnoreCase(FileTypes.UNKNOWN)) {
            topType = top.getElement();
          }
        }
      }
    }

   return FileTypes.UNKNOWN;
  }

  /**
   * Attempts to detect the type of the file through extensions and by reading the content of the file.
   *
   * @param location of the file who's content type need to be detected.
   * @return type of the file.
   */
  private String detectFileType(Location location) throws IOException {
    // We first attempt to detect the type of file based on extension.
    String extension = FilenameUtils.getExtension(location.getName());
    switch (extension) {
      case "parquet":
        return FileTypes.PARQUET;

      case "avro":
        return FileTypes.AVRO;

      case "log":
      case "csv":
      case "txt":
        return FileTypes.TEXT;

      case "json":
        return FileTypes.JSON;

      case "md":
        return FileTypes.MARKDOWN;

      case "jar":
        return FileTypes.JAR;

      default:
        ContentInfoUtil util = new ContentInfoUtil();
        ContentInfo info = util.findMatch(location.getInputStream());
        if (info != null) {
          return info.getContentType().getMimeType();
        } else {
          return FileTypes.UNKNOWN;
        }
    }
  }

  /**
   * Creates a JSONObject of location information.
   *
   * @param location who's information need to be extracted.
   * @return an instance of JSON Object.
   * @throws IOException thrown in case of issues with listing.
   */
  private JSONObject locationInfo(Location location) throws IOException {
    JSONObject value = new JSONObject();
    value.put("directory", location.isDirectory());
    value.put("path", location.toURI().getPath());
    value.put("name", location.getName());
    value.put("group", location.getGroup());
    value.put("owner", location.getOwner());
    value.put("permission", location.getPermissions());
    value.put("size", location.length());
    value.put("last-modified", location.lastModified());
    return value;
  }

  /**
   * Trick, to extract the URI for a location.
   *
   * @param path to a Dataset.
   * @return Location
   * @throws URISyntaxException issue constructing the URI.
   */
  private Location getLocation(String path) throws URISyntaxException {
    FileSet fileset = getContext().getDataset("lines");
    Location baseLocation = fileset.getBaseLocation();
    getContext().discardDataset(fileset);

    URI uri = baseLocation.toURI();
    Location location = baseLocation.getLocationFactory().create(
      new URI(uri.getScheme(), uri.getUserInfo(), uri.getHost(), uri.getPort(),
              path, null, null));
    return location;
  }
}
