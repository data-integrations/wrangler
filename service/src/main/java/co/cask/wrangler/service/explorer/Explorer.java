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

package co.cask.wrangler.service.explorer;

import co.cask.cdap.api.dataset.lib.FileSet;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import org.apache.commons.io.Charsets;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

/**
 * File system explorer.
 */
public final class Explorer {
  private static final Logger LOG = LoggerFactory.getLogger(Explorer.class);

  // Dataset provider interface.
  private final DatasetProvider provider;

  // Operating system this service is running on.
  private final String operatingSystem;

  // Map of file extensions to MIME names.
  private final Map<String, String> extensions;

  // Some constants for unknown or device types.
  public final static String DEVICE = "device";
  public final static String UNKNOWN = "UNKNOWN";

  public Explorer(DatasetProvider provider) {
    this.provider = provider;
    String os = System.getProperty("os.name");
    if (os == null || os.isEmpty() ) {
      this.operatingSystem = "unknown";
    } else {
      this.operatingSystem = os.toLowerCase();
    }

    this.extensions = new HashMap<>();
    File file = new File(Explorer.class.getClassLoader().getResource("file.extensions").getFile());
    try {
      Scanner scanner = new Scanner(file);
      while(scanner.hasNext()) {
        String line = scanner.nextLine();
        String[] parts = line.split("\t");
        if (parts.length == 2) {
          extensions.put(parts[0], parts[1]);
        }
      }
      scanner.close();
    } catch (FileNotFoundException e) {
      LOG.warn("Unable to load extension map. File 'file.extensions' not packaged.");
    }
  }

  /**
   * Explores the path provided on the filesystem.
   *
   * @param path to be explored.
   * @return Information about all the files/directories in the path.
   * @throws ExplorerException thrown when there is issue browsing directory.
   */
  public Map<String, Object> browse(String path, boolean hidden) throws ExplorerException {
    try {
      Map<String, Object> response = new HashMap<>();
      List<Map<String, Object>> values = new ArrayList<>();
      // Trick in getting the location.
      Location base = getLocation(path);
      // Get the list of all the files.
      List<Location> locations = base.list();
      // Iterate through each file.
      for(Location location : locations) {
        // If hidden is true, then hide all the files that start with . (dot)
        if(hidden && location.getName().toString().startsWith(".")) {
          continue;
        }
        Map<String, Object> object = locationInfo(location);
        // If it's a directory, inspect the contents further attempting to detect the type
        String type = guessLocationType(location, 1);
        boolean isWrangleable = isWrangleable(type);
        object.put("type", type);
        object.put("wrangle", isWrangleable);
        values.add(object);
      }
      response.put("status", HttpURLConnection.HTTP_OK);
      response.put("message", "Success");
      response.put("count", values.size());
      response.put("values", values);
      return response;
    } catch (IOException e){
      throw new ExplorerException(e);
    }
  }

  /**
   * This function checks if the type is wrangle-able of not.
   *
   * <p>It detects it based on the type, currently we only support
   * types that are of MIME type 'text'</p>
   *
   * @param type Specifies the MIME type.
   * @return true if it's wrangle-able, false otherwise.
   */
  private boolean isWrangleable(String type) {
    if ("text/plain".equalsIgnoreCase(type)
        || "application/json".equalsIgnoreCase(type)
        || "application/xml".equalsIgnoreCase(type)
        || "application/avro".equalsIgnoreCase(type)
        || "application/protobuf".equalsIgnoreCase(type)
        || "application/excel".equalsIgnoreCase(type)
      ) {
      return true;
    }
    return false;
  }

  /**
   * This methods provides an efficiently way to read a file from the file system specified by
   * the <code>Location.</code>. It uses a bounded line stream that limits the lines being read
   * from the {@link java.io.BufferedInputStream}.
   *
   * @param path      Specifies the path to file to be read. Assumes the file exists at the path specified.
   * @param encoding  Specifies the encoding of the file.
   * @param lines     Number of lines to be read from the file.
   * @return {@link BoundedLineInputStream}
   */
  public BoundedLineInputStream read(String path, Charset encoding, int lines) throws ExplorerException, IOException {
    Location file = getLocation(path);
    if (file.isDirectory()) {
      throw new ExplorerException(
        String.format("Path '%s' specified is a directory and not a file.", file.toURI().getPath())
      );
    }
    return BoundedLineInputStream.iterator(file.getInputStream(), encoding, lines);
  }

  public BoundedLineInputStream read(String path, String encoding, int lines) throws ExplorerException, IOException {
    return read(path, Charsets.toCharset(encoding), lines);
  }

  /**
   * Reads the 'size' bytes from the file.
   *
   * @param path to the file being read.
   * @param size specifies the bytes to be read.
   * @return bytes read.
   */
  public byte[] read(String path, int size) throws ExplorerException, IOException {
    Location file = getLocation(path);
    if (file.isDirectory()) {
      throw new ExplorerException(
        String.format("Path '%s' specified is a directory and not a file.", file.toURI().getPath())
      );
    }

    int min = (int) Math.min(file.length(), size);
    byte[] buffer  = new byte[min + 1];
    IOUtils.read(file.getInputStream(), buffer);
    return buffer;
  }

  /**
   * Inspects the location to detect the type of the file.
   *
   * @param path of the file or path to be investigated.
   * @param lookahead Specifies a look a head parameter.
   */
  private String guessLocationType(Location path, int lookahead) {
    try {
      // If we have gone beyond, we exit immediately.
      if (lookahead < 0) {
        return UNKNOWN;
      }

      if (!path.isDirectory()) {
        return detectFileType(path);
      } else {
        Multiset<String> types = HashMultiset.create();
        List<Location> listing = path.list();
        if (listing.size() > 0) {
          for (Location location : path.list()) {
            String type = guessLocationType(location, lookahead - 1);
            types.add(type);
          }
          String topType = UNKNOWN;
          for (Multiset.Entry<String> top : types.entrySet()) {
            if(topType.equalsIgnoreCase(UNKNOWN)) {
              topType = top.getElement();
            }
          }
          return topType;
        }
      }
    } catch (IOException e) {
      // We might not have permission, so ignore on look-ahead.
    }
    return UNKNOWN;
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
    if (extensions.containsKey(extension)) {
      return extensions.get(extension);
    }
    return UNKNOWN;
  }

  /**
   * Returns a map of location info collected.
   *
   * @param location who's information need to be extracted.
   * @return an instance of JSON Object.
   * @throws IOException thrown in case of issues with listing.
   */
  private Map<String, Object> locationInfo(Location location) throws IOException {
    Map<String, Object> response = new HashMap<>();
    response.put("directory", location.isDirectory());
    response.put("path", location.toURI().getPath());
    response.put("name", location.getName());
    response.put("group", location.getGroup());
    response.put("owner", location.getOwner());
    response.put("permission", location.getPermissions());
    response.put("size", location.length());
    response.put("last-modified", location.lastModified());
    response.put("uri", location.toURI().toString());
    return response;
  }

  /**
   * Trick, to extract the URI for a location.
   *
   * @param path to a Dataset.
   * @return Location
   * @throws URISyntaxException issue constructing the URI.
   */
  public Location getLocation(String path) throws ExplorerException {
    FileSet fileset = null;
    try {
      fileset = (FileSet) provider.acquire();
      Location baseLocation = fileset.getBaseLocation();
      provider.release(fileset);

      URI uri = baseLocation.toURI();
      Location location = baseLocation.getLocationFactory().create(
        new URI(uri.getScheme(), uri.getUserInfo(), uri.getHost(), uri.getPort(),
                path, null, null));
      return location;
    } catch (Exception e) {
      throw new ExplorerException(e);
    }
  }
}
