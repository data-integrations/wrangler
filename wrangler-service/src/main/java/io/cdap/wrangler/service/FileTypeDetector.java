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

package io.cdap.wrangler.service;

import org.apache.commons.io.FilenameUtils;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

/**
 * This class <code>FileTypeDetector</code> is used for detecting file types.
 */
public class FileTypeDetector {
  private static final Logger LOG = LoggerFactory.getLogger(FileTypeDetector.class);
  public static final String UNKNOWN = "UNKNOWN";

  // Map of file extensions to MIME names.
  private final Map<String, String> extensions;

  public FileTypeDetector() {
    this.extensions = new HashMap<>();
    try (Scanner scanner = new Scanner(getClass().getClassLoader().getResource("file.extensions").openStream(),
                                       "UTF-8")) {
      while (scanner.hasNext()) {
        String line = scanner.nextLine();
        String[] parts = line.split("\\s+");
        if (parts.length == 2) {
          extensions.put(parts[0], parts[1]);
        }
      }
    } catch (IOException e) {
      LOG.warn("Unable to load extension map.", e);
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
  public boolean isWrangleable(String type) {
    if ("text/plain".equalsIgnoreCase(type)
      || "application/json".equalsIgnoreCase(type)
      || "application/xml".equalsIgnoreCase(type)
      || "application/avro".equalsIgnoreCase(type)
      || "application/protobuf".equalsIgnoreCase(type)
      || "application/excel".equalsIgnoreCase(type)
      || type.contains("image/")
      || type.contains("text/")
      ) {
      return true;
    }
    return false;
  }

  /**
   * Attempts to detect the type of the file through extensions and by reading the content of the file.
   *
   * @param location of the file who's content type need to be detected.
   * @return type of the file.
   */
  public String detectFileType(Location location) {
    return detectFileType(location.getName());
  }

  /**
   * Attempts to detect the type of the file through extensions and by reading the content of the file.
   *
   * @param location of the file who's content type need to be detected.
   * @return type of the file.
   */
  public String detectFileType(String location) {
    // We first attempt to detect the type of file based on extension.
    String extension = FilenameUtils.getExtension(location);
    extension = extension == null ? null : extension.toLowerCase();
    if (extensions.containsKey(extension)) {
      return extensions.get(extension);
    } else {
      String name = FilenameUtils.getBaseName(location);
      if (name.equalsIgnoreCase(location)) {
        // CDAP-14397 return text type if there is no extension in the filename.
        return extensions.get("txt");
      }
      return detectFileType(name);
    }
  }
}
