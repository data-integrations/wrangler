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

package co.cask.wrangler.service;

import co.cask.wrangler.service.explorer.Explorer;
import org.apache.commons.io.FilenameUtils;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

/**
 * This class <code>FileTypeDetector</code> is used for detecting file types.
 */
public class FileTypeDetector {
  private static final Logger LOG = LoggerFactory.getLogger(FileTypeDetector.class);
  public final static String UNKNOWN = "UNKNOWN";

  // Map of file extensions to MIME names.
  private final Map<String, String> extensions;

  public FileTypeDetector() {
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
  public String detectFileType(Location location) throws IOException {
    return detectFileType(location.getName());
  }

  /**
   * Attempts to detect the type of the file through extensions and by reading the content of the file.
   *
   * @param location of the file who's content type need to be detected.
   * @return type of the file.
   */
  public String detectFileType(String location) throws IOException {
    // We first attempt to detect the type of file based on extension.
    String extension = FilenameUtils.getExtension(location);
    if (extensions.containsKey(extension)) {
      return extensions.get(extension);
    }
    return UNKNOWN;
  }
}
