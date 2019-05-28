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

package io.cdap.wrangler.service.filesystem;

import io.cdap.cdap.api.app.AbstractApplication;
import io.cdap.cdap.api.dataset.lib.FileSet;
import io.cdap.cdap.api.dataset.lib.FileSetProperties;
import io.cdap.wrangler.service.explorer.FilesystemExplorer;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class TestApp extends AbstractApplication {
  /**
   * Override this method to declare and configure the application.
   */
  @Override
  public void configure() {
    setName("dataprep");
    setDescription("DataPrep Backend Service");
    // Used by the file service.
    createDataset("indexds", FileSet.class, FileSetProperties.builder()
      .setBasePath("dataprep/indexds")
      .setInputFormat(TextInputFormat.class)
      .setOutputFormat(TextOutputFormat.class)
      .setDescription("Store Dataset Index files")
      .build());
    addService("service", new FilesystemExplorer());
  }
}
