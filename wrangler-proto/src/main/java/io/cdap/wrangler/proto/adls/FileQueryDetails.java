/*
 * Copyright © 2018-2019 Cask Data, Inc.
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
package io.cdap.wrangler.proto.adls;

/**
 * A file utility encapsulating query parameters related to it.
 */
public class FileQueryDetails {

  private String filePath;
  private int lines;
  private String sampler;
  private double fraction;
  private String scope;
  private String header;

  public FileQueryDetails(String header, String filePath, int lines, String sampler, double fraction, String scope) {
    this.filePath = filePath;
    this.lines = lines;
    this.sampler = sampler;
    this.fraction = fraction;
    this.scope = scope;
    this.header = header;
  }

  public String getFilePath() {
    return filePath;
  }

  public String getHeader() {
    return header;
  }

  public int getLines() {
    return lines;
  }

  public String getSampler() {
    return sampler;
  }

  public double getFraction() {
    return fraction;
  }

  public String getScope() {
    return scope;
  }
}
