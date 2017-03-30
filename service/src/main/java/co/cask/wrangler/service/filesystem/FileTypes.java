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

/**
 * Specifies the type of the files being detected.
 */
public final class FileTypes {
  public final static String TEXT = "application/text";
  public final static String SEQUENCE = "application/sequence";
  public final static String PARQUET = "application/parquet";
  public final static String AVRO = "application/avro";
  public final static String HBASE = "application/hbase";
  public final static String HIVE = "application/hive";
  public final static String CDAP = "application/cdap";
  public final static String UNKNOWN = "UNKNOWN";
}
