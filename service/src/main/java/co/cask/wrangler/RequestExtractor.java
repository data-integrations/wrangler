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

package co.cask.wrangler;

import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.wrangler.proto.Request;
import co.cask.wrangler.service.directive.RequestDeserializer;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

/**
 * This class {@link RequestExtractor} provides utility functions for extracting different aspects of the request.
 * It provides functionality to extract headers and content.
 */
public final class RequestExtractor {
  private final HttpServiceRequest request;
  public static final String CONTENT_TYPE_HEADER = PropertyIds.CONTENT_TYPE;
  public static final String CHARSET_HEADER = PropertyIds.CHARSET;

  public RequestExtractor(HttpServiceRequest request) {
    this.request = request;
  }

  /**
   * Checks if the request has a HTTP header.
   *
   * @param name of the header to be checked.
   * @return true if header is found, false otherwise.
   */
  public boolean hasHeader(String name) {
    String header = request.getHeader(name);
    return (header == null ? false : true);
  }

  /**
   * Extracts the HTTP header, if the header is not present, then default value is returned.
   *
   * @param name of the HTTP header to be extracted.
   * @param defaultValue value to returned if header doesn't exist.
   * @return value defined for the header.
   */
  public <T> T getHeader(String name, String defaultValue) {
    String header = request.getHeader(name);
    return (T) (header == null ? defaultValue : header);
  }

  /**
   * @return Content as received by the HTTP multipart/form body.
   */
  public byte[] getContent() {
    ByteBuffer content = request.getContent();
    if (content != null && content.hasRemaining()) {
      byte[] bytes = new byte[content.remaining()];
      content.get(bytes);
      return bytes;
    }
    return null;
  }

  /**
   * Returns the content by converting it to UNICODE from the provided charset.
   *
   * @param charset of the content being extracted and converted to UNICODE.
   * @return UNICODE representation of the content, else null.
   */
  public String getContent(Charset charset) {
    ByteBuffer content = request.getContent();
    if (content != null && content.hasRemaining()) {
      return charset.decode(content).toString();
    }
    return null;
  }

  public String getContent(String charset) {
    return getContent(Charset.forName(charset));
  }

  /**
   * Returns the content transformed into a Class defined.
   * It first transforms from the charset into unicode and then applies the transformation.
   *
   * @param charset source charset of the content.
   * @param type class to converted to.
   * @return instance of type T as defined by the class.
   */
  public <T> T getContent(String charset, Class<?> type) {
    String data = getContent(charset);
    if (data != null) {
      GsonBuilder builder = new GsonBuilder();
      builder.registerTypeAdapter(Request.class, new RequestDeserializer());
      Gson gson = builder.create();
      return (T) gson.fromJson(data, type);
    }
    return null;
  }

  /**
   * Checks if the 'Content-Type' matches expected.
   *
   * @param expectedType to be checked for content type.
   * @return true if it matches, false if it's not present or doesn't match expected.
   */
  public boolean isContentType(String expectedType) {
    if (hasHeader(CONTENT_TYPE_HEADER)) {
      String header = getHeader(CONTENT_TYPE_HEADER, null);
      return (header != null && !header.equalsIgnoreCase(expectedType)) ? false : true;
    }
    return false;
  }
}
