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

package io.cdap.wrangler;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonParseException;
import io.cdap.cdap.api.service.http.HttpServiceRequest;
import io.cdap.wrangler.dataset.workspace.RequestDeserializer;
import io.cdap.wrangler.proto.BadRequestException;
import io.cdap.wrangler.proto.Request;
import io.cdap.wrangler.proto.connection.ConnectionMeta;
import io.cdap.wrangler.proto.connection.ConnectionType;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import javax.annotation.Nullable;

/**
 * This class {@link RequestExtractor} provides utility functions for extracting different aspects of the request.
 * It provides functionality to extract headers and content.
 */
public final class RequestExtractor {
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Request.class, new RequestDeserializer())
    .create();
  private final HttpServiceRequest request;
  public static final String CONTENT_TYPE_HEADER = PropertyIds.CONTENT_TYPE;
  public static final String CHARSET_HEADER = PropertyIds.CHARSET;

  public RequestExtractor(HttpServiceRequest request) {
    this.request = request;
  }


  /**
   * Extracts the HTTP header, if the header is not present, then default value is returned.
   *
   * @param name of the HTTP header to be extracted.
   * @param defaultValue value to returned if header doesn't exist.
   * @return value defined for the header.
   */
  public String getHeader(String name, String defaultValue) {
    String header = request.getHeader(name);
    return header == null ? defaultValue : header;
  }

  /**
   * @return Content as received by the HTTP multipart/form body.
   */
  @Nullable
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
  @Nullable
  public String getContent(Charset charset) {
    ByteBuffer content = request.getContent();
    if (content != null && content.hasRemaining()) {
      return charset.decode(content).toString();
    }
    return null;
  }

  @Nullable
  public String getContent(String charset) {
    return getContent(Charset.forName(charset));
  }

  /**
   * Get a ConnectionMeta object from the request body.
   *
   * @return the connection meta object
   * @throws IllegalArgumentException if the body is empty or not a valid ConnectionMeta
   */
  public ConnectionMeta getConnectionMeta() {
    String bodyStr = getContent(StandardCharsets.UTF_8);
    if (bodyStr == null) {
      throw new BadRequestException("No connection was found in the request body.");
    }
    ConnectionMeta connectionMeta;
    try {
      connectionMeta = GSON.fromJson(bodyStr, ConnectionMeta.class);
    } catch (JsonParseException e) {
      throw new BadRequestException(e.getMessage(), e);
    }
    connectionMeta.validate();
    return connectionMeta;
  }

  /**
   * Get a ConnectionMeta object from the request body.
   *
   * @return the connection meta object
   * @throws IllegalArgumentException if the body is empty or not a valid ConnectionMeta
   */
  public ConnectionMeta getConnectionMeta(ConnectionType expectedType) {
    ConnectionMeta meta = getConnectionMeta();
    if (expectedType != meta.getType()) {
      throw new BadRequestException(String.format("Expected connection of type '%s' but found '%s'.",
                                                  expectedType, meta.getType()));
    }
    return meta;
  }

  /**
   * Returns the content transformed into a Class defined.
   * It first transforms from the charset into unicode and then applies the transformation.
   *
   * @param charset source charset of the content.
   * @param type class to converted to.
   * @return instance of type T as defined by the class.
   */
  @Nullable
  public <T> T getContent(String charset, Class<T> type) {
    String data = getContent(charset);
    if (data != null) {
      return GSON.fromJson(data, type);
    }
    return null;
  }
}
