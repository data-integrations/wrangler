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

import co.cask.cdap.api.service.http.HttpServiceResponder;
import co.cask.wrangler.proto.ServiceResponse;
import com.google.common.base.Charsets;
import org.bouncycastle.crypto.digests.MD5Digest;
import org.bouncycastle.util.encoders.Hex;

import java.net.HttpURLConnection;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;

/**
 * This class provides utility services to the service in this package.
 */
public final class ServiceUtils {

  /**
   * Generates a MD5 hash for a given string.
   *
   * This implementation is based on Bouncycastle. So, you would need to initialize the security
   * provider to use {@link org.bouncycastle.jce.provider.BouncyCastleProvider}.
   *
   * <code>
   *   Security.addProvider(new BouncyCastleProvider());
   * </code>
   * @param value to be converted to MD5.
   * @return String representation of MD5.
   */
  public static String generateMD5(String value) {
    byte[] input = value.getBytes(Charsets.UTF_8);
    MD5Digest md5Digest = new MD5Digest();
    md5Digest.update(input, 0, input.length);
    byte[] output = new byte[md5Digest.getDigestSize()];
    md5Digest.doFinal(output, 0);
    return new String(Hex.encode(output));
  }

  /**
   * Sends the error response back to client.
   *
   * @param responder to respond to the service request.
   * @param message to be included as part of the error
   */
  public static void error(HttpServiceResponder responder, String message) {
    ServiceResponse<Void> response = new ServiceResponse<>(message);
    responder.sendJson(HttpURLConnection.HTTP_INTERNAL_ERROR, response);
  }

  /**
   * Sends the error response back to client for not Found.
   *
   * @param responder to respond to the service request.
   * @param message to be included as part of the error
   */
  public static void notFound(HttpServiceResponder responder, String message) {
    ServiceResponse<Void> response = new ServiceResponse<>(message);
    responder.sendJson(HttpURLConnection.HTTP_NOT_FOUND, response);
  }

  /**
   * Sends the error response back to client with error status.
   *
   * @param responder to respond to the service request.
   * @param message to be included as part of the error
   */
  public static void error(HttpServiceResponder responder, int status, String message) {
    ServiceResponse<Void> response = new ServiceResponse<>(message);
    responder.sendJson(status, response);
  }

  /**
   * Returns a Json response back to client.
   *
   * @param responder to respond to the service request.
   * @param status code to be returned to client.
   * @param body to be sent back to client.
   */
  public static void sendJson(HttpServiceResponder responder, int status, String body) {
    responder.send(status, ByteBuffer.wrap(body.getBytes(StandardCharsets.UTF_8)),
                   "application/json", new HashMap<>());
  }

  /**
   * Sends the success response back to client.
   *
   * @param responder to respond to the service request.
   * @param message to be included as part of the error
   */
  public static void success(HttpServiceResponder responder, String message) {
    ServiceResponse<Void> response = new ServiceResponse<>(message);
    responder.sendJson(response);
  }
}
