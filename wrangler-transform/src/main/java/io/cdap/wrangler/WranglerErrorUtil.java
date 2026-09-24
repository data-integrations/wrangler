/*
 * Copyright © 2025 Cask Data, Inc.
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

import com.google.common.base.Throwables;
import io.cdap.wrangler.expression.ELPermissionException;

/**
 * Error util file to handle exceptions caught in Wrangler plugin.
 */
public final class WranglerErrorUtil {

  /**
   * Private constructor to prevent instantiation of this utility class.
   * <p>
   * This class is designed to contain only static utility methods for handling exceptions and
   * should not be instantiated. Any attempt to create an instance of this class will result in an
   * {@link IllegalStateException}.
   */
  private WranglerErrorUtil() {
    throw new IllegalStateException("Utility class");
  }

  /**
   * Checks whether the given {@link Throwable} or any cause in its causal chain is an
   * {@link ELPermissionException}.
   *
   * @param e the {@link Throwable} to inspect
   * @return {@code true} if {@link ELPermissionException} is present in the causal chain,
   *     {@code false} otherwise
   */
  public static boolean isCriticalException(Throwable e) {
    return e != null && Throwables.getCausalChain(e).stream()
        .anyMatch(ELPermissionException.class::isInstance);
  }
}
