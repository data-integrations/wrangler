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

package io.cdap.wrangler.i18n;

import io.cdap.wrangler.api.annotations.Public;

import java.util.Locale;

/**
 * i18n Messages Factory for creating different {@link Messages}.
 */
@Public
public final class MessagesFactory {

  /**
   * @return Default {@link Messages} with resource bundle 'user' and default {@link Locale}
   */
  public static Messages getMessages() {
    return new Messages();
  }

  /**
   * Creates a {@link Messages} with specified {@link java.util.ResourceBundle} name and default {@link Locale}
   * @param name of the base bundle for creating {@link Messages}
   * @return an instance of {@link Messages}
   */
  public static Messages getMessages(String name) {
    return new Messages(name);
  }

  /**
   * Creates a {@link Messages} with specified {@link java.util.ResourceBundle} name and {@link Locale}
   * @param name of the base bundle for creating {@link Messages}
   * @param locale to be used for creating the {@link Messages}
   * @return an instance of {@link Messages}
   */
  public static Messages getMessages(String name, Locale locale) {
    return new Messages(name, locale);
  }
}
