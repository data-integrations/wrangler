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

import java.text.MessageFormat;
import java.util.Locale;
import java.util.MissingResourceException;
import java.util.ResourceBundle;

/**
 * Messages is a i18n error or descriptive messages.
 *
 * <code>
 *   private static final Messages MSG = MessageFactory.getMessages();
 *   private static final Messages MSG = MessageFactory.getMessages(name, Locale.FRANCE);
 * </code>
 */
@Public
public final class Messages {
  // Resource bundle.
  private static ResourceBundle bundle;

  // Name of the resource.
  private final String name;

  // Locale of the resource.
  private final Locale locale;

  public Messages() {
    this("user/messages", Locale.getDefault());
  }

  public Messages(String name) {
    this(name, Locale.getDefault());
  }

  public Messages(String name, Locale locale) {
    this.name = name;
    this.locale = locale;
    bundle = ResourceBundle.getBundle(name, locale, Messages.class.getClassLoader());
  }

  /**
   * Return the message based on the key.
   *
   * @param key of the message.
   * @return resolved string message.
   */
  public String get(String key) {
    if (bundle == null) {
      return String.format("Key '%s' not found in bundle '%s' and locale '%s'",
                           key, name, locale.toString());
    }

    try {
      return bundle.getString(key);
    } catch (MissingResourceException e) {
      return String.format("Key '%s' not found in bundle '%s' and locale '%s'",
                           key, name, locale.toString());
    }
  }

  /**
   * Returns a string message with resolved arguments.
   *
   * @param key name of the key.
   * @param args list of arguments.
   * @return resolved string representation.
   */
  public String get(String key, Object ... args) {
    return MessageFormat.format(get(key), args);
  }
}
