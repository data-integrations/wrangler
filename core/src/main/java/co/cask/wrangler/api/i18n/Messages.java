/*
 * Copyright © 2017 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations under
 *  the License.
 *
 */

package co.cask.wrangler.api.i18n;

import java.text.MessageFormat;
import java.util.Locale;
import java.util.MissingResourceException;
import java.util.ResourceBundle;

/**
 * <code>
 *   private static final Messages MSG = MessageFactory.getMessages();
 *   private static final Messages MSG = MessageFactory.getMessages(name, Locale.FRANCE);
 * </code>
 */
public final class Messages {
  private static ResourceBundle bundle;
  private final String name;
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
    bundle = ResourceBundle.getBundle(name, locale, this.getClass().getClassLoader());
  }

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

  public String get(String key, Object ... args) {
    return MessageFormat.format(get(key), args);
  }
}
