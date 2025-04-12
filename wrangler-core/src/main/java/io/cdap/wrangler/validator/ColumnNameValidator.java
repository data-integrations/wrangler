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

package io.cdap.wrangler.validator;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

/**
 * This class performs validation on the column names of the record.
 *
 * <p>
 *   Following are the checks that this validator performs on the column.
 *   <ul>
 *     <li> Column name is a an identifier that is a sequence of alphaNumeric and under_score
 *     characters or a sequence.</li>
 *     <li> Column name is a non-reserved word. Check reserved-column-names.txt for the list of names.</li>
 *     <li> Column name is less than 255 characters.</li>
 *   </ul>
 * </p>
 */
public class ColumnNameValidator implements Validator<String> {
  private static final String RESERVED_WORDS_FILE = "reserved-column-names.txt";
  private final Set<String> reservedWords = new HashSet<>();

  /**
   * Initializes this validator.
   *
   * @throws Exception thrown when reserved words file is not accessible.
   */
  public void initialize() throws Exception {
    InputStream in = ColumnNameValidator.class.getClassLoader().getResourceAsStream(RESERVED_WORDS_FILE);
    if (in == null) {
      throw new Exception("Unable to load '" + RESERVED_WORDS_FILE + "' from the resources");
    }
    InputStreamReader isr = new InputStreamReader(in);
    try (BufferedReader reader = new BufferedReader(isr)) {
      String word;
      while ((word = reader.readLine()) != null) {
        reservedWords.add(word.toLowerCase(Locale.ENGLISH));
      }
    }
  }

  /**
   * Validates the T properties.
   *
   * @param name to be validated.
   * @throws ValidatorException thrown when there are issues with validation.
   */
  @Override
  public void validate(String name) throws ValidatorException {
    // Only alphanumeric and underscore (_) allowed.
    if (!name.matches("^[a-zA-Z0-9_]*$")) {
      throw new ValidatorException("Column '" + name + "' contains non-alphanumeric characters");
    }
    // Reserved words not allowed
    if (reservedWords.contains(name)) {
      throw new ValidatorException("Column '" + name + "' is a reserved word.");
    }
    // Column name length.
    if (name.length() > 255) {
      throw new ValidatorException("Column '" + name + "' is greater than 255 characters.");
    }
  }
}


