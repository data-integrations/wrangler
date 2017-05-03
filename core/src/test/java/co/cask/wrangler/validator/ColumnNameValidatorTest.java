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

package co.cask.wrangler.validator;

import co.cask.wrangler.api.validator.Validator;
import co.cask.wrangler.api.validator.ValidatorException;
import co.cask.wrangler.validator.ColumnNameValidator;
import org.junit.Test;

/**
 * Tests {@link ColumnNameValidator}
 */
public class ColumnNameValidatorTest {

  @Test
  public void testGoodColumnNames() throws Exception {
    Validator validator = new ColumnNameValidator();
    validator.initialize();
    validator.validate("first_name");
    validator.validate("id");
    validator.validate("last_name");
    validator.validate("emailid");
    validator.validate("address");
    validator.validate("adhara_number");
  }

  @Test(expected = ValidatorException.class)
  public void testReservedWord() throws Exception {
    Validator validator = new ColumnNameValidator();
    validator.initialize();
    validator.validate("timestamp");
  }

  @Test(expected = ValidatorException.class)
  public void testNonAlphaNumeric() throws Exception {
    Validator validator = new ColumnNameValidator();
    validator.initialize();
    validator.validate("event.timestamp");
  }

  @Test(expected = ValidatorException.class)
  public void testLongColumnName() throws Exception {
    Validator validator = new ColumnNameValidator();
    validator.initialize();
    validator.validate("eventsdakjdadkjadkajdadjkadajkdaldjadljadalkjdakldjaldkjasdlajdsakdjalkdjadkljadakjda" +
                         "asdakdaldkajdlkasjdsalkdjadlkjadlkjadlakjdaldkjaldkjadlkjadakjdadjadlkajdlakjdsakd" +
                         "asdakldjalkdjadlkjadlakjdlakjdaslkdjadlkjsadlkajdalkdjadlkjadlkajdajdasdjkasda" +
                         "adlkajdalkdjadlkjadlkjadsajkldjadlkajdlakjdaslkdjalkdjadlkjadlakjdaslkdjsadkljsadjas" +
                         "asdaksdjaslkdjaslkdjadlkjadlkasjdalkdjaldkjadlajkdlakjdsalkdjadlkjasdlskajdsalkjdsad" +
                         "aldjadlkajdaslkdjsaldkjasdlkajdlkasjdaljdasldkjadlkasjdalkjdaslkjdaldkjasdlkjadja" +
                         "adajkdlaksjdalkdjsaldkasdlkasjdaslkjdsalkdjsadlkjasdlaskjdsalkjdasldkjadlkjasdadaljkda");
  }
}
