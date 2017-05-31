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

package co.cask.wrangler.statistics;

import org.apache.commons.lang3.builder.ToStringExclude;

/**
 * Created by kewang on 5/30/17.
 */
public class AddressParser {

  public String[] parseToArr(String str) {
    //all spaces
    String [] strArray = str.split(" ");
    return strArray;
  }

  public Address parseToAddress (String [] strArray) {
    for (int i = strArray.length - 1; i >= 0; i --) {

    }
  }

}
