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

import co.cask.wrangler.api.Record;
import co.cask.wrangler.api.statistics.ColumnMetric;
import co.cask.wrangler.api.statistics.Statistics;
import io.dataapps.chlorine.finder.FinderEngine;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by nitin on 2/4/17.
 */
public class BasicStatistics implements Statistics {
  private final FinderEngine engine;
  private final PhoneNumberFinder phoneNumberFinder;
  private final ISBNFinder isbnFinder;
  private final AddressFinder addressFinder;


  public BasicStatistics() throws Exception {
    engine = new FinderEngine("wrangler-finder-1.xml", true, false);
    phoneNumberFinder = new PhoneNumberFinder();
    isbnFinder = new ISBNFinder();
    addressFinder = new AddressFinder();
  }

  @Override
  public Record aggregate(List<Record> records) {
    ColumnMetric types = new ColumnMetric();
    ColumnMetric stats = new ColumnMetric();

    Double count = new Double(0);
    for (Record record : records) {
      ++count;
      for (int i = 0; i < record.length(); ++i) {
        String column = record.getColumn(i);
        Object object = record.getValue(i);

        if (object == null) {
          stats.increment(column, "null");
        } else {
          stats.increment(column, "non-null");
        }

        if (object instanceof String) {
          String value = ((String) object);
          if (value.isEmpty()) {
            stats.increment(column, "empty");
          }

          //only after other finders all failed, we use default regex finders
          else {
            Map<String, List<String>> finds = engine.findWithType(value);

            //types:
            Set<String> keySet = finds.keySet();

            //all types
            //Integer, Mastercard, Visa, AMEX, Discover, JCB, URL, France_Postal_Code, Canadian_Postal_Code, Email
            //Date_Time, Month, Time, Month/Year, Date, Mac_Address, IPV4, US_Postal_Codes, US_State, SSN, IPV6, Text, Gender, Boolean
            //IBAN, ISBN, Zip_Code, Currency, Longitude, Latitude, Street_Address


            //TODO: Street_Address need improve
            //Date_Time, Month, Time, Month/Year, Date
            if (addressFinder.isUSAddress(value)) {
              types.increment(column, "Street_Address");
            }

            //Boolean
            else if (keySet.contains("Boolean")) {
              types.increment(column, "Boolean");
            }
            //URL
            else if (keySet.contains("URL")) {
              types.increment(column, "URL");
            }
            //US_State
            else if (keySet.contains("US_State")) {
              types.increment(column, "US_State");
            }
            //Gender
            else if (keySet.contains("Gender")) {
              types.increment(column, "Gender");
            }
            //IPV4, IPV6
            else if (keySet.contains("IPV6")) {
              types.increment(column, "IPV6");
            }
            else if (keySet.contains("IPV4")) {
              types.increment(column, "IPV4");
            }
            //MAC address
            else if (keySet.contains("Mac_Address")) {
              types.increment(column, "Mac_Address");
            }

            //ISBN
            else if (isbnFinder.isISBN(value)) {
              types.increment(column, "ISBN");
            }

            //Currency
            else if (keySet.contains("Currency")) {
              types.increment(column, "Currency");
            }
            //Email
            else if (keySet.contains("Email")) {
              types.increment(column, "Email");
            }
            //SSN
            else if (keySet.contains("SSN")) {
              types.increment(column, "SSN");
            }

            //Mastercard, Visa, AMEX, Discover, JCB
            //TODO: Maybe shouldn't exclusive
            else if (keySet.contains("Mastercard")) {
              types.increment(column, "Mastercard");
            }
            else if (keySet.contains("Visa")) {
              types.increment(column, "Visa");
            }
            else if (keySet.contains("AMEX")) {
              types.increment(column, "AMEX");
            }
            else if (keySet.contains("Discover")) {
              types.increment(column, "Discover");
            }
            else if (keySet.contains("JCB")) {
              types.increment(column, "JCB");
            }

            //Longitude, Latitude
            //TODO: might need to distinguish
            else if (keySet.contains("Longitude") || keySet.contains("Latitude")) {
              types.increment(column, "Longitude_Latitude");
            }

            //TODO: They ARE zip code though
            //France_Postal_Code, Canadian_Postal_Code, US_Postal_Codes
            else if (keySet.contains("US_Postal_Codes")) {
              types.increment(column, "US_Postal_Codes");
            }
            else if (keySet.contains("France_Postal_Code")) {
              types.increment(column, "France_Postal_Code");
            }
            else if (keySet.contains("Canadian_Postal_Code")) {
              types.increment(column, "Canadian_Postal_Code");
            }

            //Zip_Code
            else if (keySet.contains("Zip_Code")) {
              types.increment(column, "Zip_Code");
            }

            //Text
            else if (keySet.contains("Text")) {
              types.increment(column, "Text");
            }

            //Phone number comes second last, otherwise it can easily mess up with other types cause so many phone number format to match
            else if (phoneNumberFinder.isValidPhone(value)) {
              types.increment(column, "Phone");
            }

            //Integer comes last
            else if (keySet.contains("Integer")) {
              types.increment(column, "Integer");
            }
            else {
              types.increment(column, "Unknown");
            }
          }
        }
      }
    }

    Record recordTypes = new Record();
    for (String column : types.getColumns()) {
      recordTypes.add(column, types.percentage(column, count));
    }

    Record recordStats = new Record();
    for (String column : stats.getColumns()) {
      recordStats.add(column, stats.percentage(column, count));
    }

    Record record = new Record();
    record.add("types", recordTypes);
    record.add("stats", recordStats);
    record.add("total", count);

    return record;
  }

}
