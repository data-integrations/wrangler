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

import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.wrangler.api.Record;
import co.cask.wrangler.api.statistics.ColumnMetric;
import co.cask.wrangler.api.statistics.Measurements;
import co.cask.wrangler.api.statistics.Statistics;
import io.dataapps.chlorine.finder.FinderEngine;

import java.util.List;
import java.util.Map;

/**
 * Created by kewang on 5/19/17.
 */
public class TypeStatistics implements Statistics {

  public TypeStatistics() throws Exception {
    //TODO: add sth.
  }

  /**
   *
   * @param records to be aggregated.
   * @return a Record where Columns are column names. Values are KeyValues <inferred type, percentage>
   */
  @Override
  public Record aggregate(List<Record> records) {
    Record typeRecord = new Record();
    ColumnMetric typeMetric = new ColumnMetric();
    Double count = new Double(0);
    for (Record record : records) {
      ++count;
      for (int i = 0; i < record.length(); ++i) {
        String column = record.getColumn(i);
        Object object = record.getValue(i);
        if (object != null && object instanceof String) {
          String value = ((String) object);
          if (! value.isEmpty()) {
            String typeName = null;



            //TODO: get type here
            if (value.length() > 8) {
              typeName = "phone number";
            }
            else {
              typeName = "address";
            }




            typeMetric.increment(column, typeName);
          }
        }
        else { //unknown type
          typeMetric.increment(column, "unknown");
        }
      }
    }

    for (String columnName: typeMetric.getColumns()) {
      List<KeyValue<String, Double>> percentages = typeMetric.percentage(columnName, count);
      String maxTypeName = null;
      Double maxTypeValue = 0.0;
      for (KeyValue<String, Double> percentage : percentages) {
        String typeName = percentage.getKey();
        Double typeValue = percentage.getValue();
        if (typeValue > maxTypeValue) {
          maxTypeValue = typeValue;
          maxTypeName = typeName;
        }
      }
      typeRecord.add(columnName, new KeyValue(maxTypeName, maxTypeValue));
    }
    return typeRecord;
  }

  //TODO: For test only Ken
  public static String typeRecordToStr(Record typeRecord) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < typeRecord.length(); i ++) {
      String column = typeRecord.getColumn(i);
      KeyValue <String, Double> typeKeyValue = (KeyValue<String, Double>) typeRecord.getValue(i);
      String typeStr = typeKeyValue.getKey() + ':' + typeKeyValue.getValue();
      sb.append(column);
      sb.append(" -> ");
      sb.append(typeStr);
      sb.append("\n");
    }
    return sb.toString();
  }
}
