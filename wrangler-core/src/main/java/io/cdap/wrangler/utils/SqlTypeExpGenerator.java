/*
 *  Copyright © 2017-2019 Cask Data, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *  use this file except in compliance with the License. You may obtain a copy of
 *  the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations under
 *  the License.
 */

package io.cdap.wrangler.utils;

/**
 * Utility class that returns a string of SQL expression for the given data type.
 */
public final class SqlTypeExpGenerator {

    public static String getColumnTypeExp(String toType, String column, int scale) {
        toType = toType.toUpperCase();
        String expression;
        if (!toType.equals("DECIMAL")) {
            return column;
        }
        expression = ("CAST(" + column + " AS DECIMAL(38," + scale + "))");
        return expression;
    }
    public static String getColumnTypeExp(String toType, String column) {
        toType = toType.toUpperCase();
        String expression = "";
        switch (toType) {
            case "INTEGER":
            case "I64":
            case "INT": {
                expression += "CAST(" + column + " AS INT)";
                return expression;
            }

            case "I32":
            case "SHORT": {
                expression += "CAST(" + column + " AS SMALLINT)";
                return expression;
            }

            case "LONG": {
                expression += "CAST(" + column + " AS BIGINT)";
                return expression;
            }

            case "BOOL":
            case "BOOLEAN": {
                expression += "CAST(" + column + " AS BOOLEAN)";
                return expression;
            }

            case "STRING": {
                expression += "CAST(" + column + " AS STRING)";
                return expression;
            }

            case "FLOAT": {
                expression += "CAST(" + column + " AS FLOAT)";
                return expression;
            }

            case "DOUBLE": {
                expression += "CAST(" + column + " AS DOUBLE)";
                return expression;
            }

            case "BYTES": {
                expression += "CAST(" + column + " AS TINYINT)";
                return expression;
            }

            default:
                return column;
        }
    }
}