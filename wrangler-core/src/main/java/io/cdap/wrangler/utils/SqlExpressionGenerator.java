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

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.relational.Expression;
import io.cdap.cdap.etl.api.relational.ExpressionFactory;
import io.cdap.cdap.etl.api.relational.RelationalTranformContext;
import io.cdap.cdap.etl.api.relational.StringExpressionFactoryType;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nullable;

/**
 * Utility class that contains methods for sql expression generation.
 */
public class SqlExpressionGenerator {

    public static Optional<ExpressionFactory<String>> getExpressionFactory(RelationalTranformContext ctx) {
        return ctx.getEngine().getExpressionFactory(StringExpressionFactoryType.SQL);
    }

    public static Map<String, Expression> generateColumnExpMap(Collection columns, ExpressionFactory<String> factory) {
        Map<String, Expression> columnExpMap = new LinkedHashMap<>();
        columns.forEach((colName)-> columnExpMap.put((String) colName, factory.compile(colName.toString())));
        return columnExpMap;
    }

    public static String getColumnTypeExp(String toType, String column, @Nullable Integer scale) {
        toType = toType.toUpperCase();
        String expression;
        switch (toType) {
            case "INTEGER":
            case "I64":
            case "INT": {
                expression = "CAST(" + column + " AS INT)";
                return expression;
            }

            case "I32":
            case "SHORT": {
                expression = "CAST(" + column + " AS SMALLINT)";
                return expression;
            }

            case "LONG": {
                expression = "CAST(" + column + " AS BIGINT)";
                return expression;
            }

            case "BOOL":
            case "BOOLEAN": {
                expression = "CAST(" + column + " AS BOOLEAN)";
                return expression;
            }

            case "STRING": {
                expression = "CAST(" + column + " AS STRING)";
                return expression;
            }

            case "FLOAT": {
                expression = "CAST(" + column + " AS FLOAT)";
                return expression;
            }

            case "DOUBLE": {
                expression = "CAST(" + column + " AS DOUBLE)";
                return expression;
            }

            case "DECIMAL": {
                if (scale != null) {
                    expression = String.format("CAST(%s AS DECIMAL(38,%d))", column, scale);
                    return expression;
                } else {
                    expression = String.format("CAST(%s AS DECIMAL)", column);
                }
                return expression;
            }

            case "BYTES": {
                expression = "CAST(" + column + " AS TINYINT)";
                return expression;
            }

            default:
                return column;
        }
    }

    public static List<String> generateListCols(RelationalTranformContext relationalTranformContext) {
        List<String> colnames = new ArrayList<String>();
        Set<String> inputRelationNames = relationalTranformContext.getInputRelationNames();
        for (String inputRelationName : inputRelationNames) {
            Schema schema = relationalTranformContext.getInputSchema(inputRelationName);
            List<Schema.Field> fields = schema.getFields();
            for (Schema.Field field: fields) {
                colnames.add(field.getName());
            }
        }
        return colnames;
    }


}
