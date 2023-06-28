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

import io.cdap.cdap.etl.api.relational.Expression;
import io.cdap.cdap.etl.api.relational.ExpressionFactory;
import io.cdap.cdap.etl.api.relational.RelationalTranformContext;
import io.cdap.cdap.etl.api.relational.StringExpressionFactoryType;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

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
}
