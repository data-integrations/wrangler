/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap;

import io.cdap.cdap.etl.api.relational.Expression;
import io.cdap.cdap.etl.api.relational.Relation;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

public class MockRelation implements Relation {
    String column;

    Expression expression;

    public MockRelation(@Nullable String column, @Nullable Expression expression) {
        this.column = column;
        this.expression = expression;
    }
    @Override
    public boolean isValid() {
        return true;
    }

    @Override
    public String getValidationError() {
        return null;
    }

    @Override
    public Relation setColumn(String column, Expression value) {
        return new MockRelation(column, value);
    }

    @Override
    public Relation dropColumn(String column) {
        return new MockRelation(column, null);
    }

    @Override
    public Relation select(Map<String, Expression> columnExpMap) {
        String columns = columnExpMap.entrySet()
                .stream()
                .map(column -> column.getKey())
                .collect(Collectors.joining(","));

        String expressions = columnExpMap.entrySet()
                .stream()
                .map(column -> ((MockExpression) column.getValue()).getExpression())
                .collect(Collectors.joining(","));

    return new MockRelation(columns, new MockExpression(expressions));
    }

    @Override
    public Relation filter(Expression filter) {
        return new MockRelation(null, filter);
    }

    public String getColumn() {
        return this.column;
    }

    public Expression getExpression() {
        return this.expression;
    }
}
