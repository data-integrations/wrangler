package io.cdap.wrangler.utils;

import io.cdap.cdap.etl.api.relational.Expression;
import io.cdap.cdap.etl.api.relational.ExpressionFactory;
import io.cdap.cdap.etl.api.relational.RelationalTranformContext;
import io.cdap.cdap.etl.api.relational.StringExpressionFactoryType;


import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class SqlExpressionGenerator {

    public static Optional<ExpressionFactory<String>> getExpressionFactory(RelationalTranformContext ctx) {
        return ctx.getEngine().getExpressionFactory(StringExpressionFactoryType.SQL);
    }

    public static Map<String, Expression> generateColumnExpMap(List<String> columns, ExpressionFactory<String> factory) {
        Map<String, Expression> columnExpMap = new LinkedHashMap<>();
        columns.forEach((colName)-> columnExpMap.put(colName, factory.compile(colName)));
        return columnExpMap;
    }
}
