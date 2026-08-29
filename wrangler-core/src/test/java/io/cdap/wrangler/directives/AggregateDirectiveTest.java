package io.cdap.wrangler.directives;

import io.cdap.cdap.etl.api.StageMetrics;
import io.cdap.wrangler.api.ExecutorContext;
import io.cdap.wrangler.api.ExecutorContext.Environment;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.TokenGroup;
import io.cdap.wrangler.api.TransientStore;

import org.junit.Assert;
import org.junit.Test;
import org.yaml.snakeyaml.tokens.Token;

import java.lang.invoke.MethodHandles.Lookup;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class AggregateDirectiveTest {

    @Test
    public void testAggregateDirectiveTotal() throws Exception {
        AggregateDirective directive = new AggregateDirective();
        TokenGroup args = new TokenGroup();
        args.add("sourceSizeColumn", "size");
        args.add("sourceTimeColumn", "time");
        args.add("targetSizeColumn", "total_size");
        args.add("targetTimeColumn", "total_time");
        directive.initialize(args);

        List<Row> rows = new ArrayList<>();
        rows.add(new Row().add("size", 1024L).add("time", 1000L));
        rows.add(new Row().add("size", 2048L).add("time", 2000L));

        ExecutorContext context = new ExecutorContext() {
           
            // Removed duplicate method to resolve name clash

            @Override
            public Environment getEnvironment() {
                throw new UnsupportedOperationException("Unimplemented method 'getEnvironment'");
            }

            @Override
            public String getNamespace() {
                throw new UnsupportedOperationException("Unimplemented method 'getNamespace'");
            }

            @Override
            public StageMetrics getMetrics() {
                throw new UnsupportedOperationException("Unimplemented method 'getMetrics'");
            }

            @Override
            public String getContextName() {
                throw new UnsupportedOperationException("Unimplemented method 'getContextName'");
            }

            @Override
            public URL getService(String applicationId, String serviceId) {
                throw new UnsupportedOperationException("Unimplemented method 'getService'");
            }

            @Override
            public TransientStore getTransientStore() {
                throw new UnsupportedOperationException("Unimplemented method 'getTransientStore'");
            }

            @Override
            public <T> io.cdap.cdap.etl.api.Lookup<T> provide(String arg0, Map<String, String> arg1) {
                // TODO Auto-generated method stub
                throw new UnsupportedOperationException("Unimplemented method 'provide'");
            }

            @Override
            public Map<String, String> getProperties() {
                // TODO Auto-generated method stub
                throw new UnsupportedOperationException("Unimplemented method 'getProperties'");
            }
        };

        directive.execute(rows, context);

        List<Row> result = directive.finalize(rows, context);
        Row aggregateRow = result.get(0);

        Assert.assertEquals(3072L, aggregateRow.getValue("total_size"));
        Assert.assertEquals(3000L, aggregateRow.getValue("total_time"));
    }

    @Test
    public void testAggregateDirectiveAverage() throws Exception {
        AggregateDirective directive = new AggregateDirective();
        TokenGroup args = new TokenGroup(); // Ensure TokenGroup and Token are correctly implemented
        args.add("sourceSizeColumn", "size");
        args.add("sourceTimeColumn", "time");
        args.add("targetSizeColumn", "avg_size");
        args.add("targetTimeColumn", "avg_time");
        args.add("aggregationType", "average");

        List<Row> rows = new ArrayList<>();
        rows.add(new Row().add("size", 1024L).add("time", 1000L));
        rows.add(new Row().add("size", 2048L).add("time", 2000L));

        ExecutorContext context = new ExecutorContext() {
            @Override
            public <T> io.cdap.cdap.etl.api.Lookup<T> provide(String name, java.util.Map<String, String> arguments) {
                throw new UnsupportedOperationException("Unimplemented method 'provide'");
            }

            @Override
            public Environment getEnvironment() {
                throw new UnsupportedOperationException("Unimplemented method 'getEnvironment'");
            }

            @Override
            public String getNamespace() {
                throw new UnsupportedOperationException("Unimplemented method 'getNamespace'");
            }

            @Override
            public StageMetrics getMetrics() {
                throw new UnsupportedOperationException("Unimplemented method 'getMetrics'");
            }

            @Override
            public String getContextName() {
                throw new UnsupportedOperationException("Unimplemented method 'getContextName'");
            }

            @Override
            public URL getService(String applicationId, String serviceId) {
                throw new UnsupportedOperationException("Unimplemented method 'getService'");
            }

            @Override
            public TransientStore getTransientStore() {
                throw new UnsupportedOperationException("Unimplemented method 'getTransientStore'");
            }

   
            // Removed duplicate method to resolve name clash

            @Override
            public Map<String, String> getProperties() {
                // TODO Auto-generated method stub
                throw new UnsupportedOperationException("Unimplemented method 'getProperties'");
            }
        };

        directive.execute(rows, context);

        List<Row> result = directive.finalize(rows, context);
        Row aggregateRow = result.get(0);

        Assert.assertEquals(1536.0, aggregateRow.getValue("avg_size"));
        Assert.assertEquals(1500.0, aggregateRow.getValue("avg_time"));
    }
}