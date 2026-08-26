package io.cdap.wrangler.parser;


import io.cdap.wrangler.api.ExecutorContext;
import org.apache.spark.util.kvstore.InMemoryStore;
import org.reflections.Store;

public class MockExecutorContext implements ExecutorContext 
{
        private final Store store = new InMemoryStore();

        @Override public Store getStore() 
        { 
            return store; 
        }
}

        @Override public void set(String key, Object value) 
        { 
            store.put(key, value); 
        }

        @Override public Object get(String key) 
        { 
            return store.get(key); 
        }

        @Override public void remove(String key) 
        { 
            store.remove(key); 
        }
}