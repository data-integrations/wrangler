package io.cdap.wrangler.api;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Simple Row class for handling data records.
 */
public class Row {
    private final List<String> columns;
    private final Map<String, Object> values;
    
    /**
     * Creates a new Row.
     */
    public Row() {
        this.columns = new ArrayList<>();
        this.values = new HashMap<>();
    }
    
    /**
     * Adds a value to the row.
     * 
     * @param column The column name
     * @param value The value
     */
    public void add(String column, Object value) {
        if (!columns.contains(column)) {
            columns.add(column);
        }
        values.put(column, value);
    }
    
    /**
     * Gets the value for the given column.
     * 
     * @param column The column name
     * @return The value
     */
    public Object getValue(String column) {
        return values.get(column);
    }
    
    /**
     * Finds the index of the given column.
     * 
     * @param column The column name
     * @return The index, or -1 if not found
     */
    public int find(String column) {
        return columns.indexOf(column);
    }
}
