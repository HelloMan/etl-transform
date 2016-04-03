package org.etl.api;

import org.etl.api.transform.DataField;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;


public class Record implements Serializable {

    private Set<DataField> schema;

    private final Map<String, Object> map = new HashMap<>();

    public Set<DataField> getSchema() {
        return schema;
    }

    public void setSchema(Set<DataField> schema) {
        this.schema = schema;
    }

    public  <T extends Comparable>  T  getValue(String fieldName) {
        return (T) map.get(fieldName);
    }

    public void setValue(String name, Comparable value){
        map.put(name, value);
    }



    public void copy(Record record) {
        map.putAll(record.map);
    }

    public Map<String,Object> asMap() {
        return map;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Record record = (Record) o;
        return Objects.equals(map, record.map);
    }

    @Override
    public int hashCode() {
        return Objects.hash(map);
    }
}
