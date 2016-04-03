package org.etl.example;

import org.etl.api.Record;
import org.etl.spi.flink.Value;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.util.Map;

/**
 *
 */
public class GroupAndSortByTransform {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();

        final Record record1 = new Record();

        record1.setValue("maker","HTC");
        record1.setValue("colour","Yellow");
        record1.setValue("price",2500);
        record1.setValue("quantity", BigDecimal.valueOf(200));
        final Record record7 = new Record();

        record7.setValue("maker","HTC");
        record7.setValue("colour","Yellow");
        record7.setValue("price",2400);
        record7.setValue("quantity", BigDecimal.valueOf(200));
        Record record2 = new Record();
        record2.setValue("maker", "HTC");
        record2.setValue("colour","Yellow");
        record2.setValue("price",2000);
        record2.setValue("quantity", BigDecimal.valueOf(300));
        Record record3 = new Record();
        record3.setValue("maker","HTC");
        record3.setValue("colour","Yellow");
        record3.setValue("price",2800);
        record3.setValue("quantity", BigDecimal.valueOf(150));
        Record record4 = new Record();
        record4.setValue("maker", "Apple");
        record4.setValue("colour","Red");
        record4.setValue("price",2500);
        record4.setValue("quantity", BigDecimal.valueOf(850));

        Record record5 = new Record();
        record5.setValue("maker", "Apple");
        record5.setValue("colour", "Yellow");
        record5.setValue("price",2200);
        record5.setValue("quantity", BigDecimal.valueOf(120));

        Record record6= new Record();
        record6.setValue("maker", "Apple");
        record6.setValue("colour", "Red");
        record6.setValue("price",5500);
        record6.setValue("quantity", BigDecimal.valueOf(360));




        DataSet<Record> recordDataset = executionEnvironment.fromElements(record1, record3, record4, record7,record2, record6,record5);
        recordDataset .groupBy(new KeySelector<Record, Value>() {
            @Override
            public Value getKey(Record value) throws Exception {
                Value value1 = new Value();
                value1.addValue(value.getValue("maker"));
                value1.addValue(value.getValue("colour"));
                return value1;
            }
        }).sortGroup(new KeySelector<Record, Value>() {
            @Override
            public Value getKey(Record value) throws Exception {

                Value value1 = new Value(Order.DESCENDING,Order.ASCENDING);

                value1.addValue(value.getValue("price"));
                value1.addValue(value.getValue("quantity"));
                return value1;
            }
        },Order.ANY)
                
                
                
                .reduceGroup(new GroupReduceFunction<Record, Map>() {
                    @Override
                    public void reduce(Iterable< Record> values, Collector<Map> out) throws Exception {
                        for (  Record value : values) {
                            out.collect(value.asMap());
                        }

                    }
                }).print();


    }
}

