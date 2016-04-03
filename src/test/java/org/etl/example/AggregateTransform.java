package org.etl.example;

import org.etl.api.Record;
import org.etl.spi.flink.Value;
import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.util.Map;

/**
 *
 */
public class AggregateTransform {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();

        final Record record1 = new Record();

        record1.setValue("maker","HTC");
        record1.setValue("colour","Yellow");
        record1.setValue("quantity", BigDecimal.valueOf(200));
        Record record2 = new Record();
        record2.setValue("maker", "HTC");
        record2.setValue("colour","Yellow");
        record2.setValue("quantity", BigDecimal.valueOf(300));
        Record record3 = new Record();
        record3.setValue("maker","HTC");
        record3.setValue("colour","Red");
        record3.setValue("quantity", BigDecimal.valueOf(150));
        Record record4 = new Record();
        record4.setValue("maker", "Apple");
        record4.setValue("colour","Red");
        record4.setValue("quantity", BigDecimal.valueOf(850));

        Record record5 = new Record();
        record5.setValue("maker", "Apple");
        record5.setValue("colour", "Yellow");
        record5.setValue("quantity", BigDecimal.valueOf(120));

        Record record6= new Record();
        record6.setValue("maker", "Apple");
        record6.setValue("colour", "Red");
        record6.setValue("quantity", BigDecimal.valueOf(120));




        DataSet<Record> recordDataset = executionEnvironment.fromElements(record1, record2, record3, record4, record5, record6);
        recordDataset.map(new MapFunction<Record, Tuple2<Value, Record>>() {
            @Override
            public Tuple2<Value, Record> map(Record value) throws Exception {
                Value value1 = new Value();
                value1.addValue(value.getValue("maker"));
                value1.addValue(value.getValue("colour"));
                return new Tuple2<>(value1, value);
            }
        }).groupBy(0).combineGroup(new GroupCombineFunction<Tuple2<Value, Record>, Map>() {
            @Override
            public void combine(Iterable<Tuple2<Value, Record>> values, Collector<Map> out) throws Exception {
                Record record = null;
                for (Tuple2<Value, Record> value : values) {
                    Record oriRecord = value.f1;
                    if (record == null) {
                        record = new Record();
                        record.setValue("maker", oriRecord.getValue("maker"));
                        record.setValue("colour", oriRecord.getValue("colour"));
                        record.setValue("quantity", BigDecimal.ZERO);
                    }
                    BigDecimal quantity = oriRecord.getValue("quantity");
                    BigDecimal sum = record.getValue("quantity");
                    record.setValue("quantity", sum.add(quantity));
                }
                if (record != null) {
                    out.collect(record.asMap());
                }

            }
        }) .print();




    }
}

