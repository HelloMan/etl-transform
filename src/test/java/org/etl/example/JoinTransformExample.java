package org.etl.example;

import org.etl.TransformStepFactory;
import org.etl.api.Record;
import org.etl.spi.flink.Value;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;

import java.util.Map;

/**
 *
 */
public class JoinTransformExample {

    public static void main(String[] args) throws Exception {


        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();


        DataSet<Record> leftDataSet = executionEnvironment.fromCollection(TransformStepFactory.createPhoneRecords());

        DataSet<Record> rightDataSet = executionEnvironment.fromCollection(TransformStepFactory.createProduceAreaRecords());


        leftDataSet.join(rightDataSet).where(new KeySelector<Record, Value>() {
            @Override
            public Value getKey(Record value) throws Exception {
                return new Value(value.getValue(TransformStepFactory.PRODUCE_AREA_ID_FIELD));
            }
        }).equalTo(new KeySelector<Record, Value>() {
            @Override
            public Value getKey(Record value) throws Exception {
                return new Value(value.getValue(TransformStepFactory.PRODUCE_AREA_ID_FIELD));
            }
        }).with(new JoinFunction<Record, Record, Map>() {
            @Override
            public Map join(Record first, Record second) throws Exception {
                Record record = new Record();
                record.copy(first);
                record.copy(second);
                return record.asMap();
            }
        }).print();



    }
}

