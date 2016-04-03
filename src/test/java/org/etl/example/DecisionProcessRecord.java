package org.etl.example;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.Record;
import org.apache.flink.types.StringValue;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by jason zhang on 12/14/2015.
 */
public class DecisionProcessRecord {

    private static MapFunction<String,Record> splitFunc = new MapFunction<String, Record>() {
        public Record map(String value) throws Exception {
            String[] fields = value.split(",");
            Record record = new Record(3);
            record.setField(0, new StringValue(fields[0]));
            record.setField(1, new StringValue(fields[1]));
            record.setField(2, new IntValue(Integer.valueOf(fields[2])));
            return record;
        }
    };

    private static List<Tuple4<String, String, String, String>> getDecisions() {
        List<Tuple4<String, String, String, String>> result = new ArrayList<Tuple4<String, String, String, String>>();
        result.add(new Tuple4<String, String, String, String>("COLOUR == \"Yellow\" && MAKE == \"HTC\"",",YellowHTC","7","QUANTITY"));
        result.add(new Tuple4<String, String, String, String>("COLOUR == \"Yellow\" && MAKE == \"Apple\"",",YellowApple","7","QUANTITY"));
        result.add(new Tuple4<String, String, String, String>("COLOUR == \"Red\" && MAKE == \"Apple\"",",RedApple","7","QUANTITY"));
        result.add(new Tuple4<String, String, String, String>("COLOUR == \"Yellow\" && MAKE == \"Samsung\"",",YellowSamsung","7","QUANTITY"));
        return result;
    }

    public static void main(String[] args) throws Exception {
        final List<Tuple4<String, String, String, String>> decisions = getDecisions();


        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Record> phonesDataSet =   streamEnv.readTextFile("d:\\phones.txt").map(splitFunc);
        DataStream<String> phonesSocket = streamEnv.socketTextStream("172.20.30.211", 9999);
//        phonesSocket.map(splitFunc).union(phonesDataSet).print();

        phonesSocket.map(splitFunc).union(phonesDataSet).flatMap(new FlatMapFunction<Record, Record>() {
            public void flatMap(Record value, Collector<Record> out) throws Exception {
                Map<String, Object> sourceRow = new HashMap<String, Object>();
                sourceRow.put("MAKE", value.getField(0, StringValue.class).getValue());
                sourceRow.put("COLOUR", value.getField(1, StringValue.class).getValue());
                sourceRow.put("QUANTITY", value.getField(2, IntValue.class).getValue());

                for (Tuple4<String, String, String, String> dtRow : decisions) {
                    boolean condition = (Boolean) ExpressionEngine.evaluateExpression(dtRow.f0, sourceRow);
                    if (condition) {
                        Record record = new Record(3);
                        record.setField(0, new StringValue(dtRow.f1));
                        record.setField(1, new StringValue(dtRow.f2));
                        record.setField(2, new LongValue(Long.valueOf(sourceRow.get(dtRow.f3).toString())));
                        out.collect(record);
                    }
                }
            }
        }).keyBy(new KeySelector<Record, String>() {
            public String getKey(Record value) throws Exception {
                return value.getField(0, StringValue.class).getValue() + value.getField(1, StringValue.class).getValue();
            }
        }).reduce(new ReduceFunction<Record>() {
            public Record reduce(Record value1, Record value2) throws Exception {
                Record record = new Record(3);

                record.setField(0, value1.getField(0, StringValue.class));
                record.setField(1, value1.getField(1, StringValue.class));
                record.setField(2, new LongValue(value1.getField(2, LongValue.class).getValue() + value2.getField(2, LongValue.class).getValue()));

                return record;
            }
        }).writeAsText("d:\\record.csv");

        streamEnv.execute();
    }



}
