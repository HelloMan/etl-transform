package org.etl.example;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.table.Row;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by jason zhang on 12/14/2015.
 */
public class DecisionProcessRow {

    private static MapFunction<String,Row> splitFunc = new MapFunction<String, Row>() {
        public Row map(String value) throws Exception {
            String[] fields = value.split(",");
            Row record = new Row(3);
            record.setField(0,  fields[0]);
            record.setField(1,  fields[1]);
            record.setField(2, Integer.valueOf(fields[2]));
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
        DataStream<Row> phonesDataSet =   streamEnv.readTextFile("d:\\phones.txt").map(splitFunc);
        DataStream<String> phonesSocket = streamEnv.socketTextStream("172.20.30.211", 9999);
//        phonesSocket.map(splitFunc).union(phonesDataSet).print();

        phonesSocket.map(splitFunc).union(phonesDataSet).flatMap(new FlatMapFunction<Row, Row>() {
            public void flatMap(Row value, Collector<Row> out) throws Exception {
                Map<String, Object> sourceRow = new HashMap<String, Object>();
                sourceRow.put("MAKE", value.productElement(0));
                sourceRow.put("COLOUR", value.productElement(1));
                sourceRow.put("QUANTITY", value.productElement(2));

                for (Tuple4<String, String, String, String> dtRow : decisions) {
                    boolean condition = (Boolean) ExpressionEngine.evaluateExpression(dtRow.f0, sourceRow);
                    if (condition) {
                        Row record = new Row(3);
                        record.setField(0, dtRow.f1);
                        record.setField(1, dtRow.f2);
                        record.setField(2, Long.valueOf(sourceRow.get(dtRow.f3).toString()));
                        out.collect(record);
                    }
                }
            }
        }).keyBy(new KeySelector<Row, String>() {
            public String getKey(Row value) throws Exception {
                return value.productElement(0).toString() + value.productElement(1).toString();
            }
        }).reduce(new ReduceFunction<Row>() {
            public Row reduce(Row value1, Row value2) throws Exception {
                Row record = new Row(3);
                record.setField(0, value1.productElement(0));
                record.setField(1, value1.productElement(1));
                record.setField(2, Long.valueOf(value1.productElement(2).toString()) + Long.valueOf(value2.productElement(2).toString()));

                return record;
            }
        }).writeAsText("d:\\tt.csv");

        streamEnv.execute();
    }



}
