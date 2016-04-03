package org.etl.spi.flink;

import org.etl.api.Record;
import org.etl.api.transform.DuplicateTransformStep;
import org.etl.util.ExpressionUtil;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.util.Collector;

/**
 * .
 */
public class DuplicateDataSetProducer extends AbstractDataSetProducer<DuplicateTransformStep> {
    @Override
    public void validate(DuplicateTransformStep transform, DataSetProduceContext dataSetContext) {
        dataSetContext.getTransformStepDAG().addEdge(transform.getInput(),transform);
    }

    @Override
    public DataSet<Record> produce(final DuplicateTransformStep transform,
                                   final DataSetProduceContext dataSetProduceContext)
            throws Exception {
        DataSet<Record> dataSet = dataSetProduceContext.getDataSet(transform.getInput());

        return dataSet.flatMap(new DataSetDuplicateFunction(transform.getCondition()));
    }

    private static class DataSetDuplicateFunction implements FlatMapFunction<Record, Record> {

        private final  String duplicateCondition;

        public DataSetDuplicateFunction(String duplicateCondition) {
            this.duplicateCondition = duplicateCondition;
        }

        @Override
        public void flatMap(Record value, Collector<Record> out) throws Exception {
            boolean condition = (boolean) ExpressionUtil.evaluate(duplicateCondition, value.asMap());
            if (condition) {
                Record duplicate = new Record();
                duplicate.copy(value);
                out.collect(duplicate);
            }
            out.collect(value);
        }
    }

}
