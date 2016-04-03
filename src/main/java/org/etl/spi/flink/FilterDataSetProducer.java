package org.etl.spi.flink;

import org.etl.api.Record;
import org.etl.api.transform.FilterTransformStep;
import org.etl.util.ExpressionUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;

/**
 * .
 */
public class FilterDataSetProducer extends AbstractDataSetProducer<FilterTransformStep> {

    @Override
    public void validate(FilterTransformStep transform, DataSetProduceContext dataSetContext) {
        dataSetContext.getTransformStepDAG().addEdge(transform.getInput(),transform);
    }

    @Override
    public DataSet<Record> produce(final FilterTransformStep transform,final DataSetProduceContext dataSetProduceContext) throws Exception {
        final DataSet<Record> inputDataSet = dataSetProduceContext.getDataSet(transform.getInput());
        return inputDataSet.filter(new DataSetFilterFunction(transform.getFilter()));
    }

    private static class  DataSetFilterFunction implements  FilterFunction<Record>{


        private final String filter;

        public DataSetFilterFunction( String filter) {
            this.filter = filter;
        }

        @Override
        public boolean filter(Record value) throws Exception {
            return  (boolean) ExpressionUtil.evaluate(filter, value.asMap());
        }
    }
}
