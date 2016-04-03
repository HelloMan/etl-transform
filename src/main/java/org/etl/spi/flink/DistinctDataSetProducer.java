package org.etl.spi.flink;

import org.etl.api.Record;
import org.etl.api.transform.DistinctTransformStep;
import org.apache.flink.api.java.DataSet;

/**
 * .
 */
public class DistinctDataSetProducer extends AbstractDataSetProducer<DistinctTransformStep> {

    @Override
    public void validate(DistinctTransformStep transform, DataSetProduceContext dataSetContext) {
        dataSetContext.getTransformStepDAG().addEdge(transform.getInput(),transform);
    }

    @Override
    public DataSet<Record> produce(DistinctTransformStep transform,DataSetProduceContext dataSetProduceContext) throws Exception {
        return dataSetProduceContext.getDataSet(transform.getInput()).distinct();
    }
}
