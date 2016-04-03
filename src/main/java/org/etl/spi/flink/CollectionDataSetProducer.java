package org.etl.spi.flink;

import org.etl.api.Record;
import org.etl.api.transform.CollectionTransformStep;
import org.apache.flink.api.java.DataSet;


public class CollectionDataSetProducer extends AbstractDataSetProducer<CollectionTransformStep> {

    @Override
    public void validate(CollectionTransformStep transform, DataSetProduceContext dataSetContext) {
       dataSetContext.getTransformStepDAG().addVertex(transform);
    }

    @Override
    public DataSet<Record> produce(CollectionTransformStep transform, DataSetProduceContext dataSetContext) throws Exception {

        return dataSetContext.getExecutionEnvironment().fromCollection(transform.getCollection());
    }
}
