package org.etl.spi.flink;

import org.etl.api.Record;
import org.etl.api.transform.TransformStep;
import org.etl.api.transform.UnionTransformStep;
import org.apache.flink.api.java.DataSet;

/**
 * .
 */
public class UnionDataSetProducer extends AbstractDataSetProducer<UnionTransformStep> {

    @Override
    public void validate(UnionTransformStep transform, DataSetProduceContext dataSetContext) {
        for (TransformStep input : transform.getInputs()) {
            dataSetContext.getTransformStepDAG().addEdge(input, transform);
        }


    }

    @Override
    public DataSet<Record> produce(UnionTransformStep transform,final DataSetProduceContext dataSetProduceContext) throws Exception {
        DataSet<Record> resultDataSet = null;
        for (TransformStep transformStep : transform.getInputs()) {
            DataSet<Record>  dataSet = dataSetProduceContext.getDataSet(transformStep);
            if (resultDataSet == null) {
                resultDataSet = dataSet;
            }else {
                resultDataSet = resultDataSet.union(dataSet);
            }
        }
        return  resultDataSet;
    }


}
