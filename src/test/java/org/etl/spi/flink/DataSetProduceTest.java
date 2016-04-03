package org.etl.spi.flink;

import org.etl.api.TransformExecutor;
import org.etl.api.TransformExecutorFactory;
import org.etl.api.TransformExecutorType;
import org.etl.TransformStepFactory;
import org.junit.Test;


public class DataSetProduceTest {

    @Test
    public void testCollectionTransformStep() throws Exception {
        TransformExecutor transformExecutor = TransformExecutorFactory.getTransformExecutor(TransformExecutorType.Flink_DataSet);
        transformExecutor.execute(TransformStepFactory.createPhoneRecordTransformStep());

    }

    @Test
    public void testDuplicateTransformStep() throws Exception {
        TransformExecutor transformExecutor = TransformExecutorFactory.getTransformExecutor(TransformExecutorType.Flink_DataSet);
        transformExecutor.execute(TransformStepFactory.createDuplicateTransformStep());

    }

    @Test
    public void testFilterTransformStep() throws Exception {
        TransformExecutor transformExecutor = TransformExecutorFactory.getTransformExecutor(TransformExecutorType.Flink_DataSet);
        transformExecutor.execute(TransformStepFactory.createFilterTransformStep());

    }

    @Test
    public void testMapTransformStep() throws Exception {
        TransformExecutor transformExecutor = TransformExecutorFactory.getTransformExecutor(TransformExecutorType.Flink_DataSet);
        transformExecutor.execute(TransformStepFactory.createMapTrasformStep());
    }

    @Test
    public void testJoinTransformStep() throws Exception {
        TransformExecutor transformExecutor = TransformExecutorFactory.getTransformExecutor(TransformExecutorType.Flink_DataSet);
        transformExecutor.execute(TransformStepFactory.createJoinTransformStep());
    }

    @Test
    public void testAggregateTransformStep() throws Exception {
        TransformExecutor transformExecutor = TransformExecutorFactory.getTransformExecutor(TransformExecutorType.Flink_DataSet);
        transformExecutor.execute(TransformStepFactory.createAggregateTransformStep());
    }


}
