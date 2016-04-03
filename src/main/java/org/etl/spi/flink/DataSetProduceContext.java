package org.etl.spi.flink;

import org.etl.api.Record;
import org.etl.api.transform.TransformStep;
import org.etl.util.DirectedAcyclicGraph;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;


public interface DataSetProduceContext {

    /**
     * getDataSet dataSet from context
     * @param transformStep
     * @return
     */
    DataSet<Record> getDataSet(TransformStep transformStep) throws Exception;

    /**
     * getDataSet the execution environment
     * @return
     */
    ExecutionEnvironment getExecutionEnvironment();

    DirectedAcyclicGraph<TransformStep> getTransformStepDAG();

}
