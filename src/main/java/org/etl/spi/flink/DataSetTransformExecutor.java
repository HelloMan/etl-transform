package org.etl.spi.flink;

import org.etl.api.Record;
import org.etl.api.TransformExecutor;
import org.etl.api.TransformExecutorType;
import org.etl.api.transform.TransformStep;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;

import java.util.Map;


public class DataSetTransformExecutor implements TransformExecutor {

    @Override
    public void execute(TransformStep... transformSteps) throws Exception {
        DataSetProduceContext dataSetProduceContext = new DataSetProduceContextImpl();

        for (TransformStep transformStep : transformSteps) {
            dataSetProduceContext.getDataSet(transformStep);
        }



        for (TransformStep sinkTransformStep : dataSetProduceContext.getTransformStepDAG().getSinks()) {
            DataSet<Record> sinkDataSet = dataSetProduceContext.getDataSet(sinkTransformStep);
            sinkDataSet.map(new MapFunction<Record, Map>() {
                @Override
                public Map map(Record value) throws Exception {
                    return value.asMap();
                }
            }).print();
        }

    }

    @Override
    public TransformExecutorType getExecutorType() {
        return TransformExecutorType.Flink_DataSet;
    }


}
