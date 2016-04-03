package org.etl.spi.flink;

import org.etl.api.Record;
import org.etl.api.transform.TransformStep;
import org.apache.flink.api.java.DataSet;


public interface DataSetProducer<T extends TransformStep>   {

    void validate(T transform,DataSetProduceContext dataSetContext);

    DataSet<Record> produce(T transform,DataSetProduceContext dataSetContext) throws Exception;

}
