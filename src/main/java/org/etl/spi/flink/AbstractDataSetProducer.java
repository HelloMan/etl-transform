package org.etl.spi.flink;

import org.etl.api.transform.TransformStep;


public abstract  class AbstractDataSetProducer<T extends TransformStep> implements DataSetProducer<T> {

    @Override
    public void  validate(T transform,DataSetProduceContext dataSetContext) {

    }
}

