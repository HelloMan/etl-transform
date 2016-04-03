package org.etl.api;

import org.etl.api.transform.TransformStep;


public interface TransformExecutor {

    /**
     * execute all transform steps
     * @param transformSteps
     */
    void execute(TransformStep... transformSteps) throws Exception;

    /**
     * get the executor type
     * @return
     */
    TransformExecutorType getExecutorType();


}
