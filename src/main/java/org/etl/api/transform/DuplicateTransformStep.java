package org.etl.api.transform;

/**
 * .
 */
public interface DuplicateTransformStep extends SingleInputTransformStep {

    /**
     * if the input tuples match the condition then will duplicate a new record
     * @return
     */
    String getCondition();

}
