package org.etl.api.transform;

/**
 * Created by jason zhang on 12/31/2015.
 */
public interface SingleInputTransformStep extends TransformStep {

    /**
     * input transform
     * @return
     */
    TransformStep getInput();
}
