package org.etl.api.transform;

/**
 * .
 */
public interface TwoInputTransformStep extends TransformStep {

    /**
     * the first input transform
     * @return
     */
    TransformStep getInput1();

    /**
     * the second input transform
     * @return
     */
    TransformStep getInput2();
}
