package org.etl.api.transform;

import java.util.List;

/**
 * Created by jason zhang on 1/11/2016.
 */
public interface MultipleTransformStepStep extends TransformStep {

    List<TransformStep> getInputs();
}
