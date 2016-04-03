package org.etl.api.transform;

import java.util.Set;

/**
 * Created by jason zhang on 1/5/2016.
 */
public interface DistinctTransformStep extends SingleInputTransformStep {

    Set<String> getDistinctFields();
}

