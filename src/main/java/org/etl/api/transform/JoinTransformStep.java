package org.etl.api.transform;

import java.util.Set;

/**
 * Created by jason zhang on 12/28/2015.
 */
public interface JoinTransformStep extends TwoInputTransformStep {


    /**
     * join fields
     * @return
     */
    Set<JoinKeyField> getJoinKeyFields();

    /**
     * projection fields after join
     * @return
     */
    Set<ProjectionField> getProjectionFields();

}
