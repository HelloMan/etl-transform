package org.etl.api.transform;

import java.util.Set;

/**
 * Created by jason zhang on 12/29/2015.
 */
public interface GroupingTransformStep extends SingleInputTransformStep {
    /**
     * specify the group field
     * @return
     */
    Set<String> getGroupFields();
    /**
     *
     * @return
     */
    Set<Ranking> getRanking();


    Integer getTopN();

    /**
     * indicate the reduce should work on a sort group
     * @return
     */
    Set<Sorter> getSortFields();

}
