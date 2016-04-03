package org.etl.api.transform;

/**
 * Created by jason zhang on 12/31/2015.
 */
public interface FileDataSource extends AbstractDataSource {
    /**
     * the name of file
     * @return
     */
    String getFileName();
}
