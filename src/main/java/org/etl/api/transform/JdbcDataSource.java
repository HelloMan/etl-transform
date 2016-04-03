package org.etl.api.transform;

/**
 * Created by jason zhang on 12/29/2015.
 */
public interface JdbcDataSource extends AbstractDataSource {

    /**
     * the jdbc url
     * @return
     */
    String getJdbcUrl();

    /**
     * jdbc user name
     * @return
     */
    String getUserName();

    /**
     * jdbc password
     * @return
     */
    String getPassword();

    /**
     * query sql
     * @return
     */
    String getSql();

}
