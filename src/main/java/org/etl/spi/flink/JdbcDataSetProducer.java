package org.etl.spi.flink;

import org.etl.api.Record;
import org.etl.api.transform.JdbcDataSource;
import org.etl.spi.flink.inputformat.JdbcInputFormat;
import org.apache.flink.api.java.DataSet;

/**
 * .
 */
public class JdbcDataSetProducer extends AbstractDataSetProducer<JdbcDataSource> {

    @Override
    public void validate(JdbcDataSource transform, DataSetProduceContext dataSetContext) {
        dataSetContext.getTransformStepDAG().addVertex(transform);
    }

    @Override
    public DataSet<Record> produce(JdbcDataSource transform,DataSetProduceContext dataSetProduceContext) throws Exception {

        return dataSetProduceContext.getExecutionEnvironment().createInput(
                JdbcInputFormat.buildJDBCInputFormat()
                        .setDBUrl(transform.getJdbcUrl())
                        .setUsername(transform.getUserName())
                        .setPassword(transform.getPassword())
                        .setQuery(transform.getSql())
                        .finish());

    }
}
