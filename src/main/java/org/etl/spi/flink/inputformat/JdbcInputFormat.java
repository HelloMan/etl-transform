package org.etl.spi.flink.inputformat;

import org.etl.api.Record;
import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.api.common.io.NonParallelInput;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.types.NullValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.*;


public class JdbcInputFormat  extends RichInputFormat<Record, InputSplit> implements NonParallelInput {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(JdbcInputFormat.class);
    private String username;
    private String password;
    private String drivername;
    private String dbURL;
    private String query;
    private transient Connection dbConn;
    private transient Statement statement;
    private transient ResultSet resultSet;
    private int[] columnTypes = null;
    private String[] columnNames = null;

    public JdbcInputFormat() {
    }

    public void configure(Configuration parameters) {
    }

    public void open(InputSplit ignored) throws IOException {
        try {
            this.establishConnection();
            this.statement = this.dbConn.createStatement(1004, 1007);
            this.resultSet = this.statement.executeQuery(this.query);
        } catch (SQLException var3) {
            this.close();
            throw new IllegalArgumentException("open() failed." + var3.getMessage(), var3);
        } catch (ClassNotFoundException var4) {
            throw new IllegalArgumentException("JDBC-Class not found. - " + var4.getMessage(), var4);
        }
    }

    private void establishConnection() throws SQLException, ClassNotFoundException {
        Class.forName(this.drivername);
        if(this.username == null) {
            this.dbConn = DriverManager.getConnection(this.dbURL);
        } else {
            this.dbConn = DriverManager.getConnection(this.dbURL, this.username, this.password);
        }

    }

    public void close() throws IOException {
        try {
            this.resultSet.close();
        } catch (SQLException var6) {
            LOG.info("Inputformat couldn\'t be closed - " + var6.getMessage());
        } catch (NullPointerException var7) {
            ;
        }

        try {
            this.statement.close();
        } catch (SQLException var4) {
            LOG.info("Inputformat couldn\'t be closed - " + var4.getMessage());
        } catch (NullPointerException var5) {
            ;
        }

        try {
            this.dbConn.close();
        } catch (SQLException var2) {
            LOG.info("Inputformat couldn\'t be closed - " + var2.getMessage());
        } catch (NullPointerException var3) {
            ;
        }

    }

    public boolean reachedEnd() throws IOException {
        try {
            if(this.resultSet.isLast()) {
                this.close();
                return true;
            } else {
                return false;
            }
        } catch (SQLException var2) {
            throw new IOException("Couldn\'t evaluate reachedEnd() - " + var2.getMessage(), var2);
        }
    }

    public Record nextRecord(Record record) throws IOException {
        try {
            this.resultSet.next();
            if(this.columnTypes == null) {
                this.extractTypes(record);
            }

            this.addValue(record);
            return record;
        } catch (SQLException var3) {
            this.close();
            throw new IOException("Couldn\'t read data - " + var3.getMessage(), var3);
        } catch (NullPointerException var4) {
            this.close();
            throw new IOException("Couldn\'t access resultSet", var4);
        }
    }

    private void extractTypes(Record tuple) throws SQLException, IOException {
        ResultSetMetaData resultSetMetaData = this.resultSet.getMetaData();
        this.columnTypes = new int[resultSetMetaData.getColumnCount()];
        this.columnNames = new String[resultSetMetaData.getColumnCount()];

        for(int pos = 0; pos < this.columnTypes.length; ++pos) {
            this.columnTypes[pos] = resultSetMetaData.getColumnType(pos + 1);
            this.columnNames[pos] = resultSetMetaData.getColumnName(pos + 1);
        }


    }

    private void addValue(Record reuse) throws SQLException {
        for(int pos = 0; pos < this.columnTypes.length; ++pos) {
            Comparable value = null;
            switch(this.columnTypes[pos]) {
                case java.sql.Types.NULL:
                    value = NullValue.getInstance();
                    break;
                case java.sql.Types.BOOLEAN:
                    value = resultSet.getBoolean(pos + 1);
                    break;
                case java.sql.Types.BIT:
                    value = resultSet.getBoolean(pos + 1);
                    break;
                case java.sql.Types.CHAR:
                    value = resultSet.getString(pos + 1);
                    break;
                case java.sql.Types.NCHAR:
                    value = resultSet.getString(pos + 1);
                    break;
                case java.sql.Types.VARCHAR:
                    value = resultSet.getString(pos + 1);
                    break;
                case java.sql.Types.LONGVARCHAR:
                    value = resultSet.getString(pos + 1);
                    break;
                case java.sql.Types.LONGNVARCHAR:
                    value = resultSet.getString(pos + 1);
                    break;
                case java.sql.Types.TINYINT:
                    value = resultSet.getShort(pos + 1);
                    break;
                case java.sql.Types.SMALLINT:
                    value = resultSet.getShort(pos + 1);
                    break;
                case java.sql.Types.BIGINT:
                    value = resultSet.getLong(pos + 1);
                    break;
                case java.sql.Types.INTEGER:
                    value = resultSet.getInt(pos + 1);
                    break;
                case java.sql.Types.FLOAT:
                    value = resultSet.getDouble(pos + 1);
                    break;
                case java.sql.Types.REAL:
                    value = resultSet.getFloat(pos + 1);
                    break;
                case java.sql.Types.DOUBLE:
                    value = resultSet.getDouble(pos + 1);
                    break;
                case java.sql.Types.DECIMAL:
                    value = resultSet.getBigDecimal(pos + 1);
                    break;
                case java.sql.Types.NUMERIC:
                    value = resultSet.getBigDecimal(pos + 1);
                    break;
                case java.sql.Types.DATE:
                    value = resultSet.getDate(pos + 1);
                    break;
                case java.sql.Types.TIME:
                    value = resultSet.getTime(pos + 1);
                    break;
                case java.sql.Types.TIMESTAMP:
                    value = resultSet.getTimestamp(pos + 1);
                    break;

                default:
                    throw new SQLException("Unsupported sql-type [" + columnTypes[pos] + "] on column [" + pos + "]");
            }

            reuse.setValue(this.columnNames[pos+1],value);
        }

    }

    public BaseStatistics getStatistics(BaseStatistics cachedStatistics) throws IOException {
        return cachedStatistics;
    }

    public InputSplit[] createInputSplits(int minNumSplits) throws IOException {
        GenericInputSplit[] split = new GenericInputSplit[]{new GenericInputSplit(0, 1)};
        return split;
    }

    public InputSplitAssigner getInputSplitAssigner(InputSplit[] inputSplits) {
        return new DefaultInputSplitAssigner(inputSplits);
    }

    public static JDBCInputFormatBuilder buildJDBCInputFormat() {
        return new JDBCInputFormatBuilder();
    }

    public static class JDBCInputFormatBuilder {
        private final JdbcInputFormat format = new JdbcInputFormat();

        public JDBCInputFormatBuilder() {
        }

        public JDBCInputFormatBuilder setUsername(String username) {
            this.format.username = username;
            return this;
        }

        public JDBCInputFormatBuilder setPassword(String password) {
            this.format.password = password;
            return this;
        }

        public JDBCInputFormatBuilder setDrivername(String drivername) {
            this.format.drivername = drivername;
            return this;
        }

        public JDBCInputFormatBuilder setDBUrl(String dbURL) {
            this.format.dbURL = dbURL;
            return this;
        }

        public JDBCInputFormatBuilder setQuery(String query) {
            this.format.query = query;
            return this;
        }

        public JdbcInputFormat finish() {
            if(this.format.username == null) {
                LOG.info("Username was not supplied separately.");
            }

            if(this.format.password == null) {
                LOG.info("Password was not supplied separately.");
            }

            if(this.format.dbURL == null) {
                throw new IllegalArgumentException("No dababase URL supplied.");
            } else if(this.format.query == null) {
                throw new IllegalArgumentException("No query suplied");
            } else if(this.format.drivername == null) {
                throw new IllegalArgumentException("No driver supplied");
            } else {
                return this.format;
            }
        }
    }
}
