/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.db.table;

import java.util.ArrayList;

import com.lealone.common.exceptions.DbException;
import com.lealone.common.util.Utils;
import com.lealone.db.result.Row;
import com.lealone.db.result.SearchRow;
import com.lealone.db.schema.Schema;
import com.lealone.db.session.ServerSession;
import com.lealone.db.stats.QueryStatisticsData;

/**
 * This class is responsible to build the database performance meta data pseudo tables.
 */
public class PerfMetaTable extends MetaTable {

    private static final int QUERY_STATISTICS = 0;

    public static int getMetaTableTypeCount() {
        return QUERY_STATISTICS + 1;
    }

    public PerfMetaTable(Schema schema, int id, int type) {
        super(schema, id, type);
    }

    @Override
    public String createColumns() {
        Column[] cols;
        String indexColumnName = null;
        switch (type) {
        case QUERY_STATISTICS: {
            setObjectName("QUERY_STATISTICS");
            cols = createColumns("SQL_STATEMENT", "EXECUTION_COUNT INT", "MIN_EXECUTION_TIME DOUBLE",
                    "MAX_EXECUTION_TIME DOUBLE", "CUMULATIVE_EXECUTION_TIME DOUBLE",
                    "AVERAGE_EXECUTION_TIME DOUBLE", "STD_DEV_EXECUTION_TIME DOUBLE",
                    "MIN_ROW_COUNT INT", "MAX_ROW_COUNT INT", "CUMULATIVE_ROW_COUNT LONG",
                    "AVERAGE_ROW_COUNT DOUBLE", "STD_DEV_ROW_COUNT DOUBLE");
            break;
        }
        default:
            throw DbException.getInternalError("type=" + type);
        }
        setColumns(cols);
        return indexColumnName;
    }

    @Override
    public ArrayList<Row> generateRows(ServerSession session, SearchRow first, SearchRow last) {
        ArrayList<Row> rows = Utils.newSmallArrayList();
        switch (type) {
        case QUERY_STATISTICS: {
            QueryStatisticsData statData = database.getQueryStatisticsData();
            if (statData != null) {
                for (QueryStatisticsData.QueryEntry entry : statData.getQueries()) {
                    add(rows,
                            // SQL_STATEMENT
                            entry.sqlStatement,
                            // EXECUTION_COUNT
                            "" + entry.count,
                            // MIN_EXECUTION_TIME
                            "" + entry.executionTimeMinNanos / 1000d / 1000,
                            // MAX_EXECUTION_TIME
                            "" + entry.executionTimeMaxNanos / 1000d / 1000,
                            // CUMULATIVE_EXECUTION_TIME
                            "" + entry.executionTimeCumulativeNanos / 1000d / 1000,
                            // AVERAGE_EXECUTION_TIME
                            "" + entry.executionTimeMeanNanos / 1000d / 1000,
                            // STD_DEV_EXECUTION_TIME
                            "" + entry.getExecutionTimeStandardDeviation() / 1000d / 1000,
                            // MIN_ROW_COUNT
                            "" + entry.rowCountMin,
                            // MAX_ROW_COUNT
                            "" + entry.rowCountMax,
                            // CUMULATIVE_ROW_COUNT
                            "" + entry.rowCountCumulative,
                            // AVERAGE_ROW_COUNT
                            "" + entry.rowCountMean,
                            // STD_DEV_ROW_COUNT
                            "" + entry.getRowCountStandardDeviation());
                }
            }
            break;
        }
        default:
            throw DbException.getInternalError("type=" + type);
        }
        return rows;
    }
}
