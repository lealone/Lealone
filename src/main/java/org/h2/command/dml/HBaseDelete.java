package org.h2.command.dml;

import java.io.IOException;

import org.apache.hadoop.hbase.util.Bytes;
import org.h2.api.Trigger;
import org.h2.engine.Right;
import org.h2.engine.Session;
import org.h2.expression.Expression;
import org.h2.table.HBaseTable;
import org.h2.table.PlanItem;
import org.h2.table.Table;
import org.h2.table.TableFilter;

//TODO 目前只能按where rowKey=???的方式删除，还不支持按family、qualifier、timestamp删除
public class HBaseDelete extends Delete {
    private Expression condition;
    private TableFilter tableFilter;

    public HBaseDelete(Session session) {
        super(session);
    }

    @Override
    public void setTableFilter(TableFilter tableFilter) {
        super.setTableFilter(tableFilter);
        this.tableFilter = tableFilter;
    }

    @Override
    public void setCondition(Expression condition) {
        super.setCondition(condition);
        this.condition = condition;
    }

    @Override
    public int update() {
        Table table = tableFilter.getTable();
        session.getUser().checkRight(table, Right.DELETE);
        table.fire(session, Trigger.DELETE, true);
        table.lock(session, true, false);

        setCurrentRowNumber(0);
        String rowKey = getRowKey();
        if (rowKey == null)
            return 0;

        try {
            setCurrentRowNumber(1);
            session.getRegionServer().delete(session.getRegionName(),
                    new org.apache.hadoop.hbase.client.Delete(Bytes.toBytes(rowKey)));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        table.fire(session, Trigger.DELETE, false);
        return 1;
    }

    @Override
    public void prepare() {
        if (condition != null) {
            condition.mapColumns(tableFilter, 0);
            condition = condition.optimize(session);
            //condition.createIndexConditions(session, tableFilter);
        }
        PlanItem item = tableFilter.getBestPlanItem(session, 1);
        tableFilter.setPlanItem(item);
        tableFilter.prepare();
    }

    @Override
    public String getTableName() {
        return tableFilter.getTable().getName();
    }

    @Override
    public String getRowKey() {
        String[] rowKeys = null;
        if (condition != null) {
            String rowKeyName = ((HBaseTable) tableFilter.getTable()).getRowKeyName();
            rowKeys = condition.getRowKeys(rowKeyName, session);
        }
        if (rowKeys != null && rowKeys.length > 0)
            return rowKeys[0];
        return null;
    }

}
