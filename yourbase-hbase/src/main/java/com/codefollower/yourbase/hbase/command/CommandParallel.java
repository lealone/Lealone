package com.codefollower.yourbase.hbase.command;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hbase.util.Threads;

import com.codefollower.yourbase.command.Command;
import com.codefollower.yourbase.command.CommandInterface;
import com.codefollower.yourbase.constant.ErrorCode;
import com.codefollower.yourbase.engine.Session;
import com.codefollower.yourbase.expression.ParameterInterface;
import com.codefollower.yourbase.hbase.engine.HBaseSession;
import com.codefollower.yourbase.hbase.util.HBaseRegionInfo;
import com.codefollower.yourbase.hbase.util.HBaseUtils;
import com.codefollower.yourbase.message.DbException;
import com.codefollower.yourbase.result.ResultInterface;
import com.codefollower.yourbase.util.New;

public class CommandParallel implements CommandInterface {
    private static ThreadPoolExecutor pool;

    private final String sql;
    private List<CommandInterface> commands;

    private static class CommandWrapper implements CommandInterface {
        private Command c;
        private Session session;

        CommandWrapper(Command c, Session session) {
            this.c = c;
            this.session = session;
        }

        @Override
        public int getCommandType() {
            return c.getCommandType();
        }

        @Override
        public boolean isQuery() {
            return c.isQuery();
        }

        @Override
        public ArrayList<? extends ParameterInterface> getParameters() {
            return c.getParameters();
        }

        @Override
        public ResultInterface executeQuery(int maxRows, boolean scrollable) {
            return c.executeQuery(maxRows, scrollable);
        }

        @Override
        public int executeUpdate() {
            return c.executeUpdate();
        }

        @Override
        public void close() {
            session.close();
            c.close();
        }

        @Override
        public void cancel() {
            c.cancel();
        }

        @Override
        public ResultInterface getMetaData() {
            return c.getMetaData();
        }

    }

    public CommandParallel(HBaseSession session, CommandProxy commandProxy, //
            byte[] tableName, List<byte[]> startKeys, String sql) {
        this.sql = sql;
        try {
            if (pool == null) {
                synchronized (CommandParallel.class) {
                    if (pool == null) {
                        pool = new ThreadPoolExecutor(1, 20, 5, TimeUnit.SECONDS, new SynchronousQueue<Runnable>(),
                                Threads.newDaemonThreadFactory("CommandParallel"));
                        pool.allowCoreThreadTimeOut(true);
                    }
                }
            }
            commands = new ArrayList<CommandInterface>(startKeys.size());
            for (byte[] startKey : startKeys) {
                HBaseRegionInfo hri = HBaseUtils.getHBaseRegionInfo(tableName, startKey);
                if (CommandProxy.isLocal(session, hri)) {
                    //必须使用新Session，否则在并发执行executeUpdate时因为使用session对象来同步所以造成死锁
                    HBaseSession newSession = (HBaseSession) session.getDatabase().createSession(session.getUser());
                    newSession.setRegionServer(session.getRegionServer());
                    newSession.setFetchSize(session.getFetchSize());
                    Command c = newSession.prepareLocal(sql);
                    HBasePrepared hp = (HBasePrepared) c.getPrepared();
                    hp.setRegionName(hri.getRegionName());
                    commands.add(new CommandWrapper(c, newSession));
                } else {
                    commands.add(commandProxy.getCommandInterface(session.getOriginalProperties(), hri.getRegionServerURL(),
                            commandProxy.createSQL(hri.getRegionName(), sql)));
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String toString() {
        return sql;
    }

    @Override
    public int getCommandType() {
        return UNKNOWN;
    }

    @Override
    public boolean isQuery() {
        return false;
    }

    @Override
    public ArrayList<? extends ParameterInterface> getParameters() {
        if (commands != null)
            for (CommandInterface c : commands)
                return c.getParameters();
        return null;
    }

    @Override
    public ResultInterface executeQuery(int maxRows, boolean scrollable) {
        throw DbException.get(ErrorCode.METHOD_NOT_ALLOWED_FOR_QUERY);
    }

    @Override
    public int executeUpdate() {
        int updateCount = 0;
        if (commands != null) {
            int size = commands.size();
            List<Future<Integer>> futures = New.arrayList(size);
            for (int i = 0; i < size; i++) {
                final CommandInterface c = commands.get(i);
                futures.add(pool.submit(new Callable<Integer>() {
                    public Integer call() throws Exception {
                        return c.executeUpdate();
                    }
                }));
            }
            try {
                for (int i = 0; i < size; i++) {
                    updateCount += futures.get(i).get();
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return updateCount;
    }

    @Override
    public void close() {
        if (commands != null)
            for (CommandInterface c : commands)
                c.close();
    }

    @Override
    public void cancel() {
        if (commands != null)
            for (CommandInterface c : commands)
                c.cancel();
    }

    @Override
    public ResultInterface getMetaData() {
        if (commands != null)
            for (CommandInterface c : commands)
                return c.getMetaData();
        return null;
    }
}
