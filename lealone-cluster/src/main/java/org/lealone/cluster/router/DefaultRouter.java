/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lealone.cluster.router;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

import org.lealone.cluster.config.DatabaseDescriptor;
import org.lealone.cluster.dht.Token;
import org.lealone.cluster.gms.FailureDetector;
import org.lealone.cluster.gms.Gossiper;
import org.lealone.cluster.service.StorageService;
import org.lealone.cluster.utils.FBUtilities;
import org.lealone.command.CommandInterface;
import org.lealone.command.FrontendCommand;
import org.lealone.command.ddl.DefineCommand;
import org.lealone.command.dml.Insert;
import org.lealone.command.router.CommandParallel;
import org.lealone.command.router.FrontendSessionPool;
import org.lealone.command.router.Router;
import org.lealone.message.DbException;
import org.lealone.result.Row;
import org.lealone.util.New;
import org.lealone.value.Value;

import com.google.common.collect.Iterables;

public class DefaultRouter implements Router {
    private static final DefaultRouter INSTANCE = new DefaultRouter();

    public static DefaultRouter getInstance() {
        return INSTANCE;
    }

    private DefaultRouter() {
    }

    @Override
    public int executeInsert(Insert insert) {
        final String localDataCenter = DatabaseDescriptor.getEndpointSnitch().getDatacenter(FBUtilities.getBroadcastAddress());
        String keyspaceName = insert.getTable().getSchema().getName();
        //AbstractReplicationStrategy rs = Keyspace.open(keyspaceName).getReplicationStrategy();

        List<Row> localRows = null;
        Map<InetAddress, List<Row>> localDataCenterRows = null;
        Map<InetAddress, List<Row>> remoteDataCenterRows = null;

        Value partitionKey;
        for (Row row : insert.getRows()) {
            partitionKey = row.getRowKey();
            Token tk = StorageService.getPartitioner().getToken(ByteBuffer.wrap(partitionKey.getBytesNoCopy()));
            List<InetAddress> naturalEndpoints = StorageService.instance.getNaturalEndpoints(keyspaceName, tk);
            Collection<InetAddress> pendingEndpoints = StorageService.instance.getTokenMetadata().pendingEndpointsFor(tk,
                    keyspaceName);

            Iterable<InetAddress> targets = Iterables.concat(naturalEndpoints, pendingEndpoints);
            for (InetAddress destination : targets) {
                if (FailureDetector.instance.isAlive(destination)) {
                    if (destination.equals(FBUtilities.getBroadcastAddress())) {
                        if (localRows == null)
                            localRows = New.arrayList();
                        localRows.add(row);
                    } else {
                        String dc = DatabaseDescriptor.getEndpointSnitch().getDatacenter(destination);
                        if (localDataCenter.equals(dc)) {
                            if (localDataCenterRows == null)
                                localDataCenterRows = New.hashMap();

                            List<Row> rows = localDataCenterRows.get(destination);
                            if (rows == null) {
                                rows = New.arrayList();
                                localDataCenterRows.put(destination, rows);
                            }
                            rows.add(row);
                        } else {
                            if (remoteDataCenterRows == null)
                                remoteDataCenterRows = New.hashMap();

                            List<Row> rows = remoteDataCenterRows.get(destination);
                            if (rows == null) {
                                rows = New.arrayList();
                                remoteDataCenterRows.put(destination, rows);
                            }
                            rows.add(row);
                        }
                    }
                }
            }
        }

        List<Callable<Integer>> commands = New.arrayList();
        int updateCount = 0;
        try {
            createUpdateCallable(insert, commands, localDataCenterRows);
            createUpdateCallable(insert, commands, remoteDataCenterRows);

            if (localRows != null) {
                insert.setRows(localRows);
                commands.add(insert);
            }

            updateCount = CommandParallel.executeUpdateCallable(commands);
        } catch (Exception e) {
            throw DbException.convert(e);
        }

        return updateCount;
    }

    private static void createUpdateCallable(Insert insert, //
            List<Callable<Integer>> commands, Map<InetAddress, List<Row>> rows) throws Exception {
        if (rows != null) {
            for (Map.Entry<InetAddress, List<Row>> e : rows.entrySet()) {
                final FrontendCommand c = FrontendSessionPool.getFrontendCommand(insert.getSession(), insert, //
                        insert.getSession().getURL(e.getKey()), insert.getPlanSQL(e.getValue()));
                Callable<Integer> call = new Callable<Integer>() {
                    @Override
                    public Integer call() throws Exception {
                        return c.executeUpdate();
                    }
                };

                commands.add(call);
            }
        }
    }

    @Override
    public int executeDefineCommand(DefineCommand defineCommand) {
        Set<InetAddress> liveMembers = Gossiper.instance.getLiveMembers();
        List<CommandInterface> commands = New.arrayList(liveMembers.size() - 1);
        FrontendCommand c;
        int updateCount = 0;
        try {
            for (InetAddress endpoint : liveMembers) {
                if (!endpoint.equals(FBUtilities.getBroadcastAddress())) {
                    c = FrontendSessionPool.getFrontendCommand(defineCommand.getSession(), defineCommand, //
                            defineCommand.getSession().getURL(endpoint), defineCommand.getSQL());

                    commands.add(c);
                }
            }
            updateCount = defineCommand.update();
            updateCount += CommandParallel.executeUpdate(commands);
        } catch (Exception e) {
            throw DbException.convert(e);
        }
        return updateCount;
    }
}
