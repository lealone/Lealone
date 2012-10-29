package org.apache.hadoop.hbase.h2;

import java.sql.SQLException;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.h2.tools.Server;
import org.h2.server.Service;

public class TcpServer {
	public static void start(Log log, final Configuration conf, HMaster master, HRegionServer regionServer) {
		ArrayList<String> list = new ArrayList<String>();
		int tcpPort = conf.getInt("h2.tcpPort", org.h2.engine.Constants.DEFAULT_TCP_PORT);
		list.add("-tcp");
		list.add("-tcpPort");
		list.add("" + tcpPort);

		try {
			Server server = Server.createTcpServer(list.toArray(new String[list.size()]));
			Service service = server.getService();
			org.h2.server.TcpServer tcpServer = (org.h2.server.TcpServer) service;
			tcpServer.setMaster(master);
			tcpServer.setRegionServer(regionServer);
			server.start();

			log.info("Started H2 database tcp server at port " + tcpPort);
			//org.h2.tools.Server.main(list.toArray(new String[list.size()]));
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

}
