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
package org.lealone.test.netty;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.nio.channels.spi.SelectorProvider;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

import javax.net.ssl.SSLSocketFactory;

import org.lealone.test.netty.echo.EchoServerHandler;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.internal.PlatformDependent;
import io.netty.util.internal.SocketUtils;
import io.netty.util.internal.SystemPropertyUtil;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

//Netty为什么慢:
//触发了两次NetworkInterface.getNetworkInterfaces()
//调用了一次SelectorProvider.provider().openSelector() //背后也是触发次NetworkInterface.getNetworkInterfaces()
//这两个api都是很慢的
//触发的两次NetworkInterface.getNetworkInterfaces()在下面代码中
//1. io.netty.util.NetUtil的static初始化代码(没有办法屏蔽)
//2. io.netty.channel.DefaultChannelId的static初始化代码(通过org.lealone.test.TestBase.optimizeNetty()屏蔽)
//      ->io.netty.util.internal.MacAddressUtil.defaultMachineId()
//         -> io.netty.util.internal.MacAddressUtil.bestAvailableMac()
public class NettyTest {
    private static final InternalLogger logger = InternalLoggerFactory.getInstance(NettyTest.class);
    static final int PORT = Integer.parseInt(System.getProperty("port", "8007"));

    public static void main(String[] args) throws Exception {
        expensiveApi();
        start();
    }

    public static void expensiveApi() throws Exception {
        long total = 0;
        long t1 = System.currentTimeMillis();
        SelectorProvider.provider().openSelector();
        long t2 = System.currentTimeMillis();
        System.out.println("1 openSelector: " + (t2 - t1) + "ms");
        total += (t2 - t1);

        t1 = System.currentTimeMillis();
        SelectorProvider.provider().openSelector(); // 第二次就不耗时了
        t2 = System.currentTimeMillis();
        System.out.println("2 openSelector: " + (t2 - t1) + "ms");

        // NetworkInterface.getNetworkInterfaces()每次调用都耗时
        t1 = System.currentTimeMillis();
        NetworkInterface.getNetworkInterfaces();
        t2 = System.currentTimeMillis();
        System.out.println("1 NetworkInterface.getNetworkInterfaces(): " + (t2 - t1) + "ms");
        total += (t2 - t1);

        t1 = System.currentTimeMillis();
        t2 = io.netty.util.NetUtil.SOMAXCONN; // 里面通过static初始化代码触发 NetworkInterface.getNetworkInterfaces()
        t2 = System.currentTimeMillis();
        System.out.println("2 NetworkInterface.getNetworkInterfaces(): " + (t2 - t1) + "ms");
        total += (t2 - t1);

        t1 = System.currentTimeMillis();
        // 里面也会触发 NetworkInterface.getNetworkInterfaces()，只触发一次
        javax.net.ssl.SSLContext.getInstance("TLS");
        t2 = System.currentTimeMillis();
        System.out.println("1 javax.net.ssl.SSLContext.getInstance: " + (t2 - t1) + "ms");
        total += (t2 - t1);

        // 这一次就不触发NetworkInterface.getNetworkInterfaces()了
        t1 = System.currentTimeMillis();
        javax.net.ssl.SSLContext.getInstance("TLS");
        t2 = System.currentTimeMillis();
        System.out.println("2 javax.net.ssl.SSLContext.getInstance: " + (t2 - t1) + "ms");
        total += (t2 - t1);

        // 这一次就不触发NetworkInterface.getNetworkInterfaces()了
        t1 = System.currentTimeMillis();
        // 同javax.net.ssl.SSLContext.getInstance("TLS");
        ((SSLSocketFactory) SSLSocketFactory.getDefault()).getDefaultCipherSuites();
        t2 = System.currentTimeMillis();
        System.out.println("3 getDefaultCipherSuites: " + (t2 - t1) + "ms");
        total += (t2 - t1);

        System.out.println("total: " + total + "ms");
    }

    public static void start() throws Exception {
        // org.lealone.test.TestBase.optimizeNetty();
        long t1 = System.currentTimeMillis();
        NioEventLoopGroup bossGroup = new NioEventLoopGroup(1);
        NioEventLoopGroup workerGroup = new NioEventLoopGroup();
        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup).channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        public void initChannel(SocketChannel ch) throws Exception {
                        }
                    });
            ChannelFuture f = b.bind(PORT).sync();
            long t2 = System.currentTimeMillis();
            System.out.println("total: " + (t2 - t1) + "ms");
            f.channel().closeFuture().sync();
        } finally {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }

    public static void main2(String[] args) throws Exception {
        org.lealone.test.TestBase.optimizeNetty();

        long t0 = System.currentTimeMillis();
        long t1 = System.currentTimeMillis();
        // 这行执行很慢
        // String[] a = ((SSLSocketFactory) SSLSocketFactory.getDefault()).getDefaultCipherSuites();
        // System.out.println(a.length);
        long t2 = System.currentTimeMillis();
        // System.out.println("getDefaultCipherSuites: " + (t2 - t1) + "ms");

        t1 = System.currentTimeMillis();
        NetworkInterface.getNetworkInterfaces();
        t2 = System.currentTimeMillis();
        System.out.println("1 NetworkInterface.getNetworkInterfaces(): " + (t2 - t1) + "ms");

        t1 = System.currentTimeMillis();
        NetworkInterface.getNetworkInterfaces();
        t2 = System.currentTimeMillis();
        System.out.println("2 NetworkInterface.getNetworkInterfaces(): " + (t2 - t1) + "ms");

        t1 = System.currentTimeMillis();
        PlatformDependent.isWindows();
        t2 = System.currentTimeMillis();
        System.out.println("PlatformDependent init: " + (t2 - t1) + "ms");

        t1 = System.currentTimeMillis();
        // 调用了NetworkInterface.getNetworkInterfaces()导致很慢(近400ms)
        System.out.println(io.netty.util.NetUtil.SOMAXCONN);
        // test();
        t2 = System.currentTimeMillis();
        System.out.println("NetUtil init: " + (t2 - t1) + "ms");

        t1 = System.currentTimeMillis();
        SelectorProvider.provider().openSelector();
        t2 = System.currentTimeMillis();
        System.out.println("1 openSelector: " + (t2 - t1) + "ms");

        t1 = System.currentTimeMillis();
        SelectorProvider.provider().openSelector();
        t2 = System.currentTimeMillis();
        System.out.println("2 openSelector: " + (t2 - t1) + "ms");

        t1 = System.currentTimeMillis();
        NioEventLoopGroup bossGroup = new NioEventLoopGroup(1);
        t2 = System.currentTimeMillis();
        System.out.println("1 new NioEventLoopGroup: " + (t2 - t1) + "ms");

        t1 = System.currentTimeMillis();
        NioEventLoopGroup workerGroup = new NioEventLoopGroup();
        t2 = System.currentTimeMillis();
        System.out.println("2 new NioEventLoopGroup: " + (t2 - t1) + "ms");
        System.out.println("1 total: " + (t2 - t0) + "ms");

        try {
            t1 = System.currentTimeMillis();
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup).channel(NioServerSocketChannel.class).option(ChannelOption.SO_BACKLOG, 100)
                    .handler(new LoggingHandler(LogLevel.INFO)).childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        public void initChannel(SocketChannel ch) throws Exception {
                            ChannelPipeline p = ch.pipeline();
                            // p.addLast(new LoggingHandler(LogLevel.INFO));
                            p.addLast(new EchoServerHandler());
                        }
                    });
            t2 = System.currentTimeMillis();
            System.out.println("init ServerBootstrap: " + (t2 - t1) + "ms");

            t1 = System.currentTimeMillis();
            // Start the server.
            ChannelFuture f = b.bind(PORT).sync();
            t2 = System.currentTimeMillis();
            System.out.println("bind ServerBootstrap: " + (t2 - t1) + "ms");
            System.out.println("2 total: " + (t2 - t0) + "ms");
            // Wait until the server socket is closed.
            f.channel().closeFuture().sync();
        } finally {
            // Shut down all event loops to terminate all threads.
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }

    @SuppressWarnings("unused")
    static void test() {

        byte[] LOCALHOST4_BYTES = { 127, 0, 0, 1 };
        byte[] LOCALHOST6_BYTES = { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1 };
        // Create IPv4 loopback address.
        Inet4Address localhost4 = null;
        try {
            localhost4 = (Inet4Address) InetAddress.getByAddress("localhost", LOCALHOST4_BYTES);
        } catch (Exception e) {
            // We should not get here as long as the length of the address is correct.
            PlatformDependent.throwException(e);
        }
        Inet4Address LOCALHOST4 = localhost4;

        // Create IPv6 loopback address.
        Inet6Address localhost6 = null;
        try {
            localhost6 = (Inet6Address) InetAddress.getByAddress("localhost", LOCALHOST6_BYTES);
        } catch (Exception e) {
            // We should not get here as long as the length of the address is correct.
            PlatformDependent.throwException(e);
        }
        Inet6Address LOCALHOST6 = localhost6;
        // Retrieve the list of available network interfaces.
        List<NetworkInterface> ifaces = new ArrayList<NetworkInterface>();
        try {
            long t1 = System.currentTimeMillis();
            Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
            long t2 = System.currentTimeMillis();
            System.out.println("xxxxxxxxx init: " + (t2 - t1) + "ms");
            if (interfaces != null) {
                while (interfaces.hasMoreElements()) {
                    NetworkInterface iface = interfaces.nextElement();
                    // Use the interface with proper INET addresses only.
                    if (SocketUtils.addressesFromNetworkInterface(iface).hasMoreElements()) {
                        ifaces.add(iface);
                    }
                }
            }
        } catch (SocketException e) {
            logger.warn("Failed to retrieve the list of available network interfaces", e);
        }
        // Find the first loopback interface available from its INET address (127.0.0.1 or ::1)
        // Note that we do not use NetworkInterface.isLoopback() in the first place because it takes long time
        // on a certain environment. (e.g. Windows with -Djava.net.preferIPv4Stack=true)
        NetworkInterface loopbackIface = null;
        InetAddress loopbackAddr = null;
        loop: for (NetworkInterface iface : ifaces) {
            for (Enumeration<InetAddress> i = SocketUtils.addressesFromNetworkInterface(iface); i.hasMoreElements();) {
                InetAddress addr = i.nextElement();
                if (addr.isLoopbackAddress()) {
                    // Found
                    loopbackIface = iface;
                    loopbackAddr = addr;
                    break loop;
                }
            }
        }

        // If failed to find the loopback interface from its INET address, fall back to isLoopback().
        if (loopbackIface == null) {
            try {
                for (NetworkInterface iface : ifaces) {
                    if (iface.isLoopback()) {
                        Enumeration<InetAddress> i = SocketUtils.addressesFromNetworkInterface(iface);
                        if (i.hasMoreElements()) {
                            // Found the one with INET address.
                            loopbackIface = iface;
                            loopbackAddr = i.nextElement();
                            break;
                        }
                    }
                }

                if (loopbackIface == null) {
                    logger.warn("Failed to find the loopback interface");
                }
            } catch (SocketException e) {
                logger.warn("Failed to find the loopback interface", e);
            }
        }
        if (loopbackIface != null) {
            // Found the loopback interface with an INET address.
            logger.debug("Loopback interface: {} ({}, {})", loopbackIface.getName(), loopbackIface.getDisplayName(),
                    loopbackAddr.getHostAddress());
        } else {
            // Could not find the loopback interface, but we can't leave LOCALHOST as null.
            // Use LOCALHOST6 or LOCALHOST4, preferably the IPv6 one.
            if (loopbackAddr == null) {
                try {
                    if (NetworkInterface.getByInetAddress(LOCALHOST6) != null) {
                        logger.debug("Using hard-coded IPv6 localhost address: {}", localhost6);
                        loopbackAddr = localhost6;
                    }
                } catch (Exception e) {
                    // Ignore
                } finally {
                    if (loopbackAddr == null) {
                        logger.debug("Using hard-coded IPv4 localhost address: {}", localhost4);
                        loopbackAddr = localhost4;
                    }
                }
            }
        }

        NetworkInterface LOOPBACK_IF = loopbackIface;
        InetAddress LOCALHOST = loopbackAddr;

        long t1 = System.currentTimeMillis();
        // As a SecurityManager may prevent reading the somaxconn file we wrap this in a privileged block.
        //
        // See https://github.com/netty/netty/issues/3680
        int SOMAXCONN = AccessController.doPrivileged(new PrivilegedAction<Integer>() {
            @Override
            public Integer run() {
                // Determine the default somaxconn (server socket backlog) value of the platform.
                // The known defaults:
                // - Windows NT Server 4.0+: 200
                // - Linux and Mac OS X: 128
                int somaxconn = PlatformDependent.isWindows() ? 200 : 128;
                File file = new File("/proc/sys/net/core/somaxconn");
                BufferedReader in = null;
                try {
                    // file.exists() may throw a SecurityException if a SecurityManager is used, so execute it in the
                    // try / catch block.
                    // See https://github.com/netty/netty/issues/4936
                    if (file.exists()) {
                        in = new BufferedReader(new FileReader(file));
                        somaxconn = Integer.parseInt(in.readLine());
                        if (logger.isDebugEnabled()) {
                            logger.debug("{}: {}", file, somaxconn);
                        }
                    } else {
                        // Try to get from sysctl
                        Integer tmp = null;
                        if (SystemPropertyUtil.getBoolean("io.netty.net.somaxconn.trySysctl", false)) {
                            tmp = sysctlGetInt("kern.ipc.somaxconn");
                            if (tmp == null) {
                                tmp = sysctlGetInt("kern.ipc.soacceptqueue");
                                if (tmp != null) {
                                    somaxconn = tmp;
                                }
                            } else {
                                somaxconn = tmp;
                            }
                        }

                        if (tmp == null) {
                            logger.debug("Failed to get SOMAXCONN from sysctl and file {}. Default: {}", file,
                                    somaxconn);
                        }
                    }
                } catch (Exception e) {
                    logger.debug("Failed to get SOMAXCONN from sysctl and file {}. Default: {}", file, somaxconn, e);
                } finally {
                    if (in != null) {
                        try {
                            in.close();
                        } catch (Exception e) {
                            // Ignored.
                        }
                    }
                }
                return somaxconn;
            }
        });
        long t2 = System.currentTimeMillis();
        System.out.println("xxxxxxxxx init: " + (t2 - t1) + "ms");
    }

    private static Integer sysctlGetInt(String sysctlKey) throws IOException {
        Process process = new ProcessBuilder("sysctl", sysctlKey).start();
        try {
            InputStream is = process.getInputStream();
            InputStreamReader isr = new InputStreamReader(is);
            BufferedReader br = new BufferedReader(isr);
            try {
                String line = br.readLine();
                if (line.startsWith(sysctlKey)) {
                    for (int i = line.length() - 1; i > sysctlKey.length(); --i) {
                        if (!Character.isDigit(line.charAt(i))) {
                            return Integer.valueOf(line.substring(i + 1, line.length()));
                        }
                    }
                }
                return null;
            } finally {
                br.close();
            }
        } finally {
            if (process != null) {
                process.destroy();
            }
        }
    }
}
