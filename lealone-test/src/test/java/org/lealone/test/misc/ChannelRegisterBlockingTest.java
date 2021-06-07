/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.misc;

import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

public class ChannelRegisterBlockingTest {
    public static void main(String[] args) throws Exception {
        blocking();
        nonblocking();
    }

    public static void blocking() throws Exception {
        Selector selector = Selector.open();
        System.out.println("step 1");
        new Thread(() -> {
            try {
                System.out.println("step 2");
                selector.select(3000);
                System.out.println("step 3");
            } catch (Exception e) {
            }
        }).start();
        Thread.sleep(1000);

        SocketChannel channel = SocketChannel.open();
        channel.configureBlocking(false);
        channel.register(selector, SelectionKey.OP_CONNECT); // 直到select结束时才会被执行
        System.out.println("step 4");
    }

    public static void nonblocking() throws Exception {
        // server event loop
        new Thread(() -> {
            try {
                Selector selector = Selector.open();
                ServerSocketChannel serverChannel = ServerSocketChannel.open();
                serverChannel.socket().bind(new InetSocketAddress("127.0.0.1", 9000));
                serverChannel.configureBlocking(false);
                serverChannel.register(selector, SelectionKey.OP_ACCEPT);

                while (true) {
                    selector.select(1000);
                    Set<SelectionKey> keys = selector.selectedKeys();
                    for (SelectionKey key : keys) {
                        if (key.isValid() && key.isAcceptable()) {
                            // SocketChannel channel = serverChannel.accept();
                            // System.out.println("accept: " + channel.getRemoteAddress());
                        }
                    }
                    keys.clear();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();

        AtomicBoolean selecting = new AtomicBoolean(false);
        Selector clientSelector = Selector.open();
        // client event loop
        new Thread(() -> {
            try {
                while (true) {
                    // clientSelector.select(5000);
                    if (selecting.compareAndSet(false, true)) {
                        clientSelector.select(50000);
                        selecting.set(false);
                        Thread.sleep(10); // 实际场景在后面会有很多其他代码所以不需要这一句
                    }
                    Set<SelectionKey> keys = clientSelector.selectedKeys();
                    for (SelectionKey key : keys) {
                        if (key.isValid() && key.isConnectable()) {
                            SocketChannel channel = (SocketChannel) key.channel();
                            if (channel.isConnectionPending()) {
                                channel.finishConnect();
                                // System.out.println("connect: " + channel.getLocalAddress());
                                channel.register(clientSelector, SelectionKey.OP_READ);
                            }
                        }
                    }
                    keys.clear();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();

        // connect test
        new Thread(() -> {
            try {
                for (int i = 0; i < 20; i++) {
                    SocketChannel channel = SocketChannel.open();
                    channel.configureBlocking(false);

                    long t1 = System.currentTimeMillis();
                    // 这种方式会阻塞
                    // channel.register(clientSelector, SelectionKey.OP_CONNECT);

                    // 避免被client event loop阻塞
                    while (true) {
                        if (selecting.compareAndSet(false, true)) {
                            channel.register(clientSelector, SelectionKey.OP_CONNECT);
                            selecting.set(false);
                            clientSelector.wakeup();
                            break;
                        } else {
                            clientSelector.wakeup();
                        }
                    }
                    long t2 = System.currentTimeMillis();
                    System.out.println("register time: " + (t2 - t1) + " ms");

                    channel.connect(new InetSocketAddress("127.0.0.1", 9000));
                    Thread.sleep(100);
                }
                System.exit(-1);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();
    }
}
