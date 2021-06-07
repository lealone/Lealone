/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.p2p;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Map;

import javax.net.ssl.SSLSocketFactory;

import org.lealone.common.util.IOUtils;
import org.yaml.snakeyaml.Yaml;

public class SnakeyamlTest {

    public static void main(String[] args) throws Exception {
        String str = "key: v1";
        Yaml yaml = new Yaml();
        long t1 = System.currentTimeMillis();
        Object ret = yaml.load(str);
        long t2 = System.currentTimeMillis();
        System.out.println("time1=" + (t2 - t1) + "ms");
        URL url = new File("./src/test/resources/lealone-test.yaml").toURI().toURL();

        byte[] configBytes;
        try (InputStream is = url.openStream()) {
            configBytes = IOUtils.toByteArray(is);
        } catch (IOException e) {
            throw new AssertionError(e);
        }
        t1 = System.currentTimeMillis();
        ret = yaml.load(new ByteArrayInputStream(configBytes));
        t2 = System.currentTimeMillis();
        System.out.println("time2=" + (t2 - t1) + "ms");
        // System.out.println(ret);

        Map<?, ?> map = (Map<?, ?>) ret;
        ret = map.get("base_dir");
        ret = map.get("listen_interface_prefer_ipv6");
        ret = map.get("storage_engines");
        ret = map.get("protocol_server_engines");
        ret = map.get("cluster_config");
        // t1 = System.currentTimeMillis();
        // Config config = Config.parse(map);
        // t2 = System.currentTimeMillis();
        // System.out.println("time3=" + (t2 - t1) + "ms");
        // System.out.println(config);
        System.out.println(ret);

        // 这行执行很慢
        String[] a = ((SSLSocketFactory) SSLSocketFactory.getDefault()).getDefaultCipherSuites();
        System.out.println(a.length);
    }
}
