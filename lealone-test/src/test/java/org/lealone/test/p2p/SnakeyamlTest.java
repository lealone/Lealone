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
