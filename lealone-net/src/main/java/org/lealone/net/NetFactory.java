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
package org.lealone.net;

import java.util.Properties;

import org.lealone.common.security.EncryptionOptions;
import org.lealone.common.security.EncryptionOptions.ClientEncryptionOptions;

import io.vertx.core.Vertx;
import io.vertx.core.http.ClientAuth;
import io.vertx.core.net.JksOptions;
import io.vertx.core.net.NetClientOptions;
import io.vertx.core.net.NetServer;
import io.vertx.core.net.NetServerOptions;

public class NetFactory {

    public static NetServer createNetServer(Vertx vertx, EncryptionOptions eo) {
        NetServerOptions netServerOptions = NetFactory.getNetServerOptions(eo);
        NetServer server = vertx.createNetServer(netServerOptions);
        return server;
    }

    public static NetServerOptions getNetServerOptions(EncryptionOptions eo) {
        if (eo == null) {
            return new NetServerOptions();
        }
        NetServerOptions options = new NetServerOptions().setSsl(true);
        options.setKeyStoreOptions(new JksOptions().setPath(eo.keystore).setPassword(eo.keystore_password));

        if (eo.truststore != null) {
            if (eo.require_client_auth) {
                options.setClientAuth(ClientAuth.REQUIRED);
            }
            options.setTrustStoreOptions(new JksOptions().setPath(eo.truststore).setPassword(eo.truststore_password));
        }

        if (eo.cipher_suites != null) {
            for (String cipherSuitee : eo.cipher_suites)
                options.addEnabledCipherSuite(cipherSuitee);
        }
        return options;
    }

    public static NetClientOptions getNetClientOptions(EncryptionOptions eo) {
        if (eo == null) {
            return new NetClientOptions();
        }
        NetClientOptions options = new NetClientOptions().setSsl(true);
        options.setKeyStoreOptions(new JksOptions().setPath(eo.keystore).setPassword(eo.keystore_password));

        if (eo.truststore != null) {
            options.setTrustStoreOptions(new JksOptions().setPath(eo.truststore).setPassword(eo.truststore_password));
        }

        if (eo.cipher_suites != null) {
            for (String cipherSuitee : eo.cipher_suites)
                options.addEnabledCipherSuite(cipherSuitee);
        }
        return options;
    }

    private static final String PREFIX = "lealone.security.";

    public static NetClientOptions getNetClientOptions(Properties prop) {
        if (prop == null || !prop.containsKey(PREFIX + "keystore")) {
            return new NetClientOptions();
        }

        ClientEncryptionOptions eo = new ClientEncryptionOptions();
        eo.keystore = prop.getProperty(PREFIX + "keystore");
        eo.keystore_password = prop.getProperty(PREFIX + "keystore.password");
        eo.truststore = prop.getProperty(PREFIX + "truststore");
        eo.truststore_password = prop.getProperty(PREFIX + "truststore.password");
        String cipher_suites = prop.getProperty(PREFIX + "cipher.suites");
        if (cipher_suites != null)
            eo.cipher_suites = cipher_suites.split(",");

        return getNetClientOptions(eo);
    }
}
