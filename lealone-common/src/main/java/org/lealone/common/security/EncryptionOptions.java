/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.common.security;

public abstract class EncryptionOptions {

    public String keystore = "conf/.keystore";
    public String keystore_password = "lealone";
    public String truststore = "conf/.truststore";
    public String truststore_password = "lealone";
    // 用这种方式设置默认值执行很慢
    // cipher_suites = ((SSLSocketFactory) SSLSocketFactory.getDefault()).getDefaultCipherSuites();
    public String[] cipher_suites;
    public String protocol = "TLS";
    public String algorithm = "SunX509";
    public String store_type = "JKS";
    public boolean require_client_auth = false;

    public static class ClientEncryptionOptions extends EncryptionOptions {
    }

    public static class ServerEncryptionOptions extends EncryptionOptions {
    }
}
