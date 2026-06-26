/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.lealone.server.http.util.net;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateEncodingException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;

import com.lealone.common.logging.Logger;
import com.lealone.server.http.util.ExceptionUtils;
import com.lealone.server.http.util.IntrospectionUtils;
import com.lealone.server.http.util.buf.HexUtils;
import com.lealone.server.http.util.compat.JreCompat;
import com.lealone.server.http.util.net.SSLHostConfigCertificate.StoreType;
import com.lealone.server.http.util.net.openssl.ciphers.Cipher;
import com.lealone.server.http.util.net.openssl.ciphers.Group;
import com.lealone.server.http.util.net.openssl.ciphers.SignatureScheme;
import com.lealone.server.http.util.res.StringManager;

/**
 * Abstract endpoint implementation.
 *
 * @param <S> The type used by the socket wrapper associated with this endpoint. Might be the same as U.
 * @param <U> The type of the underlying socket used by this endpoint. Might be the same as S.
 */
public abstract class AbstractEndpoint<S, U> {

    // -------------------------------------------------------------- Constants

    protected static final StringManager sm = StringManager.getManager(AbstractEndpoint.class);

    public interface Handler<S> {

        /**
         * Different types of socket states to react upon.
         */
        enum SocketState {
            // TODO Add a new state to the AsyncStateMachine and remove
            // ASYNC_END (if possible)
            OPEN,
            CLOSED,
            LONG,
            ASYNC_END,
            SENDFILE,
            UPGRADING,
            UPGRADED,
            ASYNC_IO,
            SUSPENDED
        }

        /**
         * Process the provided socket with the given current status.
         *
         * @param socket The socket to process
         * @param status The current socket status
         *
         * @return The state of the socket after processing
         */
        SocketState process(SocketWrapper<S> socket, SocketEvent status);

        /**
         * Obtain the GlobalRequestProcessor associated with the handler.
         *
         * @return the GlobalRequestProcessor
         */
        Object getGlobal();

        /**
         * Release any resources associated with the given SocketWrapper.
         *
         * @param socketWrapper The socketWrapper to release resources for
         */
        void release(SocketWrapper<S> socketWrapper);

        /**
         * Inform the handler that the endpoint has stopped accepting any new connections. Typically, the endpoint will
         * be stopped shortly afterwards but it is possible that the endpoint will be resumed so the handler should not
         * assume that a stop will follow.
         */
        void pause();

        /**
         * Recycle resources associated with the handler.
         */
        void recycle();
    }

    protected enum BindState {
        UNBOUND(false, false),
        BOUND_ON_INIT(true, true),
        BOUND_ON_START(true, true),
        SOCKET_CLOSED_ON_STOP(false, true);

        private final boolean bound;
        private final boolean wasBound;

        BindState(boolean bound, boolean wasBound) {
            this.bound = bound;
            this.wasBound = wasBound;
        }

        public boolean isBound() {
            return bound;
        }

        public boolean wasBound() {
            return wasBound;
        }
    }

    public static long toTimeout(long timeout) {
        // Many calls can't do infinite timeout so use Long.MAX_VALUE if timeout is <= 0
        return (timeout > 0) ? timeout : Long.MAX_VALUE;
    }

    // ----------------------------------------------------------------- Fields

    /**
     * Running state of the endpoint.
     */
    protected volatile boolean running = false;

    /**
     * Will be set to true whenever the endpoint is paused.
     */
    protected volatile boolean paused = false;

    /**
     * Are we using an internal executor
     */
    protected volatile boolean internalExecutor = true;

    /**
     * Socket properties
     */
    protected final SocketProperties socketProperties = new SocketProperties();

    public SocketProperties getSocketProperties() {
        return socketProperties;
    }

    /**
     * Map holding all current connections keyed with the sockets.
     */
    protected Map<U, SocketWrapper<S>> connections = new ConcurrentHashMap<>();

    /**
     * Get a set with the current open connections.
     *
     * @return A set with the open socket wrappers
     */
    public Set<SocketWrapper<S>> getConnections() {
        return new HashSet<>(connections.values());
    }

    private SSLImplementation sslImplementation = null;

    public SSLImplementation getSslImplementation() {
        return sslImplementation;
    }

    // ----------------------------------------------------------------- Properties

    private String sslImplementationName = null;

    public String getSslImplementationName() {
        return sslImplementationName;
    }

    public void setSslImplementationName(String s) {
        this.sslImplementationName = s;
    }

    private int sniParseLimit = 64 * 1024;

    public int getSniParseLimit() {
        return sniParseLimit;
    }

    public void setSniParseLimit(int sniParseLimit) {
        this.sniParseLimit = sniParseLimit;
    }

    private boolean strictSni = true;

    public boolean getStrictSni() {
        return strictSni;
    }

    public void setStrictSni(boolean strictSni) {
        this.strictSni = strictSni;
    }

    private String defaultSSLHostConfigName = SSLHostConfig.DEFAULT_SSL_HOST_NAME;

    /**
     * @return The host name for the default SSL configuration for this endpoint - always in lower case.
     */
    public String getDefaultSSLHostConfigName() {
        return defaultSSLHostConfigName;
    }

    public void setDefaultSSLHostConfigName(String defaultSSLHostConfigName) {
        this.defaultSSLHostConfigName = defaultSSLHostConfigName.toLowerCase(Locale.ENGLISH);
    }

    protected ConcurrentMap<String, SSLHostConfig> sslHostConfigs = new ConcurrentHashMap<>();

    /**
     * Add the given SSL Host configuration.
     *
     * @param sslHostConfig The configuration to add
     *
     * @throws IllegalArgumentException If the host name is not valid or if a configuration has already been provided
     *                                      for that host
     */
    public void addSslHostConfig(SSLHostConfig sslHostConfig) throws IllegalArgumentException {
        addSslHostConfig(sslHostConfig, false);
    }

    /**
     * Add the given SSL Host configuration, optionally replacing the existing configuration for the given host.
     *
     * @param sslHostConfig The configuration to add
     * @param replace       If {@code true} replacement of an existing configuration is permitted, otherwise any such
     *                          attempted replacement will trigger an exception
     *
     * @throws IllegalArgumentException If the host name is not valid or if a configuration has already been provided
     *                                      for that host and replacement is not allowed
     */
    public void addSslHostConfig(SSLHostConfig sslHostConfig, boolean replace)
            throws IllegalArgumentException {
        String key = sslHostConfig.getHostName();
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException(sm.getString("endpoint.noSslHostName"));
        }
        if (bindState != BindState.UNBOUND && bindState != BindState.SOCKET_CLOSED_ON_STOP
                && isSSLEnabled()) {
            try {
                createSSLContext(sslHostConfig);
            } catch (IllegalArgumentException e) {
                throw e;
            } catch (Exception e) {
                throw new IllegalArgumentException(e);
            }
        }
        if (replace) {
            sslHostConfigs.put(key, sslHostConfig);

            // Do not release any SSLContexts associated with a replaced
            // SSLHostConfig. They may still be in used by existing connections
            // and releasing them would break the connection at best. Let GC
            // handle the cleanup.
        } else {
            SSLHostConfig duplicate = sslHostConfigs.putIfAbsent(key, sslHostConfig);
            if (duplicate != null) {
                releaseSSLContext(sslHostConfig);
                throw new IllegalArgumentException(sm.getString("endpoint.duplicateSslHostName", key));
            }
        }
    }

    /**
     * Removes the SSL host configuration for the given host name, if such a configuration exists.
     *
     * @param hostName The host name associated with the SSL host configuration to remove
     *
     * @return The SSL host configuration that was removed, if any
     */
    public SSLHostConfig removeSslHostConfig(String hostName) {
        if (hostName == null) {
            return null;
        }
        // Host names are case-insensitive but stored/processed in lower case
        // internally because they are used as keys in a ConcurrentMap where
        // keys are compared in a case-sensitive manner.
        String hostNameLower = hostName.toLowerCase(Locale.ENGLISH);
        if (hostNameLower.equals(getDefaultSSLHostConfigName())) {
            throw new IllegalArgumentException(
                    sm.getString("endpoint.removeDefaultSslHostConfig", hostName));
        }
        SSLHostConfig sslHostConfig = sslHostConfigs.remove(hostNameLower);
        return sslHostConfig;
    }

    /**
     * Re-read the configuration files for the SSL host and replace the existing SSL configuration with the updated
     * settings. Note this replacement will happen even if the settings remain unchanged.
     *
     * @param hostName The SSL host for which the configuration should be reloaded. This must match a current SSL host
     */
    public void reloadSslHostConfig(String hostName) {
        // Host names are case-insensitive but stored/processed in lower case
        // internally because they are used as keys in a ConcurrentMap where
        // keys are compared in a case-sensitive manner.
        // This method can be called via various paths so convert the supplied
        // host name to lower case here to ensure the conversion occurs whatever
        // the call path.
        SSLHostConfig sslHostConfig = sslHostConfigs.get(hostName.toLowerCase(Locale.ENGLISH));
        if (sslHostConfig == null) {
            throw new IllegalArgumentException(sm.getString("endpoint.unknownSslHostName", hostName));
        }
        addSslHostConfig(sslHostConfig, true);
    }

    /**
     * Re-read the configuration files for all SSL hosts and replace the existing SSL configuration with the updated
     * settings. Note this replacement will happen even if the settings remain unchanged.
     */
    public void reloadSslHostConfigs() {
        for (String hostName : sslHostConfigs.keySet()) {
            reloadSslHostConfig(hostName);
        }
    }

    public SSLHostConfig[] findSslHostConfigs() {
        return sslHostConfigs.values().toArray(new SSLHostConfig[0]);
    }

    /**
     * Create the SSLContext for the given SSLHostConfig.
     *
     * @param sslHostConfig The SSLHostConfig for which the SSLContext should be created
     *
     * @throws IllegalArgumentException If the SSLContext cannot be created for the given SSLHostConfig
     */
    protected void createSSLContext(SSLHostConfig sslHostConfig) throws IllegalArgumentException {

        // Initialize group list
        LinkedHashSet<Group> groupList = sslHostConfig.getGroupList();
        if (groupList != null && getLog().isDebugEnabled()) {
            getLog().debug(sm.getString("endpoint.tls.enabledGroups", groupList));
        }

        boolean firstCertificate = true;
        for (SSLHostConfigCertificate certificate : sslHostConfig.getCertificates(true)) {
            SSLUtil sslUtil = sslImplementation.getSSLUtil(certificate);
            if (firstCertificate) {
                firstCertificate = false;
                sslHostConfig.setEnabledProtocols(sslUtil.getEnabledProtocols());
                sslHostConfig.setEnabledCiphers(sslUtil.getEnabledCiphers());
            }

            SSLContext sslContext = certificate.getSslContext();
            SSLContext sslContextGenerated = certificate.getSslContextGenerated();
            // Generate the SSLContext from configuration unless (e.g. embedded) an SSLContext has been provided.
            // Need to handle both initial configuration and reload.
            // Initial, SSLContext provided - sslContext will be non-null and sslContextGenerated will be null
            // Initial, SSLContext not provided - sslContext null and sslContextGenerated will be null
            // Reload, SSLContext provided - sslContext will be non-null and sslContextGenerated will be null
            // Reload, SSLContext not provided - sslContext non-null and equal to sslContextGenerated
            if (sslContext == null || sslContext == sslContextGenerated) {
                try {
                    sslContext = sslUtil.createSSLContext(negotiableProtocols);
                } catch (Exception e) {
                    throw new IllegalArgumentException(sm.getString("endpoint.errorCreatingSSLContext"),
                            e);
                }

                certificate.setSslContextGenerated(sslContext);
            }

            logCertificate(certificate);
        }

    }

    protected void logCertificate(SSLHostConfigCertificate certificate) {
        SSLHostConfig sslHostConfig = certificate.getSSLHostConfig();

        String certificateInfo;

        if (certificate.getStoreType() == StoreType.PEM) {
            // PEM file based
            certificateInfo = sm.getString("endpoint.tls.info.cert.pem",
                    certificate.getCertificateKeyFile(), certificate.getCertificateFile(),
                    certificate.getCertificateChainFile());
        } else {
            // Keystore based
            String keyAlias = certificate.getCertificateKeyAlias();
            if (keyAlias == null) {
                keyAlias = SSLUtilBase.DEFAULT_KEY_ALIAS;
            }
            String keystoreFile;
            if (certificate.getCertificateKeystoreInternal() != null) {
                // Keystore was set directly. Original location is unknown.
                keystoreFile = sm.getString("endpoint.tls.info.cert.keystore.direct");
            } else {
                keystoreFile = certificate.getCertificateKeystoreFile();
            }
            certificateInfo = sm.getString("endpoint.tls.info.cert.keystore", keystoreFile, keyAlias);
        }

        String trustStoreSource = sslHostConfig.getTruststoreFile();
        if (trustStoreSource == null) {
            trustStoreSource = sslHostConfig.getCaCertificateFile();
        }
        if (trustStoreSource == null) {
            trustStoreSource = sslHostConfig.getCaCertificatePath();
        }

        getLogCertificate().info(sm.getString("endpoint.tls.info", getName(),
                sslHostConfig.getHostName(), certificate.getType(), certificateInfo, trustStoreSource));

        if (getLogCertificate().isDebugEnabled()) {
            String alias = certificate.getCertificateKeyAlias();
            if (alias == null) {
                alias = SSLUtilBase.DEFAULT_KEY_ALIAS;
            }
            X509Certificate[] x509Certificates = certificate.getSslContext().getCertificateChain(alias);
            if (x509Certificates != null && x509Certificates.length > 0) {
                getLogCertificate().debug(generateCertificateDebug(x509Certificates[0]));
            } else {
                getLogCertificate().debug(sm.getString("endpoint.tls.cert.noCerts"));
            }
        }
    }

    protected String generateCertificateDebug(X509Certificate certificate) {
        StringBuilder sb = new StringBuilder();
        sb.append("\n[");
        try {
            byte[] certBytes = certificate.getEncoded();
            // SHA-256 fingerprint
            sb.append("\nSHA-256 fingerprint: ");
            MessageDigest sha512Digest = MessageDigest.getInstance("SHA-256");
            sha512Digest.update(certBytes);
            sb.append(HexUtils.toHexString(sha512Digest.digest()));
            // SHA-1 fingerprint
            sb.append("\nSHA-1 fingerprint: ");
            MessageDigest sha1Digest = MessageDigest.getInstance("SHA-1");
            sha1Digest.update(certBytes);
            sb.append(HexUtils.toHexString(sha1Digest.digest()));
        } catch (CertificateEncodingException e) {
            getLogCertificate().warn(sm.getString("endpoint.tls.cert.encodingError"), e);
        } catch (NoSuchAlgorithmException e) {
            // Unreachable code
            // All JREs are required to support SHA-1 and SHA-256
            throw new RuntimeException(e);
        }
        sb.append("\n");
        sb.append(certificate);
        sb.append("\n]");
        return sb.toString();
    }

    protected SSLEngine createSSLEngine(String sniHostName, List<Cipher> clientRequestedCiphers,
            List<String> clientRequestedApplicationProtocols, List<String> clientRequestedProtocols,
            List<Group> clientSupportedGroups, List<SignatureScheme> clientSignatureSchemes) {
        SSLHostConfig sslHostConfig = getSSLHostConfig(sniHostName);

        SSLHostConfigCertificate certificate = selectCertificate(sslHostConfig, clientRequestedCiphers,
                clientRequestedProtocols, clientSignatureSchemes);

        SSLContext sslContext = certificate.getSslContext();
        if (sslContext == null) {
            throw new IllegalStateException(sm.getString("endpoint.jsse.noSslContext", sniHostName));
        }

        SSLEngine engine = sslContext.createSSLEngine();
        engine.setUseClientMode(false);
        engine.setEnabledCipherSuites(sslHostConfig.getEnabledCiphers());
        engine.setEnabledProtocols(sslHostConfig.getEnabledProtocols());

        SSLParameters sslParameters = engine.getSSLParameters();
        sslParameters.setUseCipherSuitesOrder(sslHostConfig.getHonorCipherOrder());
        if (clientRequestedApplicationProtocols != null && !clientRequestedApplicationProtocols.isEmpty()
                && !negotiableProtocols.isEmpty()) {
            // Only try to negotiate if both client and server have at least
            // one protocol in common
            // Note: Tomcat does not explicitly negotiate http/1.1
            List<String> commonProtocols = new ArrayList<>(negotiableProtocols);
            commonProtocols.retainAll(clientRequestedApplicationProtocols);
            if (!commonProtocols.isEmpty()) {
                String[] commonProtocolsArray = commonProtocols.toArray(new String[0]);
                sslParameters.setApplicationProtocols(commonProtocolsArray);
            }
        }
        // Merge server groups with the client groups
        if (JreCompat.isJre20Available()) {
            List<String> supportedGroups = new ArrayList<>();
            LinkedHashSet<Group> serverSupportedGroups = sslHostConfig.getGroupList();
            if (serverSupportedGroups != null) {
                if (!clientSupportedGroups.isEmpty()) {
                    for (Group group : clientSupportedGroups) {
                        if (serverSupportedGroups.contains(group)) {
                            supportedGroups.add(group.toString());
                        }
                    }
                } else {
                    for (Group group : serverSupportedGroups) {
                        supportedGroups.add(group.toString());
                    }
                }
                JreCompat.getInstance().setNamedGroupsMethod(sslParameters,
                        supportedGroups.toArray(new String[0]));
            } else if (!clientSupportedGroups.isEmpty()) {
                for (Group group : clientSupportedGroups) {
                    supportedGroups.add(group.toString());
                }
                JreCompat.getInstance().setNamedGroupsMethod(sslParameters,
                        supportedGroups.toArray(new String[0]));
            }
        }
        switch (sslHostConfig.getCertificateVerification()) {
        case NONE:
            sslParameters.setNeedClientAuth(false);
            sslParameters.setWantClientAuth(false);
            break;
        case OPTIONAL:
        case OPTIONAL_NO_CA:
            sslParameters.setWantClientAuth(true);
            break;
        case REQUIRED:
            sslParameters.setNeedClientAuth(true);
            break;
        }
        // The getter (at least in OpenJDK and derivatives) returns a defensive copy
        engine.setSSLParameters(sslParameters);

        return engine;
    }

    private SSLHostConfigCertificate selectCertificate(SSLHostConfig sslHostConfig,
            List<Cipher> clientCiphers, List<String> clientRequestedProtocols,
            List<SignatureScheme> clientSignatureSchemes) {

        Set<SSLHostConfigCertificate> certificates = sslHostConfig.getCertificates(true);
        if (certificates.size() == 1) {
            return certificates.iterator().next();
        }

        // Use signature algorithm for cipher matching with TLS 1.3
        if ((clientRequestedProtocols.contains(Constants.SSL_PROTO_TLSv1_3))
                && sslHostConfig.getProtocols().contains(Constants.SSL_PROTO_TLSv1_3)) {
            for (SignatureScheme signatureScheme : clientSignatureSchemes) {
                for (SSLHostConfigCertificate certificate : certificates) {
                    if (certificate.getType().isCompatibleWith(signatureScheme)) {
                        return certificate;
                    }
                }
            }
        }

        LinkedHashSet<Cipher> serverCiphers = sslHostConfig.getCipherList();

        List<Cipher> candidateCiphers = new ArrayList<>();
        if (sslHostConfig.getHonorCipherOrder()) {
            candidateCiphers.addAll(serverCiphers);
            candidateCiphers.retainAll(clientCiphers);
        } else {
            candidateCiphers.addAll(clientCiphers);
            candidateCiphers.retainAll(serverCiphers);
        }

        for (Cipher candidate : candidateCiphers) {
            for (SSLHostConfigCertificate certificate : certificates) {
                if (certificate.getType().isCompatibleWith(candidate.getAu())) {
                    return certificate;
                }
            }
        }

        // No matches. Just return the first certificate. The handshake will
        // then fail due to no matching ciphers.
        return certificates.iterator().next();
    }

    protected void initialiseSsl() throws Exception {
        if (isSSLEnabled()) {
            sslImplementation = SSLImplementation.getInstance(getSslImplementationName());

            for (SSLHostConfig sslHostConfig : sslHostConfigs.values()) {
                createSSLContext(sslHostConfig);
            }

            // Validate default SSLHostConfigName
            if (sslHostConfigs.get(getDefaultSSLHostConfigName()) == null) {
                throw new IllegalArgumentException(sm.getString("endpoint.noSslHostConfig",
                        getDefaultSSLHostConfigName(), getName()));
            }

        }
    }

    protected void destroySsl() throws Exception {
        if (isSSLEnabled()) {
            for (SSLHostConfig sslHostConfig : sslHostConfigs.values()) {
                releaseSSLContext(sslHostConfig);
            }
        }
    }

    /**
     * Release the SSLContext, if any, associated with the SSLHostConfig.
     *
     * @param sslHostConfig The SSLHostConfig for which the SSLContext should be released
     */
    protected void releaseSSLContext(SSLHostConfig sslHostConfig) {
        for (SSLHostConfigCertificate certificate : sslHostConfig.getCertificates()) {
            if (certificate.getSslContext() != null) {
                // Only release the SSLContext if we generated it.
                SSLContext sslContext = certificate.getSslContextGenerated();
                if (sslContext != null) {
                    sslContext.destroy();
                }
            }
        }
    }

    /**
     * Look up the SSLHostConfig for the given host name. Lookup order is:
     * <ol>
     * <li>exact match</li>
     * <li>wild card match</li>
     * <li>default SSLHostConfig</li>
     * </ol>
     *
     * @param sniHostName Host name - must be in lower case
     *
     * @return The SSLHostConfig for the given host name.
     */
    protected SSLHostConfig getSSLHostConfig(String sniHostName) {
        SSLHostConfig result = null;

        if (sniHostName != null) {
            // First choice - direct match
            result = sslHostConfigs.get(sniHostName);
            if (result != null) {
                return result;
            }
            // Second choice, wildcard match
            int indexOfDot = sniHostName.indexOf('.');
            if (indexOfDot > -1) {
                result = sslHostConfigs.get("*" + sniHostName.substring(indexOfDot));
            }
        }

        // Fall-back. Use the default
        if (result == null) {
            result = sslHostConfigs.get(getDefaultSSLHostConfigName());
        }
        if (result == null) {
            // Should never happen.
            throw new IllegalStateException();
        }
        return result;
    }

    /**
     * Check if two host names share the same SSLHostConfig.
     *
     * @param sniHostName the host name from SNI, null if SNI is not in use
     * @param protocolHostName the host name from the protocol
     * @return true if SNI is not checked, if the SNI host name matches the protocol host name,
     *    if both host names use the same SSLHostConfig configuration, if there is no SNI and the
     *    protocol host name uses the default SSLHostConfig configuration, and false otherwise
     */
    public boolean checkSni(String sniHostName, String protocolHostName) {
        return (!strictSni || !isSSLEnabled()
                || (sniHostName != null && sniHostName.equalsIgnoreCase(protocolHostName))
                || getSSLHostConfig(sniHostName) == getSSLHostConfig(
                        protocolHostName != null ? protocolHostName.toLowerCase(Locale.ENGLISH) : null));
    }

    /**
     * Has the user requested that send file be used where possible?
     */
    private boolean useSendfile = true;

    public boolean getUseSendfile() {
        return useSendfile;
    }

    public void setUseSendfile(boolean useSendfile) {
        this.useSendfile = useSendfile;
    }

    /**
     * Time to wait for the internal executor (if used) to terminate when the endpoint is stopped in milliseconds.
     * Defaults to 5000 (5 seconds).
     */
    private long executorTerminationTimeoutMillis = 5000;

    public long getExecutorTerminationTimeoutMillis() {
        return executorTerminationTimeoutMillis;
    }

    public void setExecutorTerminationTimeoutMillis(long executorTerminationTimeoutMillis) {
        this.executorTerminationTimeoutMillis = executorTerminationTimeoutMillis;
    }

    /**
     * Priority of the acceptor threads.
     */
    protected int acceptorThreadPriority = Thread.NORM_PRIORITY;

    public void setAcceptorThreadPriority(int acceptorThreadPriority) {
        this.acceptorThreadPriority = acceptorThreadPriority;
    }

    public int getAcceptorThreadPriority() {
        return acceptorThreadPriority;
    }

    private int maxConnections = 8 * 1024;

    public void setMaxConnections(int maxCon) {
        this.maxConnections = maxCon;
    }

    public int getMaxConnections() {
        return this.maxConnections;
    }

    /**
     * Server socket port.
     */
    private int port = -1;

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    private int portOffset = 0;

    public int getPortOffset() {
        return portOffset;
    }

    public void setPortOffset(int portOffset) {
        if (portOffset < 0) {
            throw new IllegalArgumentException(
                    sm.getString("endpoint.portOffset.invalid", Integer.valueOf(portOffset)));
        }
        this.portOffset = portOffset;
    }

    public int getPortWithOffset() {
        // Zero is a special case and negative values are invalid
        int port = getPort();
        if (port > 0) {
            return port + getPortOffset();
        }
        return port;
    }

    public final int getLocalPort() {
        try {
            InetSocketAddress localAddress = getLocalAddress();
            if (localAddress == null) {
                return -1;
            }
            return localAddress.getPort();
        } catch (IOException ioe) {
            return -1;
        }
    }

    /**
     * Address for the server socket.
     */
    private InetAddress address;

    public InetAddress getAddress() {
        return address;
    }

    public void setAddress(InetAddress address) {
        this.address = address;
    }

    /**
     * Obtain the network address the server socket is bound to. This primarily exists to enable the correct address to
     * be used when unlocking the server socket since it removes the guess-work involved if no address is specifically
     * set.
     *
     * @return The network address that the server socket is listening on or null if the server socket is not currently
     *             bound.
     *
     * @throws IOException If there is a problem determining the currently bound socket
     */
    protected abstract InetSocketAddress getLocalAddress() throws IOException;

    /**
     * Allows the server developer to specify the acceptCount (backlog) that should be used for server sockets. By
     * default, this value is 100.
     */
    private int acceptCount = 100;

    public void setAcceptCount(int acceptCount) {
        if (acceptCount > 0) {
            this.acceptCount = acceptCount;
        }
    }

    public int getAcceptCount() {
        return acceptCount;
    }

    /**
     * Controls when the Endpoint binds the port. <code>true</code>, the default binds the port on {@link #init()} and
     * unbinds it on {@link #destroy()}. If set to <code>false</code> the port is bound on {@link #start()} and unbound
     * on {@link #stop()}.
     */
    private boolean bindOnInit = true;

    public boolean getBindOnInit() {
        return bindOnInit;
    }

    public void setBindOnInit(boolean b) {
        this.bindOnInit = b;
    }

    private volatile BindState bindState = BindState.UNBOUND;

    protected BindState getBindState() {
        return bindState;
    }

    /**
     * Keepalive timeout, if not set the soTimeout is used.
     */
    private Integer keepAliveTimeout = null;

    public int getKeepAliveTimeout() {
        if (keepAliveTimeout == null) {
            return getConnectionTimeout();
        } else {
            return keepAliveTimeout.intValue();
        }
    }

    public void setKeepAliveTimeout(int keepAliveTimeout) {
        this.keepAliveTimeout = Integer.valueOf(keepAliveTimeout);
    }

    /**
     * Socket TCP no delay.
     *
     * @return The current TCP no delay setting for sockets created by this endpoint
     */
    public boolean getTcpNoDelay() {
        return socketProperties.getTcpNoDelay();
    }

    public void setTcpNoDelay(boolean tcpNoDelay) {
        socketProperties.setTcpNoDelay(tcpNoDelay);
    }

    /**
     * Socket linger.
     *
     * @return The current socket linger time for sockets created by this endpoint
     */
    public int getConnectionLinger() {
        return socketProperties.getSoLingerTime();
    }

    public void setConnectionLinger(int connectionLinger) {
        socketProperties.setSoLingerTime(connectionLinger);
        socketProperties.setSoLingerOn(connectionLinger >= 0);
    }

    /**
     * Socket timeout.
     *
     * @return The current socket timeout for sockets created by this endpoint
     */
    public int getConnectionTimeout() {
        return socketProperties.getSoTimeout();
    }

    public void setConnectionTimeout(int soTimeout) {
        socketProperties.setSoTimeout(soTimeout);
    }

    /**
     * SSL engine.
     */
    private boolean SSLEnabled = false;

    public boolean isSSLEnabled() {
        return SSLEnabled;
    }

    public void setSSLEnabled(boolean SSLEnabled) {
        this.SSLEnabled = SSLEnabled;
    }

    /**
     * Max keep alive requests
     */
    private int maxKeepAliveRequests = 100; // as in Apache HTTPD server

    public int getMaxKeepAliveRequests() {
        // Disable keep-alive if the server socket is not bound
        if (bindState.isBound()) {
            return maxKeepAliveRequests;
        } else {
            return 1;
        }
    }

    public void setMaxKeepAliveRequests(int maxKeepAliveRequests) {
        this.maxKeepAliveRequests = maxKeepAliveRequests;
    }

    /**
     * Name of the thread pool, which will be used for naming child threads.
     */
    private String name = "TP";

    public void setName(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    /**
     * The default is true - the created threads will be in daemon mode. If set to false, the control thread will not be
     * daemon - and will keep the process alive.
     */
    private boolean daemon = true;

    public void setDaemon(boolean b) {
        daemon = b;
    }

    public boolean getDaemon() {
        return daemon;
    }

    /**
     * Expose asynchronous IO capability.
     */
    private boolean useAsyncIO = true;

    public void setUseAsyncIO(boolean useAsyncIO) {
        this.useAsyncIO = useAsyncIO;
    }

    public boolean getUseAsyncIO() {
        return useAsyncIO;
    }

    /**
     * The default behavior is to identify connectors uniquely with address and port. However, certain connectors are
     * not using that and need some other identifier, which then can be used as a replacement.
     *
     * @return the id
     */
    public String getId() {
        return null;
    }

    protected final List<String> negotiableProtocols = new ArrayList<>();

    public void addNegotiatedProtocol(String negotiableProtocol) {
        negotiableProtocols.add(negotiableProtocol);
    }

    public boolean hasNegotiableProtocols() {
        return (!negotiableProtocols.isEmpty());
    }

    /**
     * Handling of accepted sockets.
     */
    private Handler<S> handler = null;

    public void setHandler(Handler<S> handler) {
        this.handler = handler;
    }

    public Handler<S> getHandler() {
        return handler;
    }

    /**
     * Attributes provide a way for configuration to be passed to subcomponents without the
     * {@link com.lealone.server.http.protocol.ProtocolHandler} being aware of the properties available on those subcomponents.
     */
    protected HashMap<String, Object> attributes = new HashMap<>();

    /**
     * Generic property setter called when a property for which a specific setter already exists within the
     * {@link com.lealone.server.http.protocol.ProtocolHandler} needs to be made available to subcomponents. The specific setter will
     * call this method to populate the attributes.
     *
     * @param name  Name of property to set
     * @param value The value to set the property to
     */
    public void setAttribute(String name, Object value) {
        if (getLog().isTraceEnabled()) {
            getLog().trace(sm.getString("endpoint.setAttribute", name, value));
        }
        attributes.put(name, value);
    }

    /**
     * Used by subcomponents to retrieve configuration information.
     *
     * @param key The name of the property for which the value should be retrieved
     *
     * @return The value of the specified property
     */
    public Object getAttribute(String key) {
        Object value = attributes.get(key);
        if (getLog().isTraceEnabled()) {
            getLog().trace(sm.getString("endpoint.getAttribute", key, value));
        }
        return value;
    }

    public boolean setProperty(String name, String value) {
        setAttribute(name, value);
        final String socketName = "socket.";
        try {
            if (name.startsWith(socketName)) {
                return IntrospectionUtils.setProperty(socketProperties,
                        name.substring(socketName.length()), value);
            } else {
                return IntrospectionUtils.setProperty(this, name, value, false);
            }
        } catch (Exception e) {
            getLog().error(sm.getString("endpoint.setAttributeError", name, value), e);
            return false;
        }
    }

    public String getProperty(String name) {
        String value = (String) getAttribute(name);
        final String socketName = "socket.";
        if (value == null && name.startsWith(socketName)) {
            Object result = IntrospectionUtils.getProperty(socketProperties,
                    name.substring(socketName.length()));
            if (result != null) {
                value = result.toString();
            }
        }
        return value;
    }

    public boolean isRunning() {
        return running;
    }

    public boolean isPaused() {
        return paused;
    }

    // ------------------------------------------------------- Lifecycle methods

    /*
     * NOTE: There is no maintenance of state or checking for valid transitions within this class other than ensuring
     * that bind/unbind are called in the right place. It is expected that the calling code will maintain state and
     * prevent invalid state transitions.
     */

    public abstract void bind() throws Exception;

    public void unbind() throws Exception {
        for (SSLHostConfig sslHostConfig : sslHostConfigs.values()) {
            for (SSLHostConfigCertificate certificate : sslHostConfig.getCertificates()) {
                /*
                 * Only remove any generated SSLContext. If the SSLContext was provided it is left in place in case the
                 * endpoint is re-started.
                 */
                certificate.setSslContextGenerated(null);
            }
        }
    }

    public abstract void startInternal() throws Exception;

    public abstract void stopInternal() throws Exception;

    private void bindWithCleanup() throws Exception {
        try {
            bind();
        } catch (Throwable t) {
            // Ensure open sockets etc. are cleaned up if something goes
            // wrong during bind
            ExceptionUtils.handleThrowable(t);
            unbind();
            throw t;
        }
    }

    public final void init() throws Exception {
        if (bindOnInit) {
            bindWithCleanup();
            bindState = BindState.BOUND_ON_INIT;
        }
    }

    public final void start() throws Exception {
        if (bindState == BindState.UNBOUND) {
            bindWithCleanup();
            bindState = BindState.BOUND_ON_START;
        }
        startInternal();
    }

    /**
     * Pause the endpoint, which will stop it accepting new connections and unlock the acceptor.
     */
    public void pause() {
        if (running && !paused) {
            paused = true;
            getHandler().pause();
        }
    }

    /**
     * Resume the endpoint, which will make it start accepting new connections again.
     */
    public void resume() {
        if (running) {
            paused = false;
        }
    }

    public final void stop() throws Exception {
        stopInternal();
        if (bindState == BindState.BOUND_ON_START || bindState == BindState.SOCKET_CLOSED_ON_STOP) {
            unbind();
            bindState = BindState.UNBOUND;
        }
    }

    public final void destroy() throws Exception {
        if (bindState == BindState.BOUND_ON_INIT) {
            unbind();
            bindState = BindState.UNBOUND;
        }
    }

    protected abstract Logger getLog();

    protected Logger getLogCertificate() {
        return getLog();
    }

    /**
     * Close the server socket (to prevent further connections) if the server socket was originally bound on
     * {@link #start()} (rather than on {@link #init()}).
     *
     * @see #getBindOnInit()
     */
    public final void closeServerSocketGraceful() {
        if (bindState == BindState.BOUND_ON_START) {
            // Signal to any multiplexed protocols (HTTP/2) that they may wish
            // to stop accepting new streams
            getHandler().pause();
            // Update the bindState. This has the side effect of disabling
            // keep-alive for any in-progress connections
            bindState = BindState.SOCKET_CLOSED_ON_STOP;
            try {
                doCloseServerSocket();
            } catch (IOException ioe) {
                getLog().warn(sm.getString("endpoint.serverSocket.closeFailed", getName()), ioe);
            }
        }
    }

    /**
     * Wait for the client connections to the server to close gracefully. The method will return when all of the client
     * connections have closed or the method has been waiting for {@code waitTimeMillis}.
     *
     * @param waitMillis The maximum time to wait in milliseconds for the client connections to close.
     *
     * @return The wait time, if any remaining when the method returned
     */
    public final long awaitConnectionsClose(long waitMillis) {
        while (waitMillis > 0 && !connections.isEmpty()) {
            try {
                Thread.sleep(50);
                waitMillis -= 50;
            } catch (InterruptedException e) {
                Thread.interrupted();
                waitMillis = 0;
            }
        }
        return waitMillis;
    }

    /**
     * Actually close the server socket but don't perform any other clean-up.
     *
     * @throws IOException If an error occurs closing the socket
     */
    protected abstract void doCloseServerSocket() throws IOException;

    protected abstract U serverSocketAccept() throws Exception;

    protected abstract boolean setSocketOptions(U socket);

    /**
     * Close the socket when the connection has to be immediately closed when an error occurs while configuring the
     * accepted socket or trying to dispatch it for processing. The wrapper associated with the socket will be used for
     * the close.
     *
     * @param socket The newly accepted socket
     */
    protected void closeSocket(U socket) {
        SocketWrapper<S> socketWrapper = connections.get(socket);
        if (socketWrapper != null) {
            socketWrapper.close();
        }
    }

    /**
     * Close the socket. This is used when the connector is not in a state which allows processing the socket, or if
     * there was an error which prevented the allocation of the socket wrapper.
     *
     * @param socket The newly accepted socket
     */
    protected abstract void destroySocket(U socket);
}
