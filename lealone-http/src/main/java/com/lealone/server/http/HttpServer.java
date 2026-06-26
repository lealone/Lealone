/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.server.http;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.security.Principal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.lealone.common.exceptions.ConfigException;
import com.lealone.common.logging.Logger;
import com.lealone.common.logging.LoggerFactory;
import com.lealone.common.util.CaseInsensitiveMap;
import com.lealone.common.util.MapUtils;
import com.lealone.db.ConnectionInfo;
import com.lealone.db.scheduler.Scheduler;
import com.lealone.net.WritableChannel;
import com.lealone.server.AsyncServer;
import com.lealone.server.http.container.Container;
import com.lealone.server.http.container.Context;
import com.lealone.server.http.container.Engine;
import com.lealone.server.http.container.Host;
import com.lealone.server.http.container.Lifecycle;
import com.lealone.server.http.container.LifecycleEvent;
import com.lealone.server.http.container.LifecycleException;
import com.lealone.server.http.container.LifecycleListener;
import com.lealone.server.http.container.Realm;
import com.lealone.server.http.container.Server;
import com.lealone.server.http.container.Service;
import com.lealone.server.http.container.Wrapper;
import com.lealone.server.http.container.authenticator.NonLoginAuthenticator;
import com.lealone.server.http.container.connector.Connector;
import com.lealone.server.http.container.connector.CoyoteAdapter;
import com.lealone.server.http.container.core.StandardContext;
import com.lealone.server.http.container.core.StandardEngine;
import com.lealone.server.http.container.core.StandardHost;
import com.lealone.server.http.container.core.StandardServer;
import com.lealone.server.http.container.core.StandardService;
import com.lealone.server.http.container.core.StandardWrapper;
import com.lealone.server.http.container.realm.GenericPrincipal;
import com.lealone.server.http.container.realm.RealmBase;
import com.lealone.server.http.protocol.http11.Http11NioProtocol;
import com.lealone.server.http.protocol.http2.Http2Protocol;
import com.lealone.server.http.util.ExceptionUtils;
import com.lealone.server.http.util.buf.UriUtil;
import com.lealone.server.http.util.descriptor.LoginConfig;
import com.lealone.server.http.util.file.ConfigFileLoader;
import com.lealone.server.http.util.file.ConfigurationSource;
import com.lealone.server.http.util.net.SSLHostConfig;
import com.lealone.server.http.util.net.SSLHostConfigCertificate;
import com.lealone.server.http.util.res.StringManager;
import com.lealone.server.servlet.Servlet;
import com.lealone.server.servlet.ServletException;
import com.lealone.server.servlet.annotation.WebServlet;

public class HttpServer extends AsyncServer<HttpServerConnection> {

    private static final Logger logger = LoggerFactory.getLogger(HttpServer.class);
    private static final StringManager sm = StringManager.getManager(HttpServer.class);

    private final Map<String, String> userPass = new HashMap<>();
    private final Map<String, List<String>> userRoles = new HashMap<>();
    private final Map<String, Principal> userPrincipals = new HashMap<>();

    private Map<String, String> config = new HashMap<>();
    private String webRoot;
    private String jdbcUrl;

    private String contextPath;
    private Context ctx;
    private Server server;
    private Http11NioProtocol protocolHandler;

    private boolean inited;

    public HttpServer() {
        ExceptionUtils.preload();
    }

    public Context getContext() {
        return ctx;
    }

    public Http11NioProtocol getProtocolHandler() {
        return protocolHandler;
    }

    @Override
    public String getType() {
        return HttpServerEngine.NAME;
    }

    @Override
    public String getName() {
        return HttpServer.class.getSimpleName();
    }

    public String getWebRoot() {
        return webRoot;
    }

    public void setWebRoot(String webRoot) {
        this.webRoot = webRoot;
        config.put("web_root", webRoot);
    }

    public String getJdbcUrl() {
        return jdbcUrl;
    }

    public void setJdbcUrl(String jdbcUrl) {
        this.jdbcUrl = jdbcUrl;
        config.put("jdbc_url", jdbcUrl);
        System.setProperty(com.lealone.db.Constants.JDBC_URL_KEY, jdbcUrl);
    }

    public void setHost(String host) {
        config.put("host", host);
    }

    public void setPort(int port) {
        config.put("port", String.valueOf(port));
    }

    @Override
    public synchronized void init(Map<String, String> config) {
        if (inited)
            return;
        config = new CaseInsensitiveMap<>(config);
        config.putAll(this.config);
        String url = config.get("jdbc_url");
        if (url != null) {
            if (this.jdbcUrl == null)
                setJdbcUrl(url);
            ConnectionInfo ci = new ConnectionInfo(url);
            if (!config.containsKey("default_database"))
                config.put("default_database", ci.getDatabaseName());
            if (!config.containsKey("default_schema"))
                config.put("default_schema", "public");
        }
        super.init(config);
        contextPath = MapUtils.getString(config, "context_path", "");
        webRoot = MapUtils.getString(config, "web_root", "./web");
        File webRootDir = new File(webRoot);
        // 如果没有指定web_root参数也没有web目录就把docBase变成work目录
        if (!webRootDir.exists()) {
            webRootDir = new File(getBaseDir(), "work");
            if (!webRootDir.exists())
                webRootDir.mkdirs();
            webRoot = webRootDir.getAbsolutePath();
        }
        try {
            File baseFile = new File(getBaseDir()).getCanonicalFile();
            // Set configuration source
            ConfigFileLoader.setSource(new BaseConfigurationSource(baseFile));
            getServer().setBaseDir(baseFile);
            ctx = addContext(contextPath, webRootDir.getCanonicalPath());
            ((StandardServer) getServer()).setPeriodicEventDelay(0);
            getEngine().setBackgroundProcessorDelay(0);

            protocolHandler = new Http11NioProtocol();
            protocolHandler.setPort(getPort());
            protocolHandler.setMaxKeepAliveRequests(100 * 10000);
            Connector connector;
            if (MapUtils.getBoolean(config, "ssl", false)) {
                connector = createSSLConnector(config);
            } else {
                connector = new Connector(protocolHandler);
                upgradeHttp2(config);
            }
            CoyoteAdapter adapter = new CoyoteAdapter(connector);
            protocolHandler.setAdapter(adapter);
            setConnector(connector);

            HttpRouter router;
            String routerStr = config.get("router");
            if (routerStr != null) {
                try {
                    router = com.lealone.common.util.Utils.newInstance(routerStr);
                } catch (Exception e) {
                    throw new ConfigException("Failed to load router: " + routerStr, e);
                }
            } else {
                router = new HttpRouter();
            }
            router.init(this, config);
            getServer().init();
        } catch (Exception e) {
            logger.error("Failed to init http server", e);
        }

        inited = true;
        this.config = null;
    }

    private void upgradeHttp2(Map<String, String> c) {
        if (MapUtils.getBoolean(c, "upgrade_http2", false)) {
            Http2Protocol h2 = new Http2Protocol();
            h2.setOverheadCountFactor(10000000);
            protocolHandler.addUpgradeProtocol(h2);
        }
    }

    private Connector createSSLConnector(Map<String, String> c) {
        Connector connector = new Connector(protocolHandler);
        connector.setPort(8443);
        connector.setService(getService());
        upgradeHttp2(c);
        protocolHandler.setMaxKeepAliveRequests(100 * 10000);

        protocolHandler.setSSLEnabled(true); // 启用SSL
        SSLHostConfig config = new SSLHostConfig();
        SSLHostConfigCertificate certificate = new SSLHostConfigCertificate(config,
                SSLHostConfigCertificate.Type.RSA);
        certificate.setCertificateKeystoreFile(
                new File("src/test/resources/http/localhost-rsa.jks").getAbsolutePath());
        certificate.setCertificateKeyAlias("lealone-localhost");
        certificate.setCertificateKeystorePassword("123456");
        config.addCertificate(certificate);
        protocolHandler.addSslHostConfig(config);
        return connector;
    }

    @Override
    public synchronized void start() {
        if (isStarted())
            return;
        try {
            if (!inited) {
                init(new HashMap<>());
            }
            super.start();
            getServer().start();
        } catch (Exception e) {
            logger.error("Failed to start http server", e);
        }
    }

    @Override
    public synchronized void stop() {
        if (isStopped())
            return;
        try {
            super.stop();
            getServer();
            server.stop();
            server.destroy();
        } catch (LifecycleException e) {
            logger.error("Failed to stop http server", e);
        }
        // executorService.shutdown();
    }

    @Override
    protected int getDefaultPort() {
        return HttpServerEngine.DEFAULT_PORT;
    }

    @Override
    protected HttpServerConnection createConnection(WritableChannel channel, Scheduler scheduler) {
        return new HttpServerConnection(this, channel, scheduler);
    }

    public void addServlet(String servletName, Servlet servlet) {
        addServlet(ctx, servletName, servlet);
    }

    public void addServletMappingDecoded(String pattern, String name) {
        ctx.addServletMappingDecoded(pattern, name);
    }

    // private ExecutorService executorService = Executors
    // .newThreadPerTaskExecutor(Thread.ofVirtual().name("lealone-http-vt-", 1).factory());

    public void submit(Runnable task) {
        // executorService.submit(task);
        task.run();
    }

    /**
     * Add a context - programmatic mode, no default web.xml used. This means that there is no JSP support (no JSP
     * servlet), no default servlet and no web socket support unless explicitly enabled via the programmatic interface.
     * There is also no {@link com.lealone.server.servlet.ServletContainerInitializer} processing and no annotation processing. If
     * a {@link com.lealone.server.servlet.ServletContainerInitializer} is added programmatically, there will still be no scanning
     * for {@link com.lealone.server.servlet.annotation.HandlesTypes} matches.
     * <p>
     * API calls equivalent with web.xml:
     *
     * <pre>{@code
     * // context-param
     * ctx.addParameter("name", "value");
     *
     *
     * // error-page
     * ErrorPage ep = new ErrorPage();
     * ep.setErrorCode(500);
     * ep.setLocation("/error.html");
     * ctx.addErrorPage(ep);
     *
     * ctx.addMimeMapping("ext", "type");
     * }</pre>
     * <p>
     * Note: If you reload the Context, all your configuration will be lost. If you need reload support, consider using
     * a LifecycleListener to provide your configuration.
     * <p>
     *
     * @param contextPath The context mapping to use, "" for root context.
     * @param docBase     Base directory for the context, for static files. Must exist, relative to the server home
     *
     * @return the deployed context
     */
    public Context addContext(String contextPath, String docBase) {
        return addContext(getHostContainer(), contextPath, docBase);
    }

    /**
     * @param host        The host in which the context will be deployed
     * @param contextPath The context mapping to use, "" for root context.
     * @param dir         Base directory for the context, for static files. Must exist, relative to the server home
     *
     * @return the deployed context
     *
     * @see #addContext(String, String)
     */
    public Context addContext(Host host, String contextPath, String dir) {
        return addContext(host, contextPath, contextPath, dir);
    }

    /**
     * @param host        The host in which the context will be deployed
     * @param contextPath The context mapping to use, "" for root context.
     * @param contextName The context name
     * @param dir         Base directory for the context, for static files. Must exist, relative to the server home
     *
     * @return the deployed context
     *
     * @see #addContext(String, String)
     */
    public Context addContext(Host host, String contextPath, String contextName, String dir) {
        Context ctx = createContext(host, contextPath);
        ctx.setName(contextName);
        ctx.setPath(contextPath);
        ctx.setDocBase(dir);
        ctx.addLifecycleListener(new FixContextListener());

        if (host == null) {
            getHostContainer().addChild(ctx);
        } else {
            host.addChild(ctx);
        }
        return ctx;
    }

    /**
     * Create the configured {@link Context} for the given <code>host</code>. The default constructor of the class that
     * was configured with {@link StandardHost#setContextClass(String)} will be used
     *
     * @param host host for which the {@link Context} should be created, or <code>null</code> if default host should be
     *                 used
     * @param url  path of the webapp which should get the {@link Context}
     *
     * @return newly created {@link Context}
     */
    private Context createContext(Host host, String url) {
        String defaultContextClass = StandardContext.class.getName();
        String contextClass = StandardContext.class.getName();
        if (host == null) {
            host = getHostContainer();
        }
        if (host instanceof StandardHost) {
            contextClass = ((StandardHost) host).getContextClass();
        }
        try {
            if (defaultContextClass.equals(contextClass)) {
                return new StandardContext();
            } else {
                return (Context) Class.forName(contextClass).getConstructor().newInstance();
            }

        } catch (ReflectiveOperationException | IllegalArgumentException | SecurityException e) {
            throw new IllegalArgumentException(
                    sm.getString("http.noContextClass", contextClass, host, url), e);
        }
    }

    /**
     * Equivalent to &lt;servlet&gt;&lt;servlet-name&gt;&lt;servlet-class&gt;.
     * <p>
     * In general it is better/faster to use the method that takes a Servlet as param - this one can be used if the
     * servlet is not commonly used, and want to avoid loading all deps. ( for example: jsp servlet ) You can customize
     * the returned servlet, ex:
     *
     * <pre>
     * wrapper.addInitParameter("name", "value");
     * </pre>
     *
     * @param contextPath  Context to add Servlet to
     * @param servletName  Servlet name (used in mappings)
     * @param servletClass The class to be used for the Servlet
     *
     * @return The wrapper for the servlet
     */
    public Wrapper addServlet(String contextPath, String servletName, String servletClass) {
        Container ctx = getHostContainer().findChild(contextPath);
        return addServlet((Context) ctx, servletName, servletClass);
    }

    /**
     * Static version of {@link #addServlet(String, String, String)}
     *
     * @param ctx          Context to add Servlet to
     * @param servletName  Servlet name (used in mappings)
     * @param servletClass The class to be used for the Servlet
     *
     * @return The wrapper for the servlet
     */
    public static Wrapper addServlet(Context ctx, String servletName, String servletClass) {
        // will do class for name and set init params
        Wrapper sw = ctx.createWrapper();
        if (sw == null) {
            throw new IllegalStateException(sm.getString("http.noWrapper"));
        }
        sw.setServletClass(servletClass);
        sw.setName(servletName);
        ctx.addChild(sw);

        return sw;
    }

    /**
     * Add an existing Servlet to the context with no class.forName or initialisation.
     *
     * @param contextPath Context to add Servlet to
     * @param servletName Servlet name (used in mappings)
     * @param servlet     The Servlet to add
     *
     * @return The wrapper for the servlet
     */
    public Wrapper addServlet(String contextPath, String servletName, Servlet servlet) {
        Container ctx = getHostContainer().findChild(contextPath);
        return addServlet((Context) ctx, servletName, servlet);
    }

    /**
     * Static version of {@link #addServlet(String, String, Servlet)}.
     *
     * @param ctx         Context to add Servlet to
     * @param servletName Servlet name (used in mappings)
     * @param servlet     The Servlet to add
     *
     * @return The wrapper for the servlet
     */
    public static Wrapper addServlet(Context ctx, String servletName, Servlet servlet) {
        // will do class for name and set init params
        Wrapper sw = new ExistingStandardWrapper(servlet);
        sw.setName(servletName);
        ctx.addChild(sw);
        sw.setAsyncSupported(true);

        return sw;
    }

    /**
     * Add a user for the in-memory realm. All created apps use this by default, can be replaced using setRealm().
     *
     * @param user The username
     * @param pass The password
     */
    public void addUser(String user, String pass) {
        userPass.put(user, pass);
    }

    /**
     * Add a role to a user.
     *
     * @see #addUser(String, String)
     *
     * @param user The username
     * @param role The role name
     */
    public void addRole(String user, String role) {
        userRoles.computeIfAbsent(user, k -> new ArrayList<>()).add(role);
    }

    /**
     * Set the specified connector in the service, if it is not already present.
     *
     * @param connector The connector instance to add
     */
    public void setConnector(Connector connector) {
        Service service = getService();
        boolean found = false;
        for (Connector serviceConnector : service.findConnectors()) {
            if (connector == serviceConnector) {
                found = true;
                break;
            }
        }
        if (!found) {
            service.addConnector(connector);
        }
    }

    /**
     * Get the service object. Can be used to add more connectors and few other global settings.
     *
     * @return The service
     */
    public Service getService() {
        return getServer().findServices()[0];
    }

    public void setHostContainer(Host host) {
        Engine engine = getEngine();
        boolean found = false;
        for (Container engineHost : engine.findChildren()) {
            if (engineHost == host) {
                found = true;
                break;
            }
        }
        if (!found) {
            engine.addChild(host);
        }
    }

    public Host getHostContainer() {
        Engine engine = getEngine();
        if (engine.findChildren().length > 0) {
            return (Host) engine.findChildren()[0];
        }

        Host host = new StandardHost();
        host.setName(getHost());
        getEngine().addChild(host);
        return host;
    }

    /**
     * Access to the engine, for further customization.
     *
     * @return The engine
     */
    public Engine getEngine() {
        Service service = getServer().findServices()[0];
        if (service.getContainer() != null) {
            return service.getContainer();
        }
        Engine engine = new StandardEngine();
        engine.setName(getName());
        engine.setDefaultHost(getHost());
        engine.setRealm(createDefaultRealm());
        service.setContainer(engine);
        return engine;
    }

    /**
     * Get the server object. You can add listeners and few more customizations. JNDI is disabled by default.
     *
     * @return The Server
     */
    public Server getServer() {
        if (server != null) {
            return server;
        }
        server = new StandardServer();
        Service service = new StandardService();
        service.setName(getName());
        server.addService(service);
        return server;
    }

    // ---------- Helper methods and classes -------------------

    private Realm createDefaultRealm() {
        return new SimpleRealm();
    }

    private class SimpleRealm extends RealmBase {

        @Override
        protected String getPassword(String username) {
            return userPass.get(username);
        }

        @Override
        protected Principal getPrincipal(String username) {
            Principal p = userPrincipals.get(username);
            if (p == null) {
                String pass = userPass.get(username);
                if (pass != null) {
                    p = new GenericPrincipal(username, userRoles.get(username));
                    userPrincipals.put(username, p);
                }
            }
            return p;
        }
    }

    /**
     * Add the default MIME type mappings to the provided Context.
     *
     * @param context The web application to which the default MIME type mappings should be added.
     */
    public static void addDefaultMimeTypeMappings(Context context) {
        Properties defaultMimeMappings = new Properties();
        try (InputStream is = HttpServer.class.getResourceAsStream("MimeTypeMappings.properties")) {
            defaultMimeMappings.load(is);
            for (Map.Entry<Object, Object> entry : defaultMimeMappings.entrySet()) {
                context.addMimeMapping((String) entry.getKey(), (String) entry.getValue());
            }
        } catch (IOException ioe) {
            throw new IllegalStateException(sm.getString("http.defaultMimeTypeMappingsFail"), ioe);
        }
    }

    /**
     * Fix startup sequence - required if you don't use web.xml.
     * <p>
     * The start() method in context will set 'configured' to false - and expects a listener to set it back to true.
     */
    private static class FixContextListener implements LifecycleListener {

        @Override
        public void lifecycleEvent(LifecycleEvent event) {
            try {
                Context context = (Context) event.getLifecycle();
                if (event.getType().equals(Lifecycle.CONFIGURE_START_EVENT)) {
                    context.setConfigured(true);

                    // LoginConfig is required to process @ServletSecurity
                    // annotations
                    if (context.getLoginConfig() == null) {
                        context.setLoginConfig(new LoginConfig("NONE", null, null, null));
                        context.getPipeline().addValve(new NonLoginAuthenticator());
                    }
                }
            } catch (ClassCastException e) {
                // Ignore
            }
        }
    }

    /**
     * Helper class for wrapping existing servlets. This disables servlet lifecycle and normal reloading, but also
     * reduces overhead and provide more direct control over the servlet.
     */
    private static class ExistingStandardWrapper extends StandardWrapper {
        private final Servlet existing;

        public ExistingStandardWrapper(Servlet existing) {
            this.existing = existing;
            this.asyncSupported = hasAsync(existing);
        }

        private static boolean hasAsync(Servlet existing) {
            boolean result = false;
            Class<?> clazz = existing.getClass();
            WebServlet ws = clazz.getAnnotation(WebServlet.class);
            if (ws != null) {
                result = ws.asyncSupported();
            }
            return result;
        }

        @Override
        public synchronized Servlet loadServlet() throws ServletException {
            if (!instanceInitialized) {
                existing.init(facade);
                instanceInitialized = true;
            }
            return existing;
        }

        @Override
        public long getAvailable() {
            return 0;
        }

        @Override
        public boolean isUnavailable() {
            return false;
        }

        @Override
        public Servlet getServlet() {
            return existing;
        }

        @Override
        public String getServletClass() {
            return existing.getClass().getName();
        }
    }

    private static final class BaseConfigurationSource implements ConfigurationSource {

        private final File baseFile;
        private final URI baseUri;

        public BaseConfigurationSource(File baseFile) {
            this.baseFile = baseFile;
            baseUri = baseFile.toURI();
        }

        @Override
        public Resource getResource(String name) throws IOException {
            // Originally only File was supported. Class loader and URI were added
            // later. However (see bug 65106) treating some URIs as files can cause
            // problems. Therefore, if path starts with a valid URI scheme then skip
            // straight to processing this as a URI.
            if (!UriUtil.isAbsoluteURI(name)) {
                File f = new File(name);
                if (!f.isAbsolute()) {
                    f = new File(baseFile, name);
                }
                if (f.isFile()) {
                    FileInputStream fis = new FileInputStream(f);
                    return new Resource(fis, f.toURI());
                }

                // Try classloader
                InputStream stream = null;
                try {
                    stream = getClass().getClassLoader().getResourceAsStream(name);
                    if (stream != null) {
                        return new Resource(stream,
                                getClass().getClassLoader().getResource(name).toURI());
                    }
                } catch (URISyntaxException e) {
                    stream.close();
                    throw new IOException(sm.getString("configurationSource.cannotObtainURL", name), e);
                }
            }

            // Then try URI.
            URI uri;
            try {
                uri = getURIInternal(name);
            } catch (IllegalArgumentException e) {
                throw new IOException(sm.getString("configurationSource.cannotObtainURL", name));
            }

            // Obtain the input stream we need
            try {
                URL url = uri.toURL();
                return new Resource(url.openConnection().getInputStream(), uri);
            } catch (MalformedURLException e) {
                throw new IOException(sm.getString("configurationSource.cannotObtainURL", name), e);
            }
        }

        @Override
        public URI getURI(String name) {
            // Originally only File was supported. Class loader and URI were added
            // later. However (see bug 65106) treating some URIs as files can cause
            // problems. Therefore, if path starts with a valid URI scheme then skip
            // straight to processing this as a URI.
            if (!UriUtil.isAbsoluteURI(name)) {
                File f = new File(name);
                if (!f.isAbsolute()) {
                    f = new File(baseFile, name);
                }
                if (f.isFile()) {
                    return f.toURI();
                }

                // Try classloader
                try {
                    URL resource = getClass().getClassLoader().getResource(name);
                    if (resource != null) {
                        return resource.toURI();
                    }
                } catch (Exception e) {
                    // Ignore
                }
            }

            return getURIInternal(name);
        }

        private URI getURIInternal(String name) {
            // Then try URI.
            // Using resolve() enables the code to handle relative paths that did
            // not point to a file
            URI uri;
            if (baseUri != null) {
                uri = baseUri.resolve(name);
            } else {
                uri = URI.create(name);
            }
            return uri;
        }
    }
}
