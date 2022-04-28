/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.db.util;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.Reader;
import java.io.StringReader;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import javax.tools.Diagnostic;
import javax.tools.DiagnosticListener;
import javax.tools.FileObject;
import javax.tools.ForwardingJavaFileManager;
import javax.tools.JavaCompiler;
import javax.tools.JavaCompiler.CompilationTask;
import javax.tools.JavaFileManager;
import javax.tools.JavaFileObject;
import javax.tools.JavaFileObject.Kind;
import javax.tools.SimpleJavaFileObject;
import javax.tools.ToolProvider;

import org.lealone.common.exceptions.DbException;
import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;
import org.lealone.common.util.IOUtils;
import org.lealone.common.util.StringUtils;
import org.lealone.common.util.Task;
import org.lealone.common.util.Utils;
import org.lealone.db.api.ErrorCode;
import org.lealone.storage.fs.FileUtils;

/**
 * This class allows to convert source code to a class. It uses one class loader per class.
 * 
 * @author H2 Group
 * @author zhh
 */
public class SourceCompiler {

    private static final Class<?> JAVAC_SUN;

    /**
     * The class name to source code map.
     */
    final HashMap<String, String> sources = new HashMap<>();

    /**
     * The class name to byte code map.
     */
    final HashMap<String, Class<?>> compiled = new HashMap<>();

    private final String compileDir = Utils.getProperty("java.io.tmpdir", ".");

    static {
        Class<?> clazz;
        try {
            clazz = Class.forName("com.sun.tools.javac.Main");
        } catch (Exception e) {
            clazz = null;
        }
        JAVAC_SUN = clazz;
    }

    /**
     * Set the source code for the specified class.
     * This will reset all compiled classes.
     *
     * @param className the class name
     * @param source the source code
     */
    public void setSource(String className, String source) {
        sources.put(className, source);
        compiled.clear();
    }

    /**
     * Get the class object for the given name.
     *
     * @param packageAndClassName the class name
     * @return the class
     */
    public Class<?> getClass(String packageAndClassName) throws ClassNotFoundException {

        Class<?> compiledClass = compiled.get(packageAndClassName);
        if (compiledClass != null) {
            return compiledClass;
        }

        ClassLoader classLoader = new ClassLoader(getClass().getClassLoader()) {
            @Override
            public Class<?> findClass(String name) throws ClassNotFoundException {
                Class<?> classInstance = compiled.get(name);
                if (classInstance == null) {
                    String source = sources.get(name);
                    String packageName = null;
                    int idx = name.lastIndexOf('.');
                    String className;
                    if (idx >= 0) {
                        packageName = name.substring(0, idx);
                        className = name.substring(idx + 1);
                    } else {
                        className = name;
                    }
                    byte[] data = javacCompile(packageName, className, source);
                    if (data == null) {
                        classInstance = findSystemClass(name);
                    } else {
                        classInstance = defineClass(name, data, 0, data.length);
                        compiled.put(name, classInstance);
                    }
                }
                return classInstance;
            }
        };
        return classLoader.loadClass(packageAndClassName);
    }

    /**
     * Get the first public static method of the given class.
     *
     * @param className the class name
     * @return the method name
     */
    public Method getMethod(String className) throws ClassNotFoundException {
        Class<?> clazz = getClass(className);
        Method[] methods = clazz.getDeclaredMethods();
        for (Method m : methods) {
            int modifiers = m.getModifiers();
            if (Modifier.isPublic(modifiers)) {
                if (Modifier.isStatic(modifiers)) {
                    return m;
                }
            }
        }
        return null;
    }

    /**
     * Compile the given class. This method tries to use the class
     * "com.sun.tools.javac.Main" if available. If not, it tries to run "javac"
     * in a separate process.
     *
     * @param packageName the package name
     * @param className the class name
     * @param source the source code
     * @return the class file
     */
    byte[] javacCompile(String packageName, String className, String source) {
        File dir = new File(compileDir);
        if (packageName != null) {
            dir = new File(dir, packageName.replace('.', '/'));
            FileUtils.createDirectories(dir.getAbsolutePath());
        }
        File javaFile = new File(dir, className + ".java");
        File classFile = new File(dir, className + ".class");
        try {
            OutputStream f = FileUtils.newOutputStream(javaFile.getAbsolutePath(), false);
            PrintWriter out = new PrintWriter(IOUtils.getBufferedWriter(f));
            classFile.delete();
            if (source.startsWith("package ")) {
                out.println(source);
            } else {
                int endImport = source.indexOf("@CODE");
                String importCode = "import java.util.*;\n" + "import java.math.*;\n" + "import java.sql.*;\n";
                if (endImport >= 0) {
                    importCode = source.substring(0, endImport);
                    source = source.substring("@CODE".length() + endImport);
                }
                if (packageName != null) {
                    out.println("package " + packageName + ";");
                }
                out.println(importCode);
                out.println("public class " + className + " {\n" + "    public static " + source + "\n" + "}\n");
            }
            out.close();
            if (JAVAC_SUN != null) {
                javacSun(javaFile);
            } else {
                javacProcess(javaFile);
            }
            byte[] data = new byte[(int) classFile.length()];
            DataInputStream in = new DataInputStream(new FileInputStream(classFile));
            in.readFully(data);
            in.close();
            return data;
        } catch (Exception e) {
            throw DbException.convert(e);
        } finally {
            javaFile.delete();
            classFile.delete();
        }
    }

    private void javacProcess(File javaFile) {
        exec("javac", "-cp", cp(), "-sourcepath", compileDir, "-d", compileDir, "-encoding", "UTF-8",
                javaFile.getAbsolutePath());
    }

    private String cp() {
        StringBuilder cp = new StringBuilder();
        ClassLoader cl = this.getClass().getClassLoader();
        if (cl instanceof URLClassLoader) {
            URLClassLoader urlCl = (URLClassLoader) cl;
            for (URL url : urlCl.getURLs()) {
                File file = new File(url.getFile());
                cp.append(file.getAbsolutePath()).append(File.pathSeparatorChar);
            }
        }
        cp.append(".");
        return cp.toString();
    }

    private int exec(String... args) {
        ByteArrayOutputStream buff = new ByteArrayOutputStream();
        try {
            Process p = Runtime.getRuntime().exec(args);
            copyInThread(p.getInputStream(), buff);
            copyInThread(p.getErrorStream(), buff);
            p.waitFor();
            String err = new String(buff.toByteArray(), "UTF-8");
            throwSyntaxError(err);
            return p.exitValue();
        } catch (Exception e) {
            throw DbException.convert(e);
        }
    }

    private static void copyInThread(final InputStream in, final OutputStream out) {
        new Task() {
            @Override
            public void call() throws IOException {
                IOUtils.copy(in, out);
            }
        }.execute();
    }

    private void javacSun(File javaFile) {
        PrintStream old = System.err;
        ByteArrayOutputStream buff = new ByteArrayOutputStream();
        PrintStream temp = new PrintStream(buff);
        try {
            System.setErr(temp);
            Method compile;
            compile = JAVAC_SUN.getMethod("compile", String[].class);
            Object javac = JAVAC_SUN.getDeclaredConstructor().newInstance();
            compile.invoke(javac, (Object) new String[] { "-sourcepath", compileDir, "-cp", cp(), "-d", compileDir,
                    "-encoding", "UTF-8", javaFile.getAbsolutePath() });
            String err = new String(buff.toByteArray(), "UTF-8");
            throwSyntaxError(err);
        } catch (Exception e) {
            throw DbException.convert(e);
        } finally {
            System.setErr(old);
        }
    }

    private void throwSyntaxError(String err) {
        if (err.startsWith("Note:")) {
            // unchecked or unsafe operations - just a warning
        } else if (err.length() > 0) {
            err = StringUtils.replaceAll(err, compileDir, "");
            throw DbException.get(ErrorCode.SYNTAX_ERROR_1, err);
        }
    }

    public static byte[] compile(String className, String sourceCode) {
        return compile(SourceCompiler.class.getClassLoader(), className, sourceCode);
    }

    public static byte[] compile(ClassLoader classLoader, String className, String sourceCode) {
        try {
            return SCJavaCompiler.newInstance(classLoader, true).compile(className, sourceCode).entrySet().iterator()
                    .next().getValue();
        } catch (Exception e) {
            throw DbException.convert(e);
        }
    }

    public static Class<?> compileAsClass(String className, String sourceCode) {
        return compileAsClass(SourceCompiler.class.getClassLoader(), className, sourceCode);
    }

    public static <T> T compileAsInstance(String className, String sourceCode) {
        Class<?> clz = compileAsClass(className, sourceCode);
        return Utils.newInstance(clz);
    }

    public static Class<?> compileAsClass(ClassLoader classLoader, String className, String sourceCode) {
        // 不能直接传递参数classLoader给compile，要传自定义SCClassLoader，否则会有各种类找不到的问题
        SCClassLoader cl = new SCClassLoader(classLoader);
        byte[] bytes = compile(cl, className, sourceCode);
        return cl.getClass(className, bytes);
    }

    private static class SCClassLoader extends ClassLoader {
        public SCClassLoader(ClassLoader parent) {
            super(parent);
        }

        public Class<?> getClass(String className, byte[] bytes) {
            return defineClass(className, bytes, 0, bytes.length);
        }
    }

    private static class SCJavaFileManager extends ForwardingJavaFileManager<JavaFileManager> {

        private final ClassLoader classLoader;

        protected SCJavaFileManager(JavaFileManager fileManager, ClassLoader classLoader) {
            super(fileManager);
            this.classLoader = classLoader;
        }

        @Override
        public ClassLoader getClassLoader(Location location) {
            return classLoader;
        }

        @Override
        public Iterable<JavaFileObject> list(Location location, String packageName, Set<Kind> kinds, boolean recurse)
                throws IOException {
            return super.list(location, packageName, kinds, recurse);
        }

        @Override
        public JavaFileObject getJavaFileForOutput(Location location, String className, Kind kind, FileObject sibling)
                throws IOException {
            if (sibling != null && sibling instanceof SCJavaFileObject) {
                return ((SCJavaFileObject) sibling).addOutputJavaFile(className);
            }
            throw new IOException(
                    "The source file passed to getJavaFileForOutput() is not a SCJavaFileObject: " + sibling);
        }
    }

    private static class SCJavaFileObject extends SimpleJavaFileObject {

        private final String className;
        private final String sourceCode;
        private final ByteArrayOutputStream outputStream;
        private Map<String, SCJavaFileObject> outputFiles;

        public SCJavaFileObject(String className, String sourceCode) {
            super(makeURI(className), Kind.SOURCE);
            this.className = className;
            this.sourceCode = sourceCode;
            this.outputStream = null;
        }

        private SCJavaFileObject(String name, Kind kind) {
            super(makeURI(name), kind);
            this.className = name;
            this.outputStream = new ByteArrayOutputStream();
            this.sourceCode = null;
        }

        public boolean isCompiled() {
            return (outputFiles != null);
        }

        public Map<String, byte[]> getClassByteCodes() {
            Map<String, byte[]> results = new HashMap<>();
            for (SCJavaFileObject outputFile : outputFiles.values()) {
                results.put(outputFile.className, outputFile.outputStream.toByteArray());
            }
            return results;
        }

        public SCJavaFileObject addOutputJavaFile(String className) {
            if (outputFiles == null) {
                outputFiles = new LinkedHashMap<>();
            }
            SCJavaFileObject outputFile = new SCJavaFileObject(className, Kind.CLASS);
            outputFiles.put(className, outputFile);
            return outputFile;
        }

        @Override
        public Reader openReader(boolean ignoreEncodingErrors) throws IOException {
            return new StringReader(sourceCode);
        }

        @Override
        public CharSequence getCharContent(boolean ignoreEncodingErrors) throws IOException {
            return sourceCode;
        }

        @Override
        public OutputStream openOutputStream() {
            return outputStream;
        }

        private static URI makeURI(final String canonicalClassName) {
            int dotPos = canonicalClassName.lastIndexOf('.');
            String simpleClassName = dotPos == -1 ? canonicalClassName : canonicalClassName.substring(dotPos + 1);
            try {
                return new URI(simpleClassName + Kind.SOURCE.extension);
            } catch (URISyntaxException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static class SCJavaCompiler {

        private final boolean debug;
        private final JavaCompiler compiler;
        private final Collection<String> compilerOptions;
        private final DiagnosticListener<JavaFileObject> listener;
        private final SCJavaFileManager fileManager;

        public static SCJavaCompiler newInstance(ClassLoader classLoader, boolean debug) {
            JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
            if (compiler == null) {
                throw new RuntimeException("JDK Java compiler not available");
            }
            return new SCJavaCompiler(compiler, classLoader, debug);
        }

        private SCJavaCompiler(JavaCompiler compiler, ClassLoader classLoader, boolean debug) {
            this.debug = debug;
            this.compiler = compiler;
            this.listener = new SCDiagnosticListener();
            this.fileManager = new SCJavaFileManager(
                    compiler.getStandardFileManager(listener, null, Charset.forName("UTF-8")), classLoader);
            ArrayList<String> list = new ArrayList<>();
            list.add(this.debug ? "-g:source,lines,vars" : "-g:none");
            this.compilerOptions = list;
        }

        public Map<String, byte[]> compile(String className, String sourceCode)
                throws IOException, ClassNotFoundException {
            return doCompile(className, sourceCode).getClassByteCodes();
        }

        private SCJavaFileObject doCompile(String className, final String sourceCode)
                throws IOException, ClassNotFoundException {
            SCJavaFileObject compilationUnit = new SCJavaFileObject(className, sourceCode);
            CompilationTask task = compiler.getTask(null, fileManager, listener, compilerOptions, null,
                    Collections.singleton(compilationUnit));
            if (!task.call()) {
                throw new RuntimeException("Compilation failed", null);
            } else if (!compilationUnit.isCompiled()) {
                throw new ClassNotFoundException(className + ": Class file not created by compilation.");
            }
            return compilationUnit;
        }
    }

    private static class SCDiagnosticListener implements DiagnosticListener<JavaFileObject> {

        private static final Logger logger = LoggerFactory.getLogger(SCDiagnosticListener.class);

        @Override
        public void report(Diagnostic<? extends JavaFileObject> diagnostic) {
            if (diagnostic.getKind() == javax.tools.Diagnostic.Kind.ERROR) {
                String message = diagnostic.toString() + " (" + diagnostic.getCode() + ")";
                logger.error(message);
            } else if (logger.isTraceEnabled()) {
                logger.trace(diagnostic.toString() + " (" + diagnostic.getCode() + ")");
            }
        }
    }
}
