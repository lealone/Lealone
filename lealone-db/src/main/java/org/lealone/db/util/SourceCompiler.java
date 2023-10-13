/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.db.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Reader;
import java.io.StringReader;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.URI;
import java.net.URISyntaxException;
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
import org.lealone.common.util.Utils;

/**
 * This class allows to convert source code to a class. It uses one class loader per class.
 * 
 * @author H2 Group
 * @author zhh
 */
public class SourceCompiler {

    /**
     * The class name to source code map.
     */
    private final HashMap<String, String> sources = new HashMap<>();

    /**
     * The class name to byte code map.
     */
    private final HashMap<String, Class<?>> compiled = new HashMap<>();

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
     * Get the class object for the given name.
     *
     * @param packageAndClassName the class name
     * @return the class
     */
    private Class<?> getClass(String packageAndClassName) throws ClassNotFoundException {
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
                    byte[] data = compile(this, packageName, className, source);
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

    private static byte[] compile(ClassLoader classLoader, String packageName, String className,
            String source) {
        if (!source.startsWith("package ")) {
            StringBuilder buff = new StringBuilder();
            int endImport = source.indexOf("@CODE");
            String importCode = "import java.util.*;\n" + "import java.math.*;\n"
                    + "import java.sql.*;\n";
            if (endImport >= 0) {
                importCode = source.substring(0, endImport);
                source = source.substring("@CODE".length() + endImport);
            }
            if (packageName != null) {
                buff.append("package " + packageName + ";\n");
            }
            buff.append(importCode).append("\n");
            buff.append(
                    "public class " + className + " {\n" + "    public static " + source + "\n" + "}\n");
            source = buff.toString();
        }
        return compile(classLoader, className, source);
    }

    public static byte[] compile(String className, String sourceCode) {
        return compile(SourceCompiler.class.getClassLoader(), className, sourceCode);
    }

    public static byte[] compile(ClassLoader classLoader, String className, String sourceCode) {
        try {
            return SCJavaCompiler.newInstance(classLoader, true).compile(className, sourceCode)
                    .entrySet().iterator().next().getValue();
        } catch (Exception e) {
            throw DbException.convert(e);
        }
    }

    public static <T> T compileAsInstance(String className, String sourceCode) {
        Class<?> clz = compileAsClass(className, sourceCode);
        return Utils.newInstance(clz);
    }

    public static Class<?> compileAsClass(String className, String sourceCode) {
        return compileAsClass(SourceCompiler.class.getClassLoader(), className, sourceCode);
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
        public Iterable<JavaFileObject> list(Location location, String packageName, Set<Kind> kinds,
                boolean recurse) throws IOException {
            return super.list(location, packageName, kinds, recurse);
        }

        @Override
        public JavaFileObject getJavaFileForOutput(Location location, String className, Kind kind,
                FileObject sibling) throws IOException {
            if (sibling != null && sibling instanceof SCJavaFileObject) {
                return ((SCJavaFileObject) sibling).addOutputJavaFile(className);
            }
            throw new IOException(
                    "The source file passed to getJavaFileForOutput() is not a SCJavaFileObject: "
                            + sibling);
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
            String simpleClassName = dotPos == -1 ? canonicalClassName
                    : canonicalClassName.substring(dotPos + 1);
            try {
                return new URI(simpleClassName + Kind.SOURCE.extension);
            } catch (URISyntaxException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static class SCJavaCompiler {

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
                    compiler.getStandardFileManager(listener, null, Charset.forName("UTF-8")),
                    classLoader);
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
