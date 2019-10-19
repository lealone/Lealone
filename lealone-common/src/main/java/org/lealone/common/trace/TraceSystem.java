/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.common.trace;

/**
 * The trace mechanism is the logging facility of this database. There is
 * usually one trace system per database. It is called 'trace' because the term
 * 'log' is already used in the database domain and means 'transaction log'. It
 * is possible to write after close was called, but that means for each write
 * the file will be opened and closed again (which is slower).
 * 
 * @author H2 Group
 * @author zhh
 */
public class TraceSystem {

    /**
     * The parent trace level should be used.
     */
    public static final int PARENT = -1;

    /**
     * This trace level means nothing should be written.
     */
    public static final int OFF = 0;

    /**
     * This trace level means only errors should be written.
     */
    public static final int ERROR = 1;

    /**
     * This trace level means errors and informational messages should be
     * written.
     */
    public static final int INFO = 2;

    /**
     * This trace level means all type of messages should be written.
     */
    public static final int DEBUG = 3;

    /**
     * This trace level means all type of messages should be written, but
     * instead of using the trace file the messages should be written to SLF4J.
     */
    public static final int ADAPTER = 4;

    /**
     * The default level for system out trace messages.
     */
    public static final int DEFAULT_TRACE_LEVEL_SYSTEM_OUT = OFF;

    /**
     * The default level for file trace messages.
     */
    public static final int DEFAULT_TRACE_LEVEL_FILE = ERROR;

    private final DefaultTraceWriter writer;

    public TraceSystem() {
        this(null);
    }

    /**
     * Create a new trace system object.
     *
     * @param fileName the file name
     */
    public TraceSystem(String fileName) {
        writer = new DefaultTraceWriter(fileName);
    }

    /**
     * Create a trace object for this module. Trace modules with names are not cached.
     *
     * @param module the module name
     * @return the trace object
     */
    public Trace getTrace(String module) {
        return new DefaultTrace(writer, module);
    }

    /**
     * Create a trace object for this module type.  
     *
     * @param traceModuleType the module type
     * @return the trace object
     */
    public Trace getTrace(TraceModuleType traceModuleType) {
        String module = traceModuleType.name().toLowerCase();
        String traceObjectName = "";
        return new DefaultTrace(writer, module, traceObjectName, -1);
    }

    public Trace getTrace(TraceModuleType traceModuleType, TraceObjectType traceObjectType, int id) {
        String module = traceModuleType.name().toLowerCase();
        String traceObjectName = traceObjectType.getShortName() + id;
        return new DefaultTrace(writer, module, traceObjectName, id);
    }

    /**
     * Set the trace file name.
     *
     * @param name the file name
     */
    public void setFileName(String name) {
        writer.setFileName(name);
    }

    /**
     * Set the maximum trace file size in bytes.
     *
     * @param max the maximum size
     */
    public void setMaxFileSize(int max) {
        writer.setMaxFileSize(max);
    }

    /**
     * Set the trace level to use for System.out
     *
     * @param level the new level
     */
    public void setLevelSystemOut(int level) {
        writer.setLevelSystemOut(level);
    }

    /**
     * Set the file trace level.
     *
     * @param level the new level
     */
    public void setLevelFile(int level) {
        writer.setLevelFile(level);
    }

    /**
     * Close the trace system.
     */
    public void close() {
        writer.close();
    }
}
