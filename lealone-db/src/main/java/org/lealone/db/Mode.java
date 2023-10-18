/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.db;

import org.lealone.common.util.CaseInsensitiveMap;

/**
 * The compatibility modes.
 * There is a fixed set of modes (for example PostgreSQL, MySQL).
 * Each mode has different settings.
 * 
 * @author H2 Group
 * @author zhh
 */
public class Mode {

    /**
     * The default mode.
     */
    private static final Mode DEFAULT_MODE = new Mode("REGULAR");

    private static final CaseInsensitiveMap<Mode> MODES = new CaseInsensitiveMap<>();

    static {
        Mode mode = DEFAULT_MODE;
        mode.nullConcatIsNull = true;
        mode.indexDefinitionInCreateTable = true;
        add(mode);

        mode = new Mode("MySQL");
        mode.convertInsertNullToZero = true;
        mode.indexDefinitionInCreateTable = true;
        mode.lowerCaseIdentifiers = true;
        add(mode);

        mode = new Mode("PostgreSQL");
        mode.aliasColumnName = true;
        mode.nullConcatIsNull = true;
        mode.systemColumns = true;
        mode.logIsLogBase10 = true;
        mode.serialColumnIsNotPK = true;
        add(mode);
    }

    private static void add(Mode mode) {
        MODES.put(mode.name, mode);
    }

    public static Mode getDefaultMode() {
        return DEFAULT_MODE;
    }

    /**
     * Get the mode with the given name.
     *
     * @param name the name of the mode
     * @return the mode object
     */
    public static Mode getInstance(String name) {
        return MODES.get(name);
    }

    public boolean isPostgreSQL() {
        return "PostgreSQL".equalsIgnoreCase(name);
    }

    public boolean isMySQL() {
        return "MySQL".equalsIgnoreCase(name);
    }

    // Modes are also documented in the features section

    /**
     * When enabled, aliased columns (as in SELECT ID AS I FROM TEST) return the
     * alias (I in this case) in ResultSetMetaData.getColumnName() and 'null' in
     * getTableName(). If disabled, the real column name (ID in this case) and
     * table name is returned.
     */
    public boolean aliasColumnName;

    /**
     * When inserting data, if a column is defined to be NOT NULL and NULL is
     * inserted, then a 0 (or empty string, or the current timestamp for
     * timestamp columns) value is used. Usually, this operation is not allowed
     * and an exception is thrown.
     */
    public boolean convertInsertNullToZero;

    /**
     * When converting the scale of decimal data, the number is only converted
     * if the new scale is smaller than the current scale. Usually, the scale is
     * converted and 0s are added if required.
     */
    public boolean convertOnlyToSmallerScale;

    /**
     * Creating indexes in the CREATE TABLE statement is allowed using
     * <code>INDEX(..)</code> or <code>KEY(..)</code>.
     * Example: <code>create table test(id int primary key, name varchar(255),
     * key idx_name(name));</code>
     */
    public boolean indexDefinitionInCreateTable;

    /**
     * Meta data calls return identifiers in lower case.
     */
    public boolean lowerCaseIdentifiers;

    /**
     * Concatenation with NULL results in NULL. Usually, NULL is treated as an
     * empty string if only one of the operands is NULL, and NULL is only
     * returned if both operands are NULL.
     */
    public boolean nullConcatIsNull;

    /**
     * Identifiers may be quoted using square brackets as in [Test].
     */
    public boolean squareBracketQuotedNames;

    /**
     * Support for the syntax
     * [OFFSET .. ROW|ROWS] [FETCH FIRST .. ROW|ROWS ONLY]
     * as an alternative for LIMIT .. OFFSET.
     */
    public boolean supportOffsetFetch = true;

    /**
     * The system columns 'CTID' and 'OID' are supported.
     */
    public boolean systemColumns;

    /**
     * Empty strings are treated like NULL values. Useful for Oracle emulation.
     */
    public boolean treatEmptyStringsAsNull;

    /**
     * Text can be concatenated using '+'.
     */
    public boolean allowPlusForStringConcat;

    /**
     * The function LOG() uses base 10 instead of E.
     */
    public boolean logIsLogBase10;

    /**
     * SERIAL and BIGSERIAL columns are not automatically primary keys.
     */
    public boolean serialColumnIsNotPK;

    /**
     * Swap the parameters of the CONVERT function.
     */
    public boolean swapConvertFunctionParameters;

    /**
     * Support the # for column names
     */
    public boolean supportPoundSymbolForColumnNames;

    private final String name;

    private Mode(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }
}
