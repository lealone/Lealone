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
package org.lealone.mvstore;

public class Constants {

    /**
     * The file name suffix of a MVStore file.
     */
    public static final String SUFFIX_MV_FILE = ".mv.db";

    /**
     * The file name suffix of a new MVStore file, used when compacting a store.
     */
    public static final String SUFFIX_MV_STORE_NEW_FILE = ".newFile";

    /**
     * The file name suffix of a temporary MVStore file, used when compacting a
     * store.
     */
    public static final String SUFFIX_MV_STORE_TEMP_FILE = ".tempFile";

    /**
     * The file name suffix of large object files.
     */
    public static final String SUFFIX_LOB_FILE = ".lob.db";

    /**
     * The file name suffix of file lock files that are used to make sure a
     * database is open by only one process at any time.
     */
    public static final String SUFFIX_LOCK_FILE = ".lock.db";

    /**
     * The suffix of the directory name used if LOB objects are stored in a
     * directory.
     */
    public static final String SUFFIX_LOBS_DIRECTORY = ".lobs.db";

    /**
     * The file name suffix of temporary files.
     */
    public static final String SUFFIX_TEMP_FILE = ".temp.db";

    /**
     * The file name suffix of trace files.
     */
    public static final String SUFFIX_TRACE_FILE = ".trace.db";

}
