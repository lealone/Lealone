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
package org.lealone.storage;

import java.util.concurrent.atomic.AtomicInteger;

public class PageOperationHandlerFactory {

    private static PageOperationHandler DEFAULT_HANDLER = new PageOperationHandler() {
        @Override
        public void handlePageOperation(PageOperation po) {
            po.run(this);
        }
    };

    private static final AtomicInteger index = new AtomicInteger(0);
    private static PageOperationHandler[] pageOperationHandlers = { DEFAULT_HANDLER };

    public static void setPageOperationHandlers(PageOperationHandler[] pageOperationHandlers) {
        PageOperationHandlerFactory.pageOperationHandlers = pageOperationHandlers;
    }

    public static PageOperationHandler getPageOperationHandler() {
        return pageOperationHandlers[index.getAndIncrement() % pageOperationHandlers.length];
    }

    public static PageOperationHandler getNodePageOperationHandler() {
        return pageOperationHandlers[0];
    }

    public static PageOperationHandler getHandler(long key) {
        return pageOperationHandlers[(int) (key % pageOperationHandlers.length)];
    }

    public static void addPageOperation(PageOperation po) {
        getPageOperationHandler().handlePageOperation(po);
    }

    public static int getPageOperationHandlerCount() {
        return pageOperationHandlers.length;
    }
}
