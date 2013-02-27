/*
 *   Copyright (c) 2007 Mike Heath.  All rights reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *
 */
package org.adbcj.support;

import org.adbcj.*;
import org.adbcj.support.stacktracing.StackTracingOptions;

public class DefaultDbSessionFuture<T> extends DefaultDbFuture<T> implements DbSessionFuture<T> {

	private final DbSession session;
	
	public static <T> DefaultDbSessionFuture<T> createCompletedFuture(DbSession session, T value) {
		DefaultDbSessionFuture<T> future = new DefaultDbSessionFuture<T>(session);
		future.setResult(value);
		return future;
	}
	
	public static <T> DefaultDbSessionFuture<T> createCompletedErrorFuture(DbSession session, DbException exception) {
		DefaultDbSessionFuture<T> future = new DefaultDbSessionFuture<T>(session);
		future.setException(exception);
		return future;
	}
	
	@Override
	public DbSessionFuture<T> addListener(DbListener<T> listener) {
		super.addListener(listener);
		return this;
	}
	
	public DefaultDbSessionFuture(StackTracingOptions stackTracingOptions,DbSession session){
        super(stackTracingOptions);
		this.session = session;
	}

	public DefaultDbSessionFuture(StackTracingOptions stackTracingOptions,DbSession session, CancellationAction cancelAction) {
        super(stackTracingOptions,cancelAction);
		this.session = session;
	}
    /**
     * Intended for internal use, for example for transformation adaptors
     */
	protected DefaultDbSessionFuture(DbSession session, CancellationAction cancelAction) {
        super(cancelAction);
		this.session = session;
	}

    /**
     * Intended for internal use, for example for transformation adaptors
     */
	protected DefaultDbSessionFuture(DbSession session) {
        super();
		this.session = session;
	}

	public DbSession getSession() {
		return session;
	}


}
