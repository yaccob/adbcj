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

import org.adbcj.Result;
import org.adbcj.ResultSet;

import java.util.List;


public class DefaultResult implements Result {

	final long affectedRows;
	final List<String> warnings;
	
	public DefaultResult(Long affectedRows, List<String> warnings) {
		this.affectedRows = affectedRows;
		this.warnings = warnings;
	}
	
	public long getAffectedRows() {
		return affectedRows;
	}

	public ResultSet getGeneratedKeys() {
		throw new UnsupportedOperationException("Not yet implemented");
	}

	public List<String> getWarnings() {
		return warnings;
	}

}
