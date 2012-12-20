package org.adbcj.jdbc;

import org.adbcj.DbException;

/**
 * @deprecated
 */
public class ActiveRequestIncomplete extends DbException {
	private static final long serialVersionUID = 1L;

	public ActiveRequestIncomplete(String message) {
		super(message);
	}
}
