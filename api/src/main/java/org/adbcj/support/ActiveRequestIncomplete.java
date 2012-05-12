package org.adbcj.support;

import org.adbcj.DbException;

public class ActiveRequestIncomplete extends DbException {
	private static final long serialVersionUID = 1L;

	public ActiveRequestIncomplete(String message) {
		super(message);
	}
}
