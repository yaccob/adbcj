package org.adbcj;

public class DbSessionClosedException extends DbException {

	private static final long serialVersionUID = 1L;

	public DbSessionClosedException(String message, Throwable cause) {
		super( message, cause);
	}

	public DbSessionClosedException(String message) {
		super(message);
	}

	public DbSessionClosedException(Throwable cause) {
		super( cause);
	}

}
