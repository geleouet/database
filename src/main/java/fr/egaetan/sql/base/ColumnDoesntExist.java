package fr.egaetan.sql.base;

public class ColumnDoesntExist extends RuntimeException {

	private static final long serialVersionUID = 1L;

	public ColumnDoesntExist(String message) {
		super(message);
	}
	
}
