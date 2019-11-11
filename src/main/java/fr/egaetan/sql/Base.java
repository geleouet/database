package fr.egaetan.sql;

import fr.egaetan.sql.Table.Column;
import fr.egaetan.sql.Table.TableBuilder;

public class Base {

	public static Base create() {
		return new Base();
	}

	public TableBuilder createTable(String name) {
		return new TableBuilder(name);
	}

}
