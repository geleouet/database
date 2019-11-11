package fr.egaetan.sql.base;

import fr.egaetan.sql.base.Table.TableBuilder;

public class Base {

	public static Base create() {
		return new Base();
	}

	public TableBuilder createTable(String name) {
		return new TableBuilder(name);
	}

}
