package fr.egaetan.sql.common;

import fr.egaetan.sql.base.Table.ColumnType;

public interface Column {

	Object readFrom(DataRow row);

	String name();

	ColumnType type();

	default boolean need(Column c) {
		return c == this;
	}

}