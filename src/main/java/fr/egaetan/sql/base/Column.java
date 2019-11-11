package fr.egaetan.sql.base;

import fr.egaetan.sql.base.Table.ColumnType;
import fr.egaetan.sql.base.Table.TableDataRow;

public interface Column {

	Object readFrom(TableDataRow row);

	String name();

	ColumnType type();

	default boolean need(Column c) {
		return c == this;
	}

}