package fr.egaetan.sql.base;

import fr.egaetan.sql.base.Table.ColumnType;
import fr.egaetan.sql.common.Column;
import fr.egaetan.sql.common.DataRow;

public class TableColumn implements Column {
	
	String name;
	ColumnType type;
	int index;

	public TableColumn(String name, ColumnType type, int index) {
		this.name = name;
		this.type = type;
		this.index = index;
	}

	@Override
	public Object readFrom(DataRow row) {
		return row.data()[index];
	}

	@Override
	public String name() {
		return name;
	}
	
	@Override
	public  ColumnType type() {
		return type;
	}
}