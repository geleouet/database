package fr.egaetan.sql.base;

import fr.egaetan.sql.base.Table.ColumnType;
import fr.egaetan.sql.base.Table.TableDataRow;

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
	public Object readFrom(TableDataRow row) {
		return row.data[index];
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