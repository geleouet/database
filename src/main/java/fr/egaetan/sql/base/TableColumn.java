package fr.egaetan.sql.base;

import fr.egaetan.sql.base.Table.ColumnType;
import fr.egaetan.sql.common.Column;

public class TableColumn implements Column {
	
	String name;
	ColumnType type;
	int index;
	String qualifiedName;

	public TableColumn(String name, String qualifiedName, ColumnType type, int index) {
		this.name = name;
		this.qualifiedName = qualifiedName;
		this.type = type;
		this.index = index;
	}

	@Override
	public String displayName() {
		return name;
	}

	@Override
	public String qualifiedName() {
		return qualifiedName;
	}
	
	@Override
	public  ColumnType type() {
		return type;
	}
}