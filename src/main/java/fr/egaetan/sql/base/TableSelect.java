package fr.egaetan.sql.base;

import java.util.List;
import java.util.stream.Stream;

import fr.egaetan.sql.common.Column;
import fr.egaetan.sql.common.DataRow;

public interface TableSelect {

	Column column(String string);

	boolean has(Column column);

	String name();

	Stream<? extends DataRow> datas();
	
	List<? extends Column> columns();
}