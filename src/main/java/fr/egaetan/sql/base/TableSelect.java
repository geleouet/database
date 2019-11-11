package fr.egaetan.sql.base;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import fr.egaetan.sql.Resultat;
import fr.egaetan.sql.Query.RowPredicate;
import fr.egaetan.sql.common.Column;

public interface TableSelect {

	public default Resultat select(Column... resultColumns) {
		return select(Arrays.asList(resultColumns), Collections.emptyList());
	}

	Resultat select(List<Column> resultColumns, List<RowPredicate> predicates);

	Column column(String string);

	boolean has(Column column);

	String name();

}