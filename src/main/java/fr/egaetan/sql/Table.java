package fr.egaetan.sql;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import fr.egaetan.sql.Query.RowPredicate;
import fr.egaetan.sql.Resultat.ResultatBuilder;

public class Table {

	private String name;
	private List<Column> columns;
	private TableData data;

	public static enum ColumnType {
		ENTIER, 
		STRING,
		;
	}
	
	public static class TableDataRow {
		Object[] data;

		public TableDataRow(Object[] data) {
			super();
			this.data = data;
		}
		
	}
	public static class TableData {

		List<TableDataRow> rows = new ArrayList<>();
		int columns;
		
		public TableData(int size) {
			this.columns = size;
		}

		public void insert(Values values) {
			Object[] row = new Object[columns];
			for (int i = 0; i < columns; i++) {
				row[i] = values.valueAt(i);
			}
			rows.add(new TableDataRow(row));
		}
		
	}
	
	public static class Column {
		
		String name;
		ColumnType type;
		int index;

		public Column(String name, ColumnType type, int index) {
			this.name = name;
			this.type = type;
			this.index = index;
		}

		public Object readFrom(TableDataRow row) {
			return row.data[index];
		}

	}
	
	public static class TableBuilder {

		private String name;
		private List<Column> columns = new ArrayList<>();
		
		public TableBuilder(String name) {
			this.name = name;
		}

		public TableBuilder addColumn(String name, ColumnType type) {
			int index = columns.size();
			columns.add(new Column(name, type, index));
			return this;
		}

		public Table build() {
			return new Table(name, columns);
		}
		
	}
	
	public Table(String name, List<Column> columns) {
		this.name = name;
		this.columns = columns;
		this.data = new TableData(columns.size());
	}

	public Resultat select(Column... resultColumns) {
		return select(Arrays.asList(resultColumns), Collections.emptyList());
	}
	
	public Resultat select(List<Column> resultColumns, List<RowPredicate> predicates) {
		ResultatBuilder resultat = Resultat.create(resultColumns);
		int nbResultColumns = resultColumns.size();
		for (TableDataRow row : data.rows) {
			if (predicates.stream().allMatch(p -> p.valid(row))) {
				Object[] resultRow = new Object[nbResultColumns];
				for (int i = 0; i < nbResultColumns; i++) {
					resultRow[i] = resultColumns.get(i).readFrom(row);
				}
				resultat.addRow(resultRow);
			}
		}
		return resultat.build();
	}

	public static class Value {
		Object v;

		public Value(Object i) {
			super();
			this.v = i;
		}

		public static Value of(Object i) {
			return new Value(i);
		}

		public Object inside() {
			return v;
		}
	}
	
	public static class ColumnValue {
		Column column;
		Value value;
		
		public ColumnValue(Column column, Value value) {
			super();
			this.column = column;
			this.value = value;
		}
		
	}
	
	public static class Values {
		List<ColumnValue> values = new ArrayList<>();
		Table from;

		public Values(Table table) {
			from = table;
		}

		public Object valueAt(int i) {
			return values.get(i).value.inside();
		}

		public Values set(String string, Object i) {
			values.add(new ColumnValue(from.column(string), Value.of(i)));
			return this;
		}
		
	}
	
	public void insert(Values values) {
		data.insert(values);
	}

	public Column column(String string) {
		return columns.stream().filter(c -> c.name.equalsIgnoreCase(string)).findFirst().get();
	}

	public Values values() {
		return new Values(this);
	}

	public boolean has(Column c) {
		return columns.contains(c);
	}

}
