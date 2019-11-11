package fr.egaetan.sql.base;

import java.util.ArrayList;
import java.util.List;

import fr.egaetan.sql.Query.RowPredicate;
import fr.egaetan.sql.Resultat;
import fr.egaetan.sql.Resultat.ResultatBuilder;
import fr.egaetan.sql.common.Column;
import fr.egaetan.sql.common.DataRow;
import fr.egaetan.sql.exception.ColumnDoesntExist;

public class Table implements TableSelect {

	private String name;
	private List<TableColumn> columns;
	private TableData data;

	public static class ColumnType {
		
		public final static ColumnType ENTIER = new ColumnType(ColumnTypeGroup.ENTIER, 10); 
		public final static ColumnType STRING = new ColumnType(ColumnTypeGroup.STRING, 16); 
		
		public static enum ColumnTypeGroup {
			ENTIER, 
			STRING,
			;
		}
		
		ColumnTypeGroup group;
		int size;
		
		public ColumnType(ColumnTypeGroup group, int size) {
			super();
			this.group = group;
			this.size = size;
		}
		
	}
	
	
	public static class TableDataRow implements DataRow {
		private final Object[] data;

		public TableDataRow(Object[] data) {
			super();
			this.data = data;
		}

		@Override
		public Object[] data() {
			return data;
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
	
	public static class TableBuilder {

		private String name;
		private List<TableColumn> columns = new ArrayList<>();
		
		public TableBuilder(String name) {
			this.name = name;
		}

		public TableBuilder addColumn(String name, ColumnType type) {
			int index = columns.size();
			columns.add(new TableColumn(name, this.name + "." + name, type, index));
			return this;
		}

		public Table build() {
			return new Table(name, columns);
		}
		
	}
	
	public Table(String name, List<TableColumn> columns) {
		this.name = name;
		this.columns = columns;
		this.data = new TableData(columns.size());
	}

	@Override
	public Resultat select(List<Column> resultColumns, List<RowPredicate> predicates) {
		ResultatBuilder resultat = Resultat.create(resultColumns);
		int nbResultColumns = resultColumns.size();
		for (DataRow row : data.rows) {
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
		TableSelect from;

		public Values(TableSelect table) {
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

	@Override
	public Column column(String string) {
		return columns.stream().filter(c -> c.name.equalsIgnoreCase(string))
				.findFirst()
				.orElseThrow(() -> new ColumnDoesntExist(string));
	}

	public Values values() {
		return new Values(this);
	}

	public boolean has(Column column) {
		return columns.stream().anyMatch(c -> column.need(c));
	}

	
	@Override
	public String toString() {
		return name;
	}
	
	public String name() {
		return name;
	}

	public TableSelect alias(String string) {
		TableSelect origine = this;
		return new TableSelect() {
			
			@Override
			public Resultat select(List<Column> resultColumns, List<RowPredicate> predicates) {
				return origine.select(resultColumns, predicates);
			}
			
			@Override
			public Column column(String string) {
				Column column = origine.column(string);
				return new Column() {

					@Override
					public Object readFrom(DataRow row) {
						return column.readFrom(row);
					}

					@Override
					public String qualifiedName() {
						return string + "." + displayName();
					}
					
					@Override
					public String displayName() {
						return column.displayName();
					}

					@Override
					public ColumnType type() {
						return column.type();
					}
					
				};
			}

			@Override
			public boolean has(Column column) {
				return column(column.qualifiedName().split("\\.")[1]).qualified().identify(column.qualified());
			}

			@Override
			public String name() {
				return string;
			}
		};
	}
}
