package fr.egaetan.sql;

import java.util.ArrayList;
import java.util.List;

import fr.egaetan.sql.base.Table.ColumnType;
import fr.egaetan.sql.common.Column;

public class Resultat {

	
	public static class ResultatColumn {

		private String name;
		private ColumnType type;
		private int index;

		public ResultatColumn(String name, ColumnType type, int index) {
			this.name = name;
			this.type = type;
			this.index = index;
		}

		public String name() {
			return name;
		}

		public ColumnType type() {
			return type;
		}

		public Object read(ResultatRow row) {
			return row.values[index];
		}
		
	}
	
	public static class ResultatRow {
		Object[] values;

		public ResultatRow(Object[] values) {
			super();
			this.values = values;
		}

		public Object values(String string) {
			return null;
		}

	}

	public static class ResultatBuilder {

		List<ResultatRow> rows = new ArrayList<>();
		List<ResultatColumn> columns = new ArrayList<>();

		public ResultatBuilder(List<? extends Column> columns) {
			this.columns = new ArrayList<>(columns.size());
			for (int i = 0; i < columns.size(); i++) {
				Column from = columns.get(i);
				this.columns.add(new ResultatColumn(from.name(), from.type(), i));
			}
		}

		public Resultat build() {
			return new Resultat(rows, columns);
		}

		public void addRow(Object[] data) {
			rows.add(new ResultatRow(data));
		}

	}

	public static ResultatBuilder create(List<? extends Column> columns) {
		return new ResultatBuilder(columns);
	}

	public Resultat(List<ResultatRow> rows, List<ResultatColumn> columns) {
		this.rows = rows;
		this.columns = columns;
	}

	List<ResultatRow> rows = new ArrayList<>();
	List<ResultatColumn> columns = new ArrayList<>();

	public int size() {
		return rows.size();
	}

	public List<ResultatColumn> columns() {
		return columns;
	}

	public List<ResultatRow> values() {
		return rows;
	}

	public static class ResultatLine {
		ResultatRow row;
		Resultat from;
		
		public ResultatLine(ResultatRow row, Resultat from) {
			super();
			this.row = row;
			this.from = from;
		}

		public Object value(String string) {
			ResultatColumn column = from.columns.stream().filter(c -> c.name.equalsIgnoreCase(string)).findFirst().get();
			return column.read(row);
		}
		
	}
	
	public ResultatLine rowAt(int i) {
		return new ResultatLine(rows.get(i), this);
	}

	
}
