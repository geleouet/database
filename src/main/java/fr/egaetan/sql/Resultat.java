package fr.egaetan.sql;

import java.util.ArrayList;
import java.util.List;

import fr.egaetan.sql.base.Table.ColumnType;
import fr.egaetan.sql.common.Column;
import fr.egaetan.sql.common.Column.ColumnQualifiedName;
import fr.egaetan.sql.common.DataRow;

public class Resultat {

	
	public static class ResultatColumn implements Column {

		private String name;
		private ColumnType type;
		private int index;
		private String qualifiedName;

		public ResultatColumn(String name, String qualifiedName, ColumnType type, int index) {
			this.name = name;
			this.qualifiedName = qualifiedName;
			this.type = type;
			this.index = index;
		}

		@Override
		public String displayName() {
			return name;
		}

		public String qualifiedName() {
			return qualifiedName;
		}

		public ColumnType type() {
			return type;
		}

		@Override
		public Object readFrom(DataRow row) {
			return row.data()[index];
		}
		
	}
	
	public static class ResultatRow implements DataRow {
		Object[] values;

		public ResultatRow(Object[] values) {
			super();
			this.values = values;
		}

		@Override
		public Object[] data() {
			return values;
		}

	}

	public static class ResultatBuilder {

		List<ResultatRow> rows = new ArrayList<>();
		List<ResultatColumn> columns = new ArrayList<>();

		public ResultatBuilder(List<? extends Column> columns) {
			this.columns = new ArrayList<>(columns.size());
			for (int i = 0; i < columns.size(); i++) {
				Column from = columns.get(i);
				this.columns.add(new ResultatColumn(from.displayName(), from.qualifiedName(), from.type(), i));
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
			return column.readFrom(row);
		}

		public Object value(ColumnQualifiedName columnQualified) {
			ResultatColumn column = from.columns.stream().filter(c -> columnQualified.identify(c.qualified())).findFirst().get();
			return column.readFrom(row);
		}
		
	}
	
	public ResultatLine rowAt(int i) {
		return new ResultatLine(rows.get(i), this);
	}


	@Override
	public String toString() {
		StringBuilder res = new StringBuilder();
		StringBuilder lineFormat = new StringBuilder();
		StringBuilder lineHeaderFormat = new StringBuilder();
		Object[] headers = new Object[columns.size()];
		for (int i = 0; i < columns.size(); i ++) {
			if (i != 0) {
				lineFormat.append(" | ");
				lineHeaderFormat.append(" | ");
			}
			lineFormat.append("%10s");
			lineHeaderFormat.append(" %-9s");
			headers[i] = columns.get(i).displayName();
		}
		String lineFormat$ = lineFormat.toString();
		res.append(String.format(lineHeaderFormat.toString(), headers));
		res.append("\n");
		StringBuilder separator = new StringBuilder();
		for (int i = 0; i < columns.size(); i++) {
			if (i != 0) {
				separator.append("-+-");
			}
			for (int j = 0; j < 10; j++) {
				separator.append("-");
			}
		}
		res.append(separator);
		res.append("\n");
		
		for (int i = 0; i < rows.size(); i++) {
			Object[] row = new Object[columns.size()];
			for (int j = 0; j < columns.size(); j++) {
				row[j] = rows.get(i).data()[j];
			}
			res.append(String.format(lineFormat$, row));
			res.append("\n");
		}
		return res.toString();
	}
	
}
