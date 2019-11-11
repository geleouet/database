package fr.egaetan.sql;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import fr.egaetan.sql.Resultat.ResultatBuilder;
import fr.egaetan.sql.base.Column;
import fr.egaetan.sql.base.Table;
import fr.egaetan.sql.base.Table.ColumnType;
import fr.egaetan.sql.base.Table.TableDataRow;

public class Query {

	public static class RowPredicate {

		private Column column;
		private Object value;

		public RowPredicate(Column column, Object value) {
			this.column = column;
			this.value = value;
		}

		public boolean valid(TableDataRow row) {
			Object data = column.readFrom(row);
			return value.equals(data);
		}

	}

	public static class QueryPredicate {

		private Object value;
		private Column column;

		public QueryPredicate(Column column, Object value) {
			this.column = column;
			this.value = value;
		}

		public List<RowPredicate> predicates(Table table) {
			if (table.has(column)) {
				return List.of(new RowPredicate(column, value));
			}
			return Collections.emptyList();
		}

	}

//	public static class QueryRunnable {
//
//		private QueryFrom queryFrom;
//		private QueryPredicate queryPredicate;
//
//		public QueryRunnable(QueryFrom queryFrom, QueryPredicate queryPredicate) {
//			this.queryFrom = queryFrom;
//			this.queryPredicate = queryPredicate;
//		}
//
//		public Resultat execute() {
//			QuerySelect select = queryFrom.querySelect;
//			Table table = queryFrom.table;
//			
//			Resultat resultat = table.select(select.columns(table), queryPredicate.predicates(table));
//			return resultat;
//		}
//
//	}

	public static class QueryWhere {

		private Column column;
		private QueryFrom queryFrom;

		public QueryWhere(QueryFrom queryFrom, Column column) {
			this.queryFrom = queryFrom;
			this.column = column;
		}

		public QueryFrom isEqualTo(Object o) {
			queryFrom.addPredicate(new QueryPredicate(column, o));
			return queryFrom;
		}

	}

	public static class QueryFrom {
		
		private List<Table> tables;
		private QuerySelect querySelect;
		private List<QueryPredicate> queryPredicates;

		public QueryFrom(QuerySelect querySelect, Table ... tables) {
			this.tables = new ArrayList<>(Arrays.asList(tables));
			this.querySelect = querySelect;
			this.queryPredicates = new ArrayList<>();
		}

		public void addPredicate(QueryPredicate queryPredicate) {
			queryPredicates.add(queryPredicate);
		}

		public QueryWhere where(Column column) {
			return new QueryWhere(this, column);
		}

		public Resultat execute() {
			QuerySelect select = querySelect;
			List<Resultat> resultats = new ArrayList<>();
			for (Table table : tables) {

				List<RowPredicate> predicates = new ArrayList<>();
				
				for (QueryPredicate queryPredicate : queryPredicates) {
					predicates.addAll(queryPredicate.predicates(table));
				}
				
				List<Column> columns = select.columns(table);
				Resultat resultat = table.select(columns, predicates);
				resultats.add(resultat);
			}
			
			ResultatBuilder builder = new ResultatBuilder(Arrays.asList(select.columns));
			List<Integer> current = new ArrayList<>();
			for (int i = 0; i < resultats.size(); i++) {
				current.add(0);
			}
			
			boolean finished = false;
			while (!finished) {
				Object[] row = new Object[select.columns.length];
				for (int i = 0; i < select.columns.length; i++) {
					String columnName = select.columns[i].name();
					for (int j = 0; j < resultats.size(); j++) {
						Resultat res = resultats.get(j);
						if (res.columns().stream().anyMatch(r -> r.name().equalsIgnoreCase(columnName))) {
							Object value = res.rowAt(current.get(j)).value(columnName);
							row[i] = value;
							break;
						}
					}
					
				}
				
				builder.addRow(row);
				
				
				finished = true;
				for (int i = 0; i < current.size(); i++) {
					if (current.get(i) + 1 < resultats.get(i).size()) {
						current.set(i, current.get(i) + 1);
						finished = false;
						break;
					}
					else {
						current.set(i, 0);
					}
				}
			}
			
			
			
			return builder.build();
		}

		public QueryWhere and(Column column) {
			return where(column);
		}

		public QueryJoin innerJoin(Table tableColor) {
			return null;
		}
		
	}
	
	public static class QueryJoinOn {

		public QueryFrom isEqualTo(Column column) {
			return null;
		}
		
	}
	public static class QueryJoin {

		public  QueryJoinOn on(Column column) {
			return null;
		}
		
	}
	
	public static class QuerySelect {

		private Column[] columns;

		public QuerySelect(Column[] columns) {
			this.columns = columns;
		}
		
		public List<Column> columns(Table table) {
			return Arrays.stream(columns).filter(c -> table.has(c)).collect(Collectors.toList());
		}

		public QueryFrom from(Table ... tables) {
			return new QueryFrom(this, tables);
		}

	}

	public QuerySelect select(Column ...columns) {
		return new QuerySelect(columns);
	}
	
	public static interface EntierFunction {
		Integer apply(Integer i);
	}
	public static interface StringFunction {
		String apply(String i);
	}

	public static Column compound(Column column, String name, EntierFunction object) {
		return new Column() {
			
			@Override
			public boolean need(Column c) {
				return column.need(c);
			}
			
			@Override
			public ColumnType type() {
				return ColumnType.ENTIER;
			}
			
			@Override
			public Object readFrom(TableDataRow row) {
				return object.apply((Integer) column.readFrom(row));
			}
			
			@Override
			public String name() {
				return name;
			}
		};
	}
	public static Column compound(Column column, String name, StringFunction object) {
		return new Column() {
			
			@Override
			public boolean need(Column c) {
				return column.need(c);
			}
			
			@Override
			public ColumnType type() {
				return ColumnType.STRING;
			}
			
			@Override
			public Object readFrom(TableDataRow row) {
				return object.apply((String) column.readFrom(row));
			}
			
			@Override
			public String name() {
				return name;
			}
		};
	}

}
