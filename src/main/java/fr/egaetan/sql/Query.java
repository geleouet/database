package fr.egaetan.sql;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import fr.egaetan.sql.Resultat.ResultatBuilder;
import fr.egaetan.sql.base.Table.ColumnType;
import fr.egaetan.sql.base.TableSelect;
import fr.egaetan.sql.common.Column;
import fr.egaetan.sql.common.Column.ColumnQualifiedName;
import fr.egaetan.sql.common.DataRow;
import fr.egaetan.sql.exception.ColumnDoesntExist;
import fr.egaetan.sql.exception.TableNameSpecifiedMoreThanOnce;

public class Query {

	public static class RowPredicate {

		private Column column;
		private Object value;

		public RowPredicate(Column column, Object value) {
			this.column = column;
			this.value = value;
		}

		public boolean valid(DataRow row) {
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

		public List<RowPredicate> predicates(TableSelect table) {
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
		
		private List<TableSelect> tables;
		private QuerySelect querySelect;
		private List<QueryPredicate> queryPredicates;
		private List<QueryPredicateJoin> queryJoinPredicates;

		public QueryFrom(QuerySelect querySelect, TableSelect ... tables) {
			this.tables = new ArrayList<>(Arrays.asList(tables));
			this.querySelect = querySelect;
			this.queryPredicates = new ArrayList<>();
			this.queryJoinPredicates = new ArrayList<>();
		}

		public void addPredicate(QueryPredicate queryPredicate) {
			queryPredicates.add(queryPredicate);
		}

		public QueryWhere where(Column column) {
			return new QueryWhere(this, column);
		}

		public Resultat execute() {
			QuerySelect select = querySelect;
			ResultatBuilder builder = new ResultatBuilder(Arrays.asList(select.columns));
			List<Resultat> resultats = new ArrayList<>();
			
			for (int i = 0; i < tables.size(); i++) {
				
				TableSelect table = tables.get(i);
				List<RowPredicate> predicates = new ArrayList<>();
				
				for (QueryPredicate queryPredicate : queryPredicates) {
					predicates.addAll(queryPredicate.predicates(table));
				}
				
				List<Column> columns = select.columns(table);
				for (QueryPredicateJoin q : queryJoinPredicates) {
					if (table.has(q.a) && columns.stream().noneMatch(c -> c.qualified().identify(q.a.qualified()))) {
						columns.add(q.a);
					}
					if (table.has(q.b) && columns.stream().noneMatch(c -> c.qualified().identify(q.b.qualified()))) {
						columns.add(q.b);
					}
				}
				
				
				Resultat resultat = table.select(columns, predicates);
				resultats.add(resultat);
			}
			
			List<Integer> current = initCartesianProductIndex(resultats);
			
			boolean finished = false;
			while (!finished) {
				Object[] row = new Object[select.columns.length];
				for (int i = 0; i < select.columns.length; i++) {
					ColumnQualifiedName columnQualified = select.columns[i].qualified();
					
					for (int j = 0; j < resultats.size(); j++) {
						Resultat res = resultats.get(j);
						if (res.columns().stream().anyMatch(r -> columnQualified.identify(r.qualified()))) {
							Object value = res.rowAt(current.get(j)).value(columnQualified);
							row[i] = value;
							break;
						}
					}
					
				}

				boolean allChecked = true;
				for (QueryPredicateJoin q : queryJoinPredicates) {
					ColumnQualifiedName joinA = q.a.qualified();
					ColumnQualifiedName joinB = q.b.qualified();
					Object a = null;
					Object b = null;
					for (int j = 0; j < resultats.size(); j++) {
						Resultat res = resultats.get(j);
						if (res.columns().stream().anyMatch(r -> joinA.identify(r.qualified()))) {
							Object value = res.rowAt(current.get(j)).value(joinA);
							a = value;
						}
						if (res.columns().stream().anyMatch(r -> joinB.identify(r.qualified()))) {
							Object value = res.rowAt(current.get(j)).value(joinB);
							b = value;
						}
					}
					allChecked = allChecked && q.predicate.check(a, b);
				}
				
				if (allChecked) {
					builder.addRow(row);
				}
				
				
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

		private List<Integer> initCartesianProductIndex(List<Resultat> resultats) {
			List<Integer> current = new ArrayList<>();
			for (int i = 0; i < resultats.size(); i++) {
				current.add(0);
			}
			return current;
		}

		public QueryWhere and(Column column) {
			return where(column);
		}

		public QueryJoin innerJoin(TableSelect tableSelect) {
			if (tables.stream().anyMatch(t -> t.name().equalsIgnoreCase(tableSelect.name()))) {
				throw new TableNameSpecifiedMoreThanOnce(tableSelect.name());
			}
			
			return new QueryJoin(this, tableSelect);
		}
		
	}
	
	public static interface PredicateJoin {
		boolean check(Object a, Object b);
	}
	
	public static class QueryPredicateJoin {
		Column a;
		Column b;
		PredicateJoin predicate;
		
		public QueryPredicateJoin(Column a, Column b, PredicateJoin predicate) {
			super();
			this.a = a;
			this.b = b;
			this.predicate = predicate;
		}
		
		public boolean verify(Column[] columns, Object[] row) {
			int aColonne = -1;
			int bColonne = -1;
			for (int i = 0; i < columns.length; i++) {
				ColumnQualifiedName columnName = columns[i].qualified();
				if (columnName.identify(a.qualified())) {
					aColonne = i;
				}
				if (columnName.identify(b.qualified())) {
					bColonne = i;
				}
			}
			if (aColonne == -1) {
				throw new ColumnDoesntExist(a.qualifiedName());
			}
			if (bColonne == -1) {
				throw new ColumnDoesntExist(b.qualifiedName());
			}
			
			return predicate.check(row[aColonne], row[bColonne]);
		}
		
	}
	
	public static class QueryJoinOn {

		private QueryFrom queryFrom;
		private TableSelect table;
		private Column column;

		public QueryJoinOn(QueryFrom queryFrom, TableSelect table, Column column) {
			this.queryFrom = queryFrom;
			this.table = table;
			this.column = column;
		}

		public QueryFrom isEqualTo(Column column) {
			queryFrom.queryJoinPredicates.add(new QueryPredicateJoin(this.column, column, (a,b) -> a.equals(b)));
			queryFrom.tables.add(table);
			return queryFrom;
		}
		
	}
	public static class QueryJoin {

		private QueryFrom queryFrom;
		private TableSelect table;

		public QueryJoin(QueryFrom queryFrom, TableSelect table) {
			this.queryFrom = queryFrom;
			this.table = table;
			
		}

		public  QueryJoinOn on(Column column) {
			return new QueryJoinOn(queryFrom, table, column);
		}
		
	}
	
	public static class QuerySelect {

		private Column[] columns;

		public QuerySelect(Column[] columns) {
			this.columns = columns;
		}
		
		public List<Column> columns(TableSelect table) {
			return Arrays.stream(columns).filter(c -> table.has(c)).collect(Collectors.toList());
		}

		public QueryFrom from(TableSelect ... tables) {
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
			public Object readFrom(DataRow row) {
				return object.apply((Integer) column.readFrom(row));
			}
			
			@Override
			public String displayName() {
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
			public Object readFrom(DataRow row) {
				return object.apply((String) column.readFrom(row));
			}
			
			@Override
			public String displayName() {
				return name;
			}
		};
	}

}
