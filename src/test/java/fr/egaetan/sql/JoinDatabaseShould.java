package fr.egaetan.sql;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import fr.egaetan.sql.base.Base;
import fr.egaetan.sql.base.Table;
import fr.egaetan.sql.base.Table.ColumnType;

public class JoinDatabaseShould {

	
	@Test
	public void createTable_full_join() {
		// GIVEN
		Base base = Base.create();
		Table tableClient = base
				.createTable("client")
				.addColumn("id", ColumnType.ENTIER)
				.addColumn("value", ColumnType.STRING)
				.build();
		tableClient.insert(tableClient.values().set("id", 1).set("value", "John"));
		tableClient.insert(tableClient.values().set("id", 1).set("value", "Roger"));
		tableClient.insert(tableClient.values().set("id", 1).set("value", "Paul"));
		tableClient.insert(tableClient.values().set("id", 2).set("value", "Jack"));
		tableClient.insert(tableClient.values().set("id", 2).set("value", "Rick"));
		Table tableColor = base
				.createTable("color")
				.addColumn("id", ColumnType.ENTIER)
				.addColumn("color", ColumnType.STRING)
				.build();
		tableColor.insert(tableColor.values().set("id", 1).set("color", "Blue"));
		tableColor.insert(tableColor.values().set("id", 2).set("color", "Red"));
		
		// WHEN
		Resultat res = new Query().select(tableClient.column("id"), tableClient.column("value"), tableColor.column("color"))
				.from(tableClient, tableColor)
				//.innerJoin(tableColor).on(tableClient.column("id")).isEqualTo(tableColor.column("id"))
				.execute();
		
		// THEN
		
		System.out.println( 
				String.format("%10s | %10s | %10s", res.columns().get(0).name(), res.columns().get(1).name(), res.columns().get(2).name()));
		for (int i = 0; i < res.size(); i++)
			System.out.println( 
					String.format("%10s | %10s | %10s", res.rowAt(i).value("id"),res.rowAt(i).value("value"),res.rowAt(i).value("color")));
		
		
		Assertions.assertThat(res.size()).isEqualTo(10);
		Assertions.assertThat(res.columns().size()).isEqualTo(3);
		Assertions.assertThat(res.columns().get(0).name()).isEqualTo("id");
		Assertions.assertThat(res.columns().get(1).name()).isEqualTo("value");
		Assertions.assertThat(res.columns().get(0).type()).isEqualTo(ColumnType.ENTIER);
		Assertions.assertThat(res.columns().get(1).type()).isEqualTo(ColumnType.STRING);
		Assertions.assertThat(res.rowAt(0).value("value")).isEqualTo("John");
		Assertions.assertThat(res.rowAt(0).value("id")).isEqualTo(1);
		Assertions.assertThat(res.rowAt(0).value("color")).isEqualTo("Blue");
	}
	
	@Test
	public void createTable_inner_join_all_columns_requested() {
		// GIVEN
		Base base = Base.create();
		Table tableClient = base
				.createTable("client")
				.addColumn("id", ColumnType.ENTIER)
				.addColumn("value", ColumnType.STRING)
				.build();
		tableClient.insert(tableClient.values().set("id", 1).set("value", "John"));
		tableClient.insert(tableClient.values().set("id", 1).set("value", "Roger"));
		tableClient.insert(tableClient.values().set("id", 1).set("value", "Paul"));
		tableClient.insert(tableClient.values().set("id", 2).set("value", "Jack"));
		tableClient.insert(tableClient.values().set("id", 2).set("value", "Rick"));
		Table tableColor = base
				.createTable("color")
				.addColumn("id_color", ColumnType.ENTIER)
				.addColumn("color", ColumnType.STRING)
				.build();
		tableColor.insert(tableColor.values().set("id_color", 1).set("color", "Blue"));
		tableColor.insert(tableColor.values().set("id_color", 2).set("color", "Red"));
		
		// WHEN
		Resultat res = new Query().select(tableClient.column("id"), tableClient.column("value"), tableColor.column("color"), tableColor.column("id_color"))
				.from(tableClient)
				.innerJoin(tableColor).on(tableClient.column("id")).isEqualTo(tableColor.column("id_color"))
				.execute();
		
		// THEN
		System.out.println( 
				String.format("%10s | %10s | %10s", res.columns().get(0).name(), res.columns().get(1).name(), res.columns().get(2).name()));
		for (int i = 0; i < res.size(); i++)
			System.out.println( 
					String.format("%10s | %10s | %10s", res.rowAt(i).value("id"),res.rowAt(i).value("value"),res.rowAt(i).value("color")));
		
		Assertions.assertThat(res.size()).isEqualTo(5);
		Assertions.assertThat(res.columns().size()).isEqualTo(4);
		Assertions.assertThat(res.columns().get(0).name()).isEqualTo("id");
		Assertions.assertThat(res.columns().get(1).name()).isEqualTo("value");
		Assertions.assertThat(res.columns().get(0).type()).isEqualTo(ColumnType.ENTIER);
		Assertions.assertThat(res.columns().get(1).type()).isEqualTo(ColumnType.STRING);
		Assertions.assertThat(res.rowAt(0).value("value")).isEqualTo("John");
		Assertions.assertThat(res.rowAt(0).value("id")).isEqualTo(1);
		Assertions.assertThat(res.rowAt(0).value("color")).isEqualTo("Blue");
		
		
		
	}
	
}
