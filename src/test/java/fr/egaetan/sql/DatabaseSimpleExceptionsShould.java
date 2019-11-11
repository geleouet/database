package fr.egaetan.sql;

import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

import fr.egaetan.sql.base.Base;
import fr.egaetan.sql.base.ColumnDoesntExist;
import fr.egaetan.sql.base.Table;
import fr.egaetan.sql.base.Table.ColumnType;

public class DatabaseSimpleExceptionsShould {
	
	@Test
	public void createTable_insert_select_entier_formula() {
		// GIVEN
		Base base = Base.create();
		Table table = base
				.createTable("client")
				.addColumn("id", ColumnType.ENTIER)
				.addColumn("value", ColumnType.ENTIER)
				.build();
		table.insert(table.values().set("id", 1).set("value", 2));
		
		// THEN
		assertThrows(ColumnDoesntExist.class, () -> 

			// WHEN
			new Query().select(table.column("unknown"))
				.from(table).execute());
		
		
	}
	

	
}
