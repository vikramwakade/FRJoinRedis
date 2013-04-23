package db.redis.akka.java.msgs;

import java.io.Serializable;

public class Predicate implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String table1_column;
	private String table2_column;
	private String operator;
	
	public Predicate(String c1, String c2, String op) {
		table1_column = c1;
		table2_column = c2;
		operator = op;
	}
	
	public String getTable1_column() {
		return table1_column;
	}

	public void setTable1_column(String table1_column) {
		this.table1_column = table1_column;
	}

	public String getTable2_column() {
		return table2_column;
	}

	public void setTable2_column(String table2_column) {
		this.table2_column = table2_column;
	}

	public String getOperator() {
		return operator;
	}

	public void setOperator(String operator) {
		this.operator = operator;
	}
}
