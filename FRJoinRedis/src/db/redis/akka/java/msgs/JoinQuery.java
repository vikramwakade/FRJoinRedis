package db.redis.akka.java.msgs;

import java.util.List;
import java.io.Serializable;


public class JoinQuery implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String table1;		// outer table
	private String table2;		// inner table
	private String joinType;
	private Predicate pr;
	private List<String> t1_columns;	// columns of table1 to be selected
	private List<String> t2_columns;	// columns of table2 to be selected
	
	public JoinQuery(String table1, String table2, String joinType,
			Predicate pr, List<String> columns1, List<String> columns2) {
		this.table1 = table1;
		this.table2 = table2;
		this.joinType = joinType;
		this.pr = pr;
		this.t1_columns = columns1;
		this.t2_columns = columns2;
	}
	
	public List<String> getT1_columns() {
		return t1_columns;
	}

	public void setT1_columns(List<String> t1_columns) {
		this.t1_columns = t1_columns;
	}

	public List<String> getT2_columns() {
		return t2_columns;
	}

	public void setT2_columns(List<String> t2_columns) {
		this.t2_columns = t2_columns;
	}

	public String getTable1() {
		return table1;
	}

	public void setTable1(String table1) {
		this.table1 = table1;
	}

	public String getTable2() {
		return table2;
	}

	public void setTable2(String table2) {
		this.table2 = table2;
	}

	public String getJoinType() {
		return joinType;
	}

	public void setJoinType(String joinType) {
		this.joinType = joinType;
	}

	public Predicate getPr() {
		return pr;
	}

	public void setPr(Predicate pr) {
		this.pr = pr;
	}
}
