package db.redis.akka.java.msgs;

import java.io.Serializable;
import java.util.List;

public class BroadcastTable implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	String tableName;
	String joinColumnName;
	List<String> columnNames;
	List<List<String>> table;
	
	public BroadcastTable() {
		
	}
	
	public BroadcastTable(String tableName, String joinColumnName, List<String> columnNames,
			List<List<String>> table) {
		super();
		this.tableName = tableName;
		this.joinColumnName = joinColumnName;
		this.columnNames = columnNames;
		this.table = table;
	}

	public String getTableName() {
		return tableName;
	}
	
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
	
	public String getJoinColumnName() {
		return joinColumnName;
	}

	public void setJoinColumnName(String joinColumnName) {
		this.joinColumnName = joinColumnName;
	}

	public List<String> getColumnNames() {
		return columnNames;
	}
	
	public void setColumnNames(List<String> columnNames) {
		this.columnNames = columnNames;
	}
	
	public List<List<String>> getTable() {
		return table;
	}
	
	public void setTable(List<List<String>> table) {
		this.table = table;
	}
}
