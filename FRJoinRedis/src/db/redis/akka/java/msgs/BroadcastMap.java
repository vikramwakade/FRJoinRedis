package db.redis.akka.java.msgs;

import java.io.Serializable;
import java.util.List;
import java.util.HashMap;

public class BroadcastMap implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String tableName;
	private String joinColumnName;
	private List<String> columnNames;
	private HashMap<String, List<String>> table;
	
	public BroadcastMap(String tableName, String joinColumnName,
			List<String> columnNames, HashMap<String, List<String>> table) {
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

	public HashMap<String, List<String>> getTable() {
		return table;
	}

	public void setTable(HashMap<String, List<String>> table) {
		this.table = table;
	}

}
