package db.redis.akka.java.msgs;

import java.io.Serializable;

public class AggregateQuery implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	public static enum Measure {
		COUNT, SUM, AVG, MIN, MAX
	}
	
	private Measure measure;
	
	private String tableName;
	
	private String columnName;
	
	public AggregateQuery(Measure measure, String tableName, String columnName) {
		super();
		this.measure = measure;
		this.tableName = tableName;
		this.columnName = columnName;
	}
	
	public Measure getMeasure() {
		return measure;
	}

	public void setMeasure(Measure measure) {
		this.measure = measure;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public String getColumnName() {
		return columnName;
	}

	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}

}
