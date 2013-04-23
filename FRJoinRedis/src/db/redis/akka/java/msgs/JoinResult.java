package db.redis.akka.java.msgs;

import java.io.Serializable;

import java.util.List;

public class JoinResult implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private List<String> result;
	private List<String> columns;
	
	public JoinResult(List<String> result, List<String> columns) {
		this.result = result;
		this.columns = columns;
	}
	
	public List<String> getResult() {
		return result;
	}
	
	public void setResult(List<String> result) {
		this.result = result;
	}
	
	public List<String> getColumns() {
		return columns;
	}
	
	public void setColumns(List<String> columns) {
		this.columns = columns;
	}
}
