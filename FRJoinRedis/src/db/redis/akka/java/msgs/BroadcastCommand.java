package db.redis.akka.java.msgs;

import java.io.Serializable;
import java.util.List;
import akka.actor.ActorRef;

public class BroadcastCommand implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	String type;
	String tableName;
	String joinColumnName;
	List<String> columnNames;
	List<ActorRef> remoteActors;
	
	public BroadcastCommand(String type, String tableName, String joinColumnName, List<String> columnNames, List<ActorRef> remoteActors) {
		super();
		this.type = type;
		this.tableName = tableName;
		this.joinColumnName = joinColumnName;
		this.columnNames = columnNames;
		this.remoteActors = remoteActors;
	}
	
	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
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

	public List<ActorRef> getRemoteActors() {
		return remoteActors;
	}
	
	public void setRemoteActors(List<ActorRef> remoteActors) {
		this.remoteActors = remoteActors;
	}
}
