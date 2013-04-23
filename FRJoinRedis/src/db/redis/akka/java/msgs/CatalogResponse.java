package db.redis.akka.java.msgs;

import java.io.Serializable;
import java.util.Map;

import akka.actor.ActorRef;

public class CatalogResponse implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private ActorRef actor;
	// Map(TableName, Length)
	private Map<String, Long> result;
	
	public CatalogResponse(ActorRef actor, Map<String, Long> map) {
		this.actor = actor;
		this.result = map;
	}
	
	public ActorRef getActor() {
		return actor;
	}
	public Map<String, Long> getResult() {
		return result;
	}
}
