package db.redis.akka.java.util;

import java.util.Map;
import akka.actor.ActorRef;

public class SystemCatalog {
	
	public static Map<String, Map<ActorRef, Long>> catalog;

}
