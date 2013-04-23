package db.redis.akka.java;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import scala.concurrent.Future;
import scala.concurrent.duration.Duration;
import akka.util.Timeout;
import scala.concurrent.Await;
import akka.actor.ActorRef;
import akka.actor.UntypedActor;
import akka.dispatch.Futures;
import akka.dispatch.Mapper;
import akka.pattern.Patterns;
import redis.clients.jedis.*;

import db.redis.akka.java.msgs.*;
import db.redis.akka.java.process.QueryProcessor;

public class SlaveActor extends UntypedActor {
    Jedis conn;
    String joinType;
    BroadcastMap bMap;
    BroadcastTable bTable;
    
    public void preStart() {
    	conn = new Jedis("localhost");
    }
    
    public void postStop() {
    	
    }
    
    @Override
    public void onReceive(Object message) throws Exception {
        	
    	if (message.equals("GET_CATALOG")) {
			// <tableName, Length>
			Map<String, Long> result = new HashMap<String, Long>();
            
            Jedis conn = new Jedis("localhost");
            
            List<String> schema = conn.lrange("schema", 0, -1);
            for (String tableName : schema) {
            	String col = conn.lindex(tableName+":columns", 0);
            	result.put(tableName, conn.llen(tableName+":"+col));
            	System.out.println(tableName + " " + conn.llen(tableName+":"+col));
            }
            
            System.out.println("Sending response from host : " + getSelf().toString() +
            		"\nTo : " + getSender().toString());
            getSender().tell(new CatalogResponse(getSelf(), result), getSelf());
    	}
    	else if (message instanceof BroadcastCommand) {
    		
    		BroadcastCommand br = (BroadcastCommand) message;
    		joinType = br.getType();
    		
			String tableName = br.getTableName();
			String joinColumnName = br.getJoinColumnName();
			List<String> columnNames = br.getColumnNames();
			List<List<String>> table = new ArrayList<List<String>>();
			List<ActorRef> remoteActors = br.getRemoteActors();
			
			// future objects to wait for the broadcast table to be received by every node
			List<Future<Object>> futures = new ArrayList<Future<Object>>();
			Timeout timeout = new Timeout(Duration.create(1000, "seconds"));
			
    		if (joinType.equals("NESTEDLOOPJOIN")) {
    			
				for (String c : columnNames) {
					List<String> val = new ArrayList<String>();
					val = conn.lrange(tableName+":"+c, 0, -1);
					table.add(val);
				}
				
				// if the join column is not in the list of columns expected in the result
				if (!columnNames.contains(joinColumnName)) {
					System.out.println("Join column not in the result list");
					List<String> val = new ArrayList<String>();
					val = conn.lrange(tableName+":"+joinColumnName, 0, -1);
					table.add(val);
				}
				
				BroadcastTable bt = new BroadcastTable(tableName, joinColumnName, columnNames, table);
				
				if (remoteActors.contains(getSelf())) {
					remoteActors.remove(getSelf());
					bTable = bt;
				}
				for (ActorRef actor : remoteActors) {
					System.out.println("Sending smaller table to : " + actor.toString());
					futures.add(Patterns.ask(actor, bt, timeout));
				}
    		}
    		else if (joinType.equals("HASHJOIN")) {
    			
				for (String c : columnNames) {
					List<String> val = new ArrayList<String>();
					val = conn.lrange(tableName+":"+c, 0, -1);
					table.add(val);
				}
				
				List<String> joinColumn = conn.lrange(tableName+":"+joinColumnName, 0, -1);				
				HashMap<String, List<String>> map = new HashMap<String, List<String>> ();
				
				for (int i = 0; i < joinColumn.size(); i++) {
					List<String> val = new ArrayList<String>();
					for (int j = 0; j < table.size(); j++) {
						val.add(table.get(j).get(i));
					}
					map.put(joinColumn.get(i), val);
				}
				
				BroadcastMap bm = new BroadcastMap(tableName, joinColumnName, columnNames, map);
				
				if (remoteActors.contains(getSelf())) {
					remoteActors.remove(getSelf());
					bMap = bm;
				}				
				for (ActorRef actor : remoteActors) {
					System.out.println("Sending smaller table to : " + actor.toString());
					futures.add(Patterns.ask(actor, bm, timeout));
				}
    			
    		}
    		
    		Future<Iterable<Object>> futureSeq = Futures.sequence(futures,
					getContext().system().dispatcher());
			
			Future<Integer> futureResult = futureSeq.map(
				new Mapper<Iterable<Object>, Integer>() {
					
					public Integer apply(Iterable<Object> objs) {
						Integer count = new Integer(0);
						
						for (Object obj : objs) {
							String rs = (String) obj;
							if (rs.equals("SUCCESS")) {
								count = count + 1;
								System.out.println("\nHashMap received by : " + getSender());
							}
						}
						
						return count;
					}
				}
				, getContext().system().dispatcher());
			
			if (remoteActors.size() == Await.result(futureResult, timeout.duration())) {
				getSender().tell("SUCCESS", getSelf());
			}
    	}
    	else if (message instanceof BroadcastTable) {
    		if (!getSelf().equals(getSender())) {
	    		System.out.println("Broadcast Table received from : " + getSender().toString() + "\n By : " + getSelf().toString());
	    		bTable = (BroadcastTable) message;
	    		joinType = "NESTEDLOOPJOIN";
    		}
    		// SUCCCESS message sent to the sender
    		System.out.println("Sending SUCCESS msg to " + getSender().toString());
    		getSender().tell("SUCCESS", getSelf());
    	}
    	else if (message instanceof BroadcastMap) {
    		if (!getSelf().equals(getSender())) {
	    		System.out.println("\nBroadcast Map received from : " + getSender().toString() + "\n By : " + getSelf().toString());
	    		bMap = (BroadcastMap) message;
	    		joinType = "HASHJOIN";
    		}
    		// SUCCCESS message sent to the sender
    		System.out.println("Sending SUCCESS msg to " + getSender().toString());
    		getSender().tell("SUCCESS", getSelf());
    	}
        else if (message instanceof JoinQuery) {
        	JoinQuery join = (JoinQuery) message;
            List<String> result;
            
            System.out.println("Type of Join : " + joinType);
            if (joinType.equals("NESTEDLOOPJOIN")) {
            	//result = NLoopJoin(join.getTable1(), join.getTable2(), join.getT1_columns()
            	//	, join.getT2_columns(), join.getJoinType(), join.getPr());
            	result = QueryProcessor.NLoopJoin(conn, join.getTable1(), join.getTable2(), join.getT1_columns()
            		, join.getT2_columns(), join.getJoinType(), join.getPr(), bTable);
            }
            else {
            	//result = HashJoin(join.getTable1(), join.getTable2(), join.getT1_columns()
                //	, join.getT2_columns(), join.getJoinType(), join.getPr());
            	result = QueryProcessor.HashJoin(conn, join.getTable1(), join.getTable2(), join.getT1_columns()
                		, join.getT2_columns(), join.getJoinType(), join.getPr(), bMap);
            }
            
            System.out.println("Result Size : " + result.size()/(join.getT1_columns().size() + join.getT2_columns().size()));
            getSender().tell(result, getSelf());
        }
        else if (message instanceof AggregateQuery) {
        	AggregateQuery query = (AggregateQuery) message;
        	AggregateResult result = QueryProcessor.Aggregate(conn, query);
        	// important thing to do
        	result.setMeasure(query.getMeasure());
        	
        	getSender().tell(result, getSelf());
        }
        else {
            System.out.println("Server Actor unhandled message received");
            unhandled(message);
        }
    }
    
}