package db.redis.akka.java;

import db.redis.akka.java.msgs.*;
import db.redis.akka.java.util.SystemCatalog;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;

import akka.actor.ActorRef;
import akka.actor.UntypedActor;
import akka.dispatch.Futures;
import akka.dispatch.Mapper;
import akka.pattern.Patterns;
import akka.util.Timeout;


public class MasterActor extends UntypedActor {
	
	@Override
    public void onReceive(Object message) throws Exception {
		
		if (message instanceof JoinQuery) {
			System.out.println("Join Query Message Received from Master System");
			
			long startTime = System.currentTimeMillis();
			
			JoinQuery jq = (JoinQuery) message;
			
			// Perform the preprocessing required for join
			List<ActorRef> remoteActors = joinPreProcessing(jq);
			
			long endTime   = System.currentTimeMillis();
			System.out.println("Time taken for replication : " + (endTime - startTime) );
			
			startTime = System.currentTimeMillis();
			
			List<Future<Object>> futures = new ArrayList<Future<Object>>();
			Timeout timeout = new Timeout(Duration.create(10000, "seconds"));
			
			for (ActorRef actor : remoteActors) {
				futures.add(Patterns.ask(actor, jq, timeout));
			}
			
			Future<Iterable<Object>> futureSeq = Futures.sequence(futures,
					getContext().system().dispatcher());
			
			Future<List<String>> futureResult = futureSeq.map(
				new Mapper<Iterable<Object>, List<String>>() {
					
					@SuppressWarnings("unchecked")
					public List<String> apply(Iterable<Object> objs) {
						List<String> finalResult = new ArrayList<String>();
						
						for (Object obj : objs) {
							List<String> rs = (List<String>) obj;
							finalResult.addAll(rs);
						}
						
						return finalResult;
					}
				}
				, getContext().system().dispatcher());
			
			List<String> finalResult = Await.result(futureResult, timeout.duration());
			
			endTime   = System.currentTimeMillis();
			
			
			int len = finalResult.size();
			
			int col = jq.getT1_columns().size() + jq.getT2_columns().size();
			
			/*for (int i = 0; i < len; i+=col) {
				for (int j = 0; j < col; j++) {
					System.out.print(finalResult.get(i+j) + "\t");
				}
				System.out.println();
			}*/
			
			System.out.println("Result Rows : " + len/col);
			System.out.println("Time taken by Join query : " + (endTime - startTime) );
			
			getSender().tell(finalResult, getSelf());
		}
		else if (message instanceof AggregateQuery) {
			System.out.println("Aggregate Query Message Received from Master System");
			
			long startTime = System.currentTimeMillis();
			
			AggregateQuery query = (AggregateQuery) message;
			
			List<ActorRef> remoteActors = new ArrayList<ActorRef>();
			remoteActors.addAll(SystemCatalog.catalog.get(query.getTableName()).keySet());
			
			List<Future<Object>> futures = new ArrayList<Future<Object>>();
			Timeout timeout = new Timeout(Duration.create(1000, "seconds"));
			
			for (ActorRef actor : remoteActors) {
				futures.add(Patterns.ask(actor, query, timeout));
			}
			
			Future<Iterable<Object>> futureSeq = Futures.sequence(futures,
					getContext().system().dispatcher());
			
			Future<AggregateResult> futureResult = futureSeq.map(
				new Mapper<Iterable<Object>, AggregateResult>() {
					
					public AggregateResult apply(Iterable<Object> objs) {
						AggregateResult finalResult = new AggregateResult();
						double value = 0;
						long count = 0;
						double max = -Double.MAX_VALUE;
						double min = Double.MAX_VALUE;
						
						for (Object obj : objs) {
							AggregateResult rs = (AggregateResult) obj;
							if (rs.getMeasure() == AggregateQuery.Measure.COUNT) {
								value += rs.getCount();
								finalResult.setResult(value);
							}
							else if (rs.getMeasure() == AggregateQuery.Measure.SUM) {
								value += rs.getResult();
								finalResult.setResult(value);
							}
							else if (rs.getMeasure() == AggregateQuery.Measure.AVG) {
								value += rs.getResult();
								count += rs.getCount();
								finalResult.setResult(value/count);
							}
							else if (rs.getMeasure() == AggregateQuery.Measure.MAX) {
								if (rs.getResult() > max) {
									max = rs.getResult();
									finalResult.setResult(max);
								}
							}
							else if (rs.getMeasure() == AggregateQuery.Measure.MIN) {
								if (rs.getResult() < min) {
									min = rs.getResult();
									finalResult.setResult(min);
								}
							}
						}
						
						return finalResult;
					}
				}
				, getContext().system().dispatcher());
			
			AggregateResult finalResult = Await.result(futureResult, timeout.duration());
			
			long endTime = System.currentTimeMillis();
			getSender().tell(finalResult, getSelf());
			//System.out.println("Result = " + finalResult.getResult());
			System.out.println("Time taken by Aggregate query : " + (endTime - startTime) );
		}
		else {
			System.out.println("Unhandled message received");
			unhandled(message);
		}
	}
	
	// Assuming the second table to be the smaller one!
	public List<ActorRef> joinPreProcessing(JoinQuery jq) throws Exception {
		String table1 = jq.getTable1();
		String table2 = jq.getTable2();
		ActorRef stActor = null;
		List<ActorRef> ltActors = new ArrayList<ActorRef>();
		
		//Map<ActorRef, Long> table1_distribution = SystemCatalog.catalog.get(table1);
		Map<ActorRef, Long> table2_distribution = SystemCatalog.catalog.get(table2);
		
		List<ActorRef> actors = new ArrayList<ActorRef>();
		actors.addAll(table2_distribution.keySet());
		stActor = actors.remove(0);
		
		ltActors.addAll(SystemCatalog.catalog.get(table1).keySet());
		
		// Assuming the second table to be the smaller one
		Timeout timeout = new Timeout(Duration.create(100, "seconds"));
		Future<Object> future = Patterns.ask(stActor, new BroadcastCommand(jq.getJoinType(), table2,
				jq.getPr().getTable2_column(), jq.getT2_columns(), ltActors), timeout);
		
		if ((new String("SUCCESS")).equals((String)Await.result(future, timeout.duration())))
			return ltActors;
		else {
			System.out.println("Some other message received!");
			return null;
		}
	}
	
}
