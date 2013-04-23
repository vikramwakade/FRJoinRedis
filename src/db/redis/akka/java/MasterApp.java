package db.redis.akka.java;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Properties;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import akka.dispatch.*;
import scala.concurrent.Future;
import scala.concurrent.Await;
import scala.concurrent.duration.Duration;
import akka.pattern.Patterns;
import akka.util.Timeout;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.kernel.Bootable;
import com.typesafe.config.ConfigFactory;

import db.redis.akka.java.msgs.AggregateQuery;
import db.redis.akka.java.msgs.AggregateResult;
import db.redis.akka.java.msgs.CatalogResponse;
import db.redis.akka.java.msgs.JoinQuery;
import db.redis.akka.java.msgs.Predicate;
import db.redis.akka.java.util.SystemCatalog;

public class MasterApp implements Bootable {
	private ActorSystem system;
	private ActorRef actor;
	private List<ActorRef> remoteActors;
	private Properties configFile;
	
	public MasterApp() {
		system = ActorSystem.create("DatabaseMasterSystem", ConfigFactory.load()
		        .getConfig("client"));
		actor = system.actorOf(new Props(MasterActor.class), 
				"masterActor");
		remoteActors = new ArrayList<ActorRef>();
		configFile = new java.util.Properties();
		
		InputStream cfg = null;
		try {
			cfg = new FileInputStream("/akka-2.1.1/config/slaves.cfg");
			configFile.load(cfg); 
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		String[] slaves_array = configFile.getProperty("slaves").split(",");
		
		// read configurations for remote actor system
		List<String> slaves = Arrays.asList(slaves_array);
		
		// in actual implementation this will have hostnames instead of remote actor names
		/*slaves.add("10.245.75.77");
		slaves.add("10.250.36.181");
		slaves.add("10.250.5.199");
		slaves.add("10.250.27.241");
		slaves.add("10.250.51.16");
		slaves.add("10.250.24.89");
		slaves.add("10.250.56.223");
		slaves.add("10.250.49.22");
		slaves.add("10.249.99.215");
		slaves.add("10.250.31.143");
		*/
		
		// search in actor hierarchy for all remote actors.
		for (String slave : slaves) {
			remoteActors.add(system.actorFor(String.format(
					"akka://%s@%s:%s%s", "DatabaseSlaveSystem", slave, "2552", "/user/slaveActor")));
		}
		
	}
	
	private void populateCatalog() {
		// execute 
		List<Future<Object>> futures = new ArrayList<Future<Object>>();
		Timeout timeout = new Timeout(Duration.create(1000, "seconds"));
		
		for (ActorRef actor : remoteActors) {
			futures.add(Patterns.ask(actor, "GET_CATALOG", timeout));
		}
	
		Future<Iterable<Object>> futureSeq = Futures.sequence(futures,
				system.dispatcher());
		
		// aggregate the catalog
		Future<Map<String, Map<ActorRef, Long>>> futureCatalog = futureSeq.map(
			new Mapper<Iterable<Object>, Map<String, Map<ActorRef, Long>>>() {
				
				public Map<String, Map<ActorRef, Long>> apply(Iterable<Object> objs) {					
					Map<String, Map<ActorRef, Long>> map = new HashMap<String, Map<ActorRef, Long>>();
					for (Object obj : objs) {
						CatalogResponse res = (CatalogResponse) obj;
						// get all table names on that host
						Map<String, Long> tableLengthMap = res.getResult();
						// get the host name
						ActorRef actor = res.getActor();
						
						// for every table on that host
						for(String table : tableLengthMap.keySet()) {
			
							if (map.containsKey(table)) {
								Map<ActorRef, Long> hostLengthMap = map.get(table);
								hostLengthMap.put(actor, tableLengthMap.get(table));
								
								System.out.println("Adding entry for table : " + table);
								System.out.println("HostName : " + actor.toString() + "  Length : " + tableLengthMap.get(table));
							}
							else {
								Map<ActorRef, Long> hostLengthMap = new HashMap<ActorRef, Long>();
								hostLengthMap.put(actor, tableLengthMap.get(table));									
								map.put(table, hostLengthMap);
								
								System.out.println("Creating entry for table : " + table);
								System.out.println("HostName : " + actor.toString() + "  Length : " + tableLengthMap.get(table));
							}
						}
					}
					
					return map;
				}
			}
			, system.dispatcher());
		
		// await for results. blocking wait.
		try {
			SystemCatalog.catalog = Await.result(futureCatalog, timeout.duration());
			System.out.println("System Catalog Created!");
			System.out.println(SystemCatalog.catalog.toString());		
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public void startup() {
		
		while(true) {
			System.out.println("Enter the operation you want to perform \n1. Join \n2. Aggregate \n -> ");
			BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
			String operation = null;
			
			try {
				operation = br.readLine();
			} catch (IOException ioe) {
		        System.out.println("IO error trying to read your name!");
		        System.exit(1);
		    }
			
			if (operation.equals("1")) {
				InputStream cfg = null;
				try {
					cfg = new FileInputStream("/akka-2.1.1/config/join.cfg");
					configFile.load(cfg); 
				} catch (FileNotFoundException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}
				
				long startTime = System.currentTimeMillis();
				// populate the catalog information for the system
				populateCatalog();
				long endTime   = System.currentTimeMillis();
				System.out.println("Time taken to populate catalog : " + (endTime - startTime) );
				
				String joinType = configFile.getProperty("JoinType");
				String table1 = configFile.getProperty("LTable");
				String table2 = configFile.getProperty("STable");
				String[] t1_columns = configFile.getProperty("LColumns").split(",");
				String[] t2_columns = configFile.getProperty("SColumns").split(",");
				
				System.out.println("Join Type = " + joinType);
				
				actor.tell(new JoinQuery(table1, table2, joinType,  new Predicate("cust_id","id","="),
						Arrays.asList(t1_columns), Arrays.asList(t2_columns)), null);
			}
			else if (operation.equals("2")) {
				InputStream cfg = null;
				try {
					cfg = new FileInputStream("/akka-2.1.1/config/aggregate.cfg");
					configFile.load(cfg); 
				} catch (FileNotFoundException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}
				
				long startTime = System.currentTimeMillis();
				// populate the catalog information for the system
				populateCatalog();
				long endTime   = System.currentTimeMillis();
				System.out.println("Time taken to populate catalog : " + (endTime - startTime) );
				
				String tableName = configFile.getProperty("TableName");
				String columnName = configFile.getProperty("ColumnName");
				String measure = configFile.getProperty("Measure");
				
				AggregateQuery.Measure m = null;
				if (measure.equals("AVG"))
					m = AggregateQuery.Measure.AVG;
				if (measure.equals("SUM"))
					m = AggregateQuery.Measure.SUM;
				if (measure.equals("COUNT"))
					m = AggregateQuery.Measure.COUNT;
				if (measure.equals("MAX"))
					m = AggregateQuery.Measure.MAX;
				if (measure.equals("MIN"))
					m = AggregateQuery.Measure.MIN;
				
				Timeout timeout = new Timeout(Duration.create(5, "seconds"));
				Future<Object> future = Patterns.ask(actor, new AggregateQuery(m, tableName, columnName), timeout);
				try {
					AggregateResult result = (AggregateResult) Await.result(future, timeout.duration());
					System.out.println("Result = " + result.getResult());
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		
	}
	
	@Override
	public void shutdown() {
	   system.shutdown();
	}
}
