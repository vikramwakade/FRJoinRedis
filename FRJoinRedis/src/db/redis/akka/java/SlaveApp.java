package db.redis.akka.java;

import java.net.UnknownHostException;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.kernel.Bootable;

import com.typesafe.config.ConfigFactory;

public class SlaveApp implements Bootable {
	private ActorSystem system;
	
	public SlaveApp() throws UnknownHostException {
		system = ActorSystem.create("DatabaseSlaveSystem", ConfigFactory.load()
		        .getConfig("server"));
		
		ActorRef actor = system.actorOf(new Props(SlaveActor.class), "slaveActor");
		
		System.out.println(actor.path());
	}
	
	@Override
	public void startup() {
		System.out.println("Server System Started");
	}
	
	@Override
	public void shutdown() {
	   system.shutdown();
	}
}
