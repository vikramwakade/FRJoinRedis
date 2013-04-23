package db.redis.akka.java;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import redis.clients.jedis.Jedis;

public class DataLoader {
	Jedis conn;
	
	public DataLoader() {
		conn = new Jedis("localhost");
	}
	
	public void loadData(String filename, String table, String[] columns, int start, int range) {
		try {
		    BufferedReader in = new BufferedReader(new FileReader(filename));
		    String str;
		    String[] vals = new String[columns.length];
		    
		    int counter = 1;
		    while ((str = in.readLine()) != null) {
		    	if (counter >= start && counter < (start + range)) {
			        vals = str.split(",");
			        for(int i = 0; i < columns.length; i++) {
			        	System.out.print(vals[i] + " ");
			        	//conn.rpush(table+":"+columns[i], vals[i]);
			        }
			        System.out.println();
		    	}
		    	else if (counter >= start + range)
		    		break;
		        counter++;
		    }
		    
		    in.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) {
		DataLoader dl = new DataLoader();
		
		String filename = "customer_100000.txt";
		String table = "customer";
		String[] columns = {"id", "name", "gender", "age"};
		
		dl.loadData(filename, table, columns, 1, 100000);
		
	}
}
