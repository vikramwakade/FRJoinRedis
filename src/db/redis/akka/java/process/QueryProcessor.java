package db.redis.akka.java.process;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import redis.clients.jedis.*;

import db.redis.akka.java.msgs.*;

public class QueryProcessor {
	
	public static List<String> NLoopJoin(Jedis conn, String t1, String t2, List<String> t1_columns, 
			List<String> t2_columns, String type, Predicate p, BroadcastTable bTable) {
		
		List<String> result = new ArrayList<String>();
		
		List<List<String>> table1 = new ArrayList<List<String>>();
		List<List<String>> table2 = null;
		
		System.out.println(" Size : " + bTable.getTable().size());
		if (t2.equals(bTable.getTableName())) {
			table2 = bTable.getTable();
		}
		else {
			System.out.println("Table2 name different than expected");
			return null;
		}
		
		for (String c : t1_columns) {
			List<String> val = new ArrayList<String>();
			val = conn.lrange(t1+":"+c, 0, -1);
			table1.add(val);
		}
		
		// if the join column is not in the list of columns expected in the result
		if (!t1_columns.contains(p.getTable1_column())) {
			List<String> val = new ArrayList<String>();
			val = conn.lrange(t1+":"+p.getTable1_column(), 0, -1);
			table1.add(val);
		}
		
		//check if col name given in join function exists in table. if so get its index ie offset from start of row
	    int c1_index = -1, c2_index = -1;
	    if (t1_columns.contains(p.getTable1_column()))
	    	c1_index = t1_columns.indexOf(p.getTable1_column());
	    else
	    	c1_index = table1.size() - 1;
	    
	    if (t2_columns.contains(p.getTable2_column()))
	    	c2_index = t2_columns.indexOf(p.getTable2_column());
	    else
	    	c2_index = table2.size() - 1;
	    
	    // t1_columns and t2_columns contain the actual column names expected in the result
	    // table1 and table2 also contain the join columns
	    System.out.println("c1 index : " + c1_index + "\nc2 index : " + c2_index);
	    for (int i = 0; i < table1.get(c1_index).size(); i++) {
	    	for (int j = 0; j < table2.get(c2_index).size(); j++) {
	    		if (table1.get(c1_index).get(i).equals(table2.get(c2_index).get(j))) {
	    			for (int k = 0; k < t1_columns.size(); k++) {
	    				// adding all elements of the corresponding row of table 1
	    				result.add((String) table1.get(k).get(i));
	    			}
	    			for (int l = 0; l < t2_columns.size(); l++) {
	    				// adding all elements of the corresponding row of table 2
	    				result.add((String) table2.get(l).get(j));
	    			}
	    		}
	    	}
	    }
		
		return result;
	}
    
	public static List<String> HashJoin(Jedis conn, String t1, String t2, List<String> t1_columns, 
			List<String> t2_columns, String type, Predicate p, BroadcastMap bMap) {
		List<String> result = new ArrayList<String>();
		
		List<List<String>> table1 = new ArrayList<List<String>>();
		Map<String, List<String>> table2_map = null;
	
		if (t2.equals(bMap.getTableName())) {
			table2_map = bMap.getTable();
		}
		else {
			System.out.println("Table2 name different than expected");
			return null;
		}
		
		for (String c : t1_columns) {
			List<String> val = new ArrayList<String>();
			val = conn.lrange(t1+":"+c, 0, -1);
			table1.add(val);
		}
		
		// if the join column is not in the list of columns expected in the result
		if (!t1_columns.contains(p.getTable1_column())) {
			List<String> val = new ArrayList<String>();
			val = conn.lrange(t1+":"+p.getTable1_column(), 0, -1);
			table1.add(val);
		}
		
		//check if col name given in join function exists in table. if so get its index ie offset from start of row
	    int c1_index = -1;
	    if (t1_columns.contains(p.getTable1_column()))
	    	c1_index = t1_columns.indexOf(p.getTable1_column());
	    else
	    	c1_index = table1.size() - 1;
	    
	    for (int i = 0; i < table1.get(c1_index).size(); i++) {
    		if (table2_map.containsKey( table1.get(c1_index).get(i)) ) {
    			for (int k = 0; k < t1_columns.size(); k++) {
    				// adding all elements of the corresponding row of table 1
    				result.add((String) table1.get(k).get(i));
    			}
    			result.addAll( table2_map.get(table1.get(c1_index).get(i)) );
    		}
	    }
		
		return result;
		
	}
	
	public static AggregateResult Aggregate(Jedis conn, AggregateQuery query) {
		String tableName = query.getTableName();
		String columnName = query.getColumnName();
		AggregateQuery.Measure measure = query.getMeasure();
		
		AggregateResult result = new AggregateResult();
		
		List<String> column = conn.lrange(tableName+":"+columnName, 0, -1);
		
		if (measure == AggregateQuery.Measure.COUNT) {
			result.setCount(column.size());
		}
		else if (measure == AggregateQuery.Measure.SUM) {
			double sum = 0;
			for (String val : column) {
				sum += (Double.parseDouble(val));
			}
			result.setResult(sum);
		}
		else if (measure == AggregateQuery.Measure.AVG) {
			double sum = 0;
			for (String val : column) {
				sum += (Double.parseDouble(val));
			}
			result.setCount(column.size());
			result.setResult(sum);
		}
		else if (measure == AggregateQuery.Measure.MAX) {
			double max = Double.parseDouble(column.get(0));
			for (String val : column) {
				double value = Double.parseDouble(val);
				if (value > max)
					max = value;
			}
			result.setResult(max);
		}
		else if (measure == AggregateQuery.Measure.MIN) {
			double min = Double.parseDouble(column.get(0));
			for (String val : column) {
				double value = Double.parseDouble(val);
				if (value < min)
					min = value;
			}
			result.setResult(min);
		}
		else {
			System.out.println("Unrecognizable Aggregate Operation!");
		}
		
		return result;
	}
	
}
