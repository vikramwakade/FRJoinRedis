package db.redis.akka.java.msgs;

import java.io.Serializable;

public class AggregateResult implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private AggregateQuery.Measure measure;
	
	private double result;
	
	private long count;

	public AggregateResult() {
		super();
		this.measure = null;
		this.result = 0;
		this.count = 0;
	}
	
	public AggregateResult(AggregateQuery.Measure measure, double result, long count) {
		super();
		this.measure = measure;
		this.result = result;
		this.count = count;
	}

	public AggregateQuery.Measure getMeasure() {
		return measure;
	}

	public void setMeasure(AggregateQuery.Measure measure) {
		this.measure = measure;
	}

	public double getResult() {
		return result;
	}

	public void setResult(double result) {
		this.result = result;
	}

	public long getCount() {
		return count;
	}

	public void setCount(long count) {
		this.count = count;
	}
	
}
