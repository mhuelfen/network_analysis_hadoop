package eu.robust.wp2.networkanalysis;

import java.io.IOException;
import java.util.HashMap;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;

public class GraphStats {
	PigServer pigServer = null;

	static private long START_TIME = 1177396027000L;
	static private long END_TIME = 1304241701000L;
	static private long WEEK = 86400 * 7 * 1000;

	public GraphStats() {

		try {
			this.pigServer = new PigServer(ExecType.LOCAL);
		} catch (ExecException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void calcStatsWeekwise(HashMap<String, String> params) {

		for (long maxTime = END_TIME - WEEK; maxTime > START_TIME; maxTime -= WEEK) {
			System.out.println(maxTime + " " + START_TIME);
			params.put("maxtime", Long.toString(maxTime));
			// call pig script
			try {
				System.out.println(params);
				pigServer.registerScript("pig/stats.pig", params);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			break;
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		HashMap<String, String> params = new HashMap<String, String>();
//		params.put("entity", "data/USER_ENTITY_2008.csv");
		params.put("entity", "data/ue_test_data.csv");
		GraphStats statcalculator = new GraphStats();
		statcalculator.calcStatsWeekwise(params);

	}
}
