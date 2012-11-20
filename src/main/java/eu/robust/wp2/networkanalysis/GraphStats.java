package eu.robust.wp2.networkanalysis;

import java.io.IOException;
import java.net.URL;
import java.util.HashMap;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;

public class GraphStats {
    PigServer pigServer = null;

    static private long START_TIME = 1177396027000L;
    static private long END_TIME = 1304241701000L;
    static private long WEEK = 86400 * 7 * 1000;

    public GraphStats() throws IOException {

        try {
            this.pigServer = new PigServer(ExecType.LOCAL);
            // this.pigServer = new PigServer(ExecType.MAPREDUCE);
        } catch (ExecException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public void calcStatsWeekwise(HashMap<String, String> params) {

        for (long maxTime = END_TIME - WEEK; maxTime > START_TIME; maxTime -= WEEK) {
            System.out.println(maxTime + " " + START_TIME);
            params.put("maxtime", Long.toString(maxTime));
//           params.put("entity","data/ue_test_data.csv");
            params.put("entity","data/USER_ENTITY_2008.csv");
            params.put("nodetype", "FORUM_THREAD");
            params.put("reltype", "REPLIED");
            
            // call pig script
            try {
                // String statsPigScript = this.getClass().getClassLoader()
                // .getResource("pig/stats.pig").getPath();
                // String statsPigScript = "pig/stats.pig";
//                String giniPigScript = this.getClass().getClassLoader()
//                        .getResource("pig/gini_coef.pig").getPath();

//                String comDistPigScript = this.getClass().getClassLoader()
//                        .getResource("pig/com_degree_dist.pig").getPath();
                String userEntroPigScript = this.getClass().getClassLoader()
                        .getResource("pig/user_com_entropy.pig").getPath();

                // pigServer.registerSccript(statsPigScript, params);
//                pigServer.registerScript(giniPigScript, params);
                //pigServer.registerScript(comDistPigScript, params);
                pigServer.registerScript(userEntroPigScript, params);
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            break;

        }
    }

    public void load_data(String dataFile) throws IOException {
        // register udf loader
        pigServer.registerJar("udf/ibmloader.jar");
        // load data
        pigServer
                .registerQuery("user_entity = LOAD '"
                        + dataFile
                        + "' USING eu.robust.wp2.networkanalysis.IbmDataLoader(';') AS (timestamp,nodeType1:chararray,nodeId1:chararray,rel:chararray,nodeType2:chararray,nodeId2:chararray,json:map[]);");
    }

    /**
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
        HashMap<String, String> params = new HashMap<String, String>();
        GraphStats statcalculator = new GraphStats();
        //statcalculator.load_data("data/USER_ENTITY_2008.csv");
        // statcalculator.load_data("data/USER_ENTITY.csv");
        // statcalculator.load_data("hdfs:///mhuelfen/USER_ENTITY.csv");

        statcalculator.calcStatsWeekwise(params);

    }
}
