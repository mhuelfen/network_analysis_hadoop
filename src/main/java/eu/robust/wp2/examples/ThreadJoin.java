package eu.robust.wp2.examples;

import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import eu.robust.wp2.examples.CountRatingsPerItemChain.SingleRatingMapper;
import eu.robust.wp2.examples.CountRatingsPerItemChain.SumRatingsReducer;
import eu.robust.wp2.networkanalysis.SimpleMapReduceChain;

public class ThreadJoin extends SimpleMapReduceChain {

	@Override
	public int run(String[] args) throws Exception {

		Map<String, String> parsedArgs = parseArgs(args);

		if (!parsedArgs.containsKey("--input")
				|| !parsedArgs.containsKey("--output")) {
			System.err.println("Specify --input and --output pathes!");
			return -1;
		}

		Path input = new Path(parsedArgs.get("--input"));
		Path output = new Path(parsedArgs.get("--output"));

		Job joinThreads = prepareJob(input, output, TextInputFormat.class,
				
				SingleRatingMapper.class, LongWritable.class,
				IntWritable.class, SumRatingsReducer.class, LongWritable.class,
				IntWritable.class,
				
				TextOutputFormat.class);
		joinThreads.setCombinerClass(SumRatingsReducer.class);

		joinThreads.waitForCompletion(true);

		return 0;
		// TODO Auto-generated method stub
	}

}
