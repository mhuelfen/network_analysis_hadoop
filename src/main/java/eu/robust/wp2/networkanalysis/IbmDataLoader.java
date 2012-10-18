package eu.robust.wp2.networkanalysis;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.pig.LoadFunc;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.codehaus.jackson.map.ObjectMapper;

public class IbmDataLoader extends LoadFunc {

	private static final String DELIM = ";";
	private static final int DEFAULT_LIMIT = 226;
	private int limit = DEFAULT_LIMIT;
	private RecordReader reader;
	// private List indexes;
	private TupleFactory tupleFactory;

	/**
	 * Pig Loaders only take string parameters. The CTOR is really the only
	 * interaction the user has with the Loader from the script.
	 * 
	 * @param indexesAsStrings
	 */
	public IbmDataLoader(String... indexesAsStrings) {
		// this.indexes = new ArrayList();
		// for(String indexAsString : indexesAsStrings) {
		// indexes.add(new Integer(indexAsString));
		// }

		tupleFactory = TupleFactory.getInstance();
	}

	@Override
	public InputFormat getInputFormat() throws IOException {
		return new TextInputFormat();

	}

	/**
	 * the input in this case is a TSV, so split it, make sure that the
	 * requested indexes are valid,
	 */
	@Override
	public Tuple getNext() throws IOException {
		Tuple tuple = null;
		List values = new ArrayList();

		try {
			boolean notDone = reader.nextKeyValue();
			if (!notDone) {
				return null;
			}
			Text value = (Text) reader.getCurrentValue();

			if (value != null) {
				String parts[] = value.toString().split(DELIM);

				for (int i = 0; i < parts.length - 1; i++) {
					values.add(parts[i]);
				}

				// split json field
				String json = parts[parts.length - 1];
				String jsonFields[] = null;
				Map<String, String> jsonMap = null;
				if (json.length() > 0) {
					// TODO better handling of parsing errors and logging
					try {
						jsonMap = new ObjectMapper().readValue(json,
								HashMap.class);
					} catch (Exception e) {
						System.err.println(value);
						jsonMap = null;
					}
									}
				// make tuple to return results
				tuple = tupleFactory.newTuple(values);
				tuple.append(jsonMap);

			}

		} catch (InterruptedException e) {
			// add more information to the runtime exception condition.
			int errCode = 6018;
			String errMsg = "Error while reading input";
			throw new ExecException(errMsg, errCode,
					PigException.REMOTE_ENVIRONMENT, e);
		}

		return tuple;

	}

	@Override
	public void prepareToRead(RecordReader reader, PigSplit pigSplit)
			throws IOException {
		this.reader = reader; // note that for this Loader, we don't care about
								// the PigSplit.
	}

	@Override
	public void setLocation(String location, Job job) throws IOException {
		FileInputFormat.setInputPaths(job, location); // the location is assumed
														// to be comma separated
														// paths.

	}
}