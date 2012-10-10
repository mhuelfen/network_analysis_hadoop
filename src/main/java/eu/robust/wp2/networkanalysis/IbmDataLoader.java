package eu.robust.wp2.networkanalysis;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.pig.LoadFunc;
import org.apache.pig.PigException;
import org.apache.pig.PigWarning;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.util.UDFContext;
import org.apache.pig.impl.util.Utils;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;

public class IbmDataLoader extends LoadFunc {
	protected RecordReader in = null;
	private byte fieldDel = '\t';
	private ArrayList<Object> mProtoTuple = null;
	private TupleFactory mTupleFactory = TupleFactory.getInstance();
	
	private static final int BUFFER_SIZE = 1024;
	
	// json part taken from JsonLoader
	private JsonFactory jsonFactory = null;
	private String udfcSignature = null;
	protected ResourceSchema schema = null;
    
	private TupleFactory tupleFactory = TupleFactory.getInstance();
    private BagFactory bagFactory = BagFactory.getInstance();
	
    private static final String SCHEMA_SIGNATURE = "pig.jsonloader.schema";

	public IbmDataLoader() {
	}

	/**
	 * Constructs a Pig loader that uses specified character as a field
	 * delimiter.
	 * 
	 * @param delimiter
	 *            the single byte character that is used to separate fields.
	 *            ("\t" is the default.)
	 */
	public IbmDataLoader(String delimiter) {
		this();
		if (delimiter.length() == 1) {
			this.fieldDel = (byte) delimiter.charAt(0);
		} else if (delimiter.length() > 1 && delimiter.charAt(0) == '\\') {
			switch (delimiter.charAt(1)) {
			case 't':
				this.fieldDel = (byte) '\t';
				break;

			case 'x':
				fieldDel = Integer.valueOf(delimiter.substring(2), 16)
						.byteValue();
				break;

			case 'u':
				this.fieldDel = Integer.valueOf(delimiter.substring(2))
						.byteValue();
				break;

			default:
				throw new RuntimeException("Unknown delimiter " + delimiter);
			}
		} else {
			throw new RuntimeException(
					"PigStorage delimeter must be a single character");
		}
	}

	@Override
	public Tuple getNext() throws IOException {
		try {
			boolean notDone = in.nextKeyValue();
			if (notDone) {
				return null;
			}
			Text value = (Text) in.getCurrentValue();
			byte[] buf = value.getBytes();
			int len = value.getLength();
			int start = 0;

			for (int i = 0; i < len; i++) {
				if (buf[i] == fieldDel) {
					readField(buf, start, i);
					start = i + 1;
				}
			}
			// // pick up the last field
			// readField(buf, start, len);
			
			// load json part from last field
			JsonParser p = jsonFactory.createJsonParser(buf, start, len);
			
//			// Read the start object marker.  Throughout this file if the parsing
//	        // isn't what we expect we return a tuple with null fields rather than
//	        // throwing an exception.  That way a few mangled lines don't fail the
//	        // job.
//	        if (p.nextToken() != JsonToken.START_OBJECT) {
//	            warn("Bad record, could not find start of record " +
//	                val.toString(), PigWarning.UDF_WARNING_1);
//	            return t;
//	        }
			
			ResourceFieldSchema[] fields = schema.getFields();
			
	        // Read each field in the record
	        for (int i = 0; i < fields.length; i++) {
	        	mProtoTuple.set(i, readJsonField(p, fields[i], i));
	        }

//	        if (p.nextToken() != JsonToken.END_OBJECT) {
//	            warn("Bad record, could not find end of record " +
//	                val.toString(), PigWarning.UDF_WARNING_1);
//	            return t;
//	        }
	        p.close();
			
			// end new

			Tuple t = mTupleFactory.newTupleNoCopy(mProtoTuple);
			mProtoTuple = null;
			return t;
		} catch (InterruptedException e) {
			int errCode = 6018;
			String errMsg = "Error while reading input";
			throw new ExecException(errMsg, errCode,
					PigException.REMOTE_ENVIRONMENT, e);
		}

	}

	private void readField(byte[] buf, int start, int end) {
		if (mProtoTuple == null) {
			mProtoTuple = new ArrayList<Object>();
		}

		if (start == end) {
			// NULL value
			mProtoTuple.add(null);
		} else {
			mProtoTuple.add(new DataByteArray(buf, start, end));
		}
	}

	private Object readJsonField(JsonParser p,
            ResourceFieldSchema field,
            int fieldnum) throws IOException {
// Read the next token
JsonToken tok = p.nextToken();
if (tok == null) {
warn("Early termination of record, expected " + schema.getFields().length
+ " fields bug found " + fieldnum, PigWarning.UDF_WARNING_1);
return null;
}

// Check to see if this value was null
if (tok == JsonToken.VALUE_NULL) return null;

// Read based on our expected type
switch (field.getType()) {
case DataType.INTEGER:
// Read the field name
tok = p.nextToken();
if (tok == JsonToken.VALUE_NULL) return null;
return p.getIntValue();

case DataType.LONG:
tok = p.nextToken();
if (tok == JsonToken.VALUE_NULL) return null;
return p.getLongValue();

case DataType.FLOAT:
tok = p.nextToken();
return p.getFloatValue();

case DataType.DOUBLE:
tok = p.nextToken();
if (tok == JsonToken.VALUE_NULL) return null;
return p.getDoubleValue();

case DataType.BYTEARRAY:
tok = p.nextToken();
if (tok == JsonToken.VALUE_NULL) return null;
byte[] b = p.getText().getBytes();
// Use the DBA constructor that copies the bytes so that we own
// the memory
return new DataByteArray(b, 0, b.length);

case DataType.CHARARRAY:
tok = p.nextToken();
if (tok == JsonToken.VALUE_NULL) return null;
return p.getText();

case DataType.MAP:
// Should be a start of the map object
if (p.nextToken() != JsonToken.START_OBJECT) {
warn("Bad map field, could not find start of object, field "
   + fieldnum, PigWarning.UDF_WARNING_1);
return null;
}
Map<String, String> m = new HashMap<String, String>();
while (p.nextToken() != JsonToken.END_OBJECT) {
String k = p.getCurrentName();
String v = p.getText();
m.put(k, v);
}
return m;

case DataType.TUPLE:
if (p.nextToken() != JsonToken.START_OBJECT) {
warn("Bad tuple field, could not find start of object, "
   + "field " + fieldnum, PigWarning.UDF_WARNING_1);
return null;
}

ResourceSchema s = field.getSchema();
ResourceFieldSchema[] fs = s.getFields();
Tuple t = tupleFactory.newTuple(fs.length);

for (int j = 0; j < fs.length; j++) {
t.set(j, readJsonField(p, fs[j], j));
}

if (p.nextToken() != JsonToken.END_OBJECT) {
warn("Bad tuple field, could not find end of object, "
   + "field " + fieldnum, PigWarning.UDF_WARNING_1);
return null;
}
return t;

case DataType.BAG:
if (p.nextToken() != JsonToken.START_ARRAY) {
warn("Bad bag field, could not find start of array, "
   + "field " + fieldnum, PigWarning.UDF_WARNING_1);
return null;
}

s = field.getSchema();
fs = s.getFields();
// Drill down the next level to the tuple's schema.
s = fs[0].getSchema();
fs = s.getFields();

DataBag bag = bagFactory.newDefaultBag();

JsonToken innerTok;
while ((innerTok = p.nextToken()) != JsonToken.END_ARRAY) {
if (innerTok != JsonToken.START_OBJECT) {
   warn("Bad bag tuple field, could not find start of "
       + "object, field " + fieldnum, PigWarning.UDF_WARNING_1);
   return null;
}

t = tupleFactory.newTuple(fs.length);
for (int j = 0; j < fs.length; j++) {
   t.set(j, readJsonField(p, fs[j], j));
}

if (p.nextToken() != JsonToken.END_OBJECT) {
   warn("Bad bag tuple field, could not find end of "
       + "object, field " + fieldnum, PigWarning.UDF_WARNING_1);
   return null;
}
bag.add(t);
}
return bag;
default:
throw new IOException("Unknown type in input schema: " +
field.getType());
}

}

//------------------------------------------------------------------------
	
	@Override
	public InputFormat getInputFormat() {
		return new TextInputFormat();
	}

	@Override
	public void prepareToRead(RecordReader reader, PigSplit split) throws IOException{
		in = reader;
		  // Get the schema string from the UDFContext object.
        UDFContext udfc = UDFContext.getUDFContext();
        Properties p =
                udfc.getUDFProperties(this.getClass(), new String[]{udfcSignature});
            String strSchema = p.getProperty(SCHEMA_SIGNATURE);
        if (strSchema == null) {
            throw new IOException("Could not find schema in UDF context");
        }

        // Parse the schema from the string stored in the properties object.
        schema = new ResourceSchema(Utils.getSchemaFromString(strSchema));

        jsonFactory = new JsonFactory();
	}

	@Override
	public void setLocation(String location, Job job) throws IOException {
		FileInputFormat.setInputPaths(job, location);
	}
}
