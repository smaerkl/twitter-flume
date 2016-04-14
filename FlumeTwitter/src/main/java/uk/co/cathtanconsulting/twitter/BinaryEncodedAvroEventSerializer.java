package uk.co.cathtanconsulting.twitter;

import static org.apache.flume.serialization.AvroEventSerializerConfigurationConstants.COMPRESSION_CODEC;
import static org.apache.flume.serialization.AvroEventSerializerConfigurationConstants.DEFAULT_COMPRESSION_CODEC;
import static org.apache.flume.serialization.AvroEventSerializerConfigurationConstants.DEFAULT_SYNC_INTERVAL_BYTES;
import static org.apache.flume.serialization.AvroEventSerializerConfigurationConstants.SYNC_INTERVAL_BYTES;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.conf.Configurable;
import org.apache.flume.conf.ConfigurationException;
import org.apache.flume.serialization.AbstractAvroEventSerializer;
import org.apache.flume.serialization.EventSerializer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BinaryEncodedAvroEventSerializer implements EventSerializer,
		Configurable {

	private static final Logger logger = LoggerFactory
			.getLogger(AbstractAvroEventSerializer.class);

	private DatumWriter writer = null;
	private DataFileWriter dataFileWriter = null;

	private Schema schema;
	private final OutputStream out;

	private BinaryEncodedAvroEventSerializer(OutputStream out) {
		this.out = out;
	}

	public void write(Event event) throws IOException {
		dataFileWriter.appendEncoded(ByteBuffer.wrap(event.getBody()));
	}

	@Override
	public void configure(Context context) {
		logger.info("Configuring BinaryEncodedAvroEventSerializer 1.04");
		//////////////////////////////////////////////
	    int syncIntervalBytes =
		        context.getInteger(SYNC_INTERVAL_BYTES, 
		        		DEFAULT_SYNC_INTERVAL_BYTES);
		    String compressionCodec =
		        context.getString(COMPRESSION_CODEC, DEFAULT_COMPRESSION_CODEC);

		    writer = new ReflectDatumWriter(getSchema());
		    dataFileWriter = new DataFileWriter(writer);

		    dataFileWriter.setSyncInterval(syncIntervalBytes);

		    try {
		      CodecFactory codecFactory = CodecFactory.
		    		  fromString(compressionCodec);
		      dataFileWriter.setCodec(codecFactory);
		    } catch (AvroRuntimeException e) {
		      logger.warn("Unable to instantiate avro codec with name (" +
		          compressionCodec + 
		          "). Compression disabled. Exception follows.", e);
		    }
		
		//////////////////////////////////////////////
		
		
		String schemaString = context.getString("AvroSchema");
		
		if (schemaString.startsWith("{") && schemaString.endsWith("}")) {
			//Assume we've been given a schema literal
			try {
			schema=new Schema.Parser().parse(schemaString);
			} catch (RuntimeException e) {
				throw new ConfigurationException("Unable to parse AvroSchema configuration: Schema is not parseable",e);
			}
		} else if (schemaString.startsWith("hdfs://")) {
			Path path=new Path(schemaString);
            FileSystem fs;
			try {
				fs = FileSystem.get(new Configuration());
				schema=new Schema.Parser().parse(fs.open(path));
			} catch (IOException e) {
				throw new ConfigurationException("Unable to acquire AvroSchema from hdfs", e);
			}
		} else if (schemaString.startsWith("file://")) {
			File file = new File(schemaString);
			try {
				schema=new Schema.Parser().parse(new FileInputStream(file));
			} catch (IOException e) {
				throw new ConfigurationException("Unable to acquire AvroSchema from local filesystem", e);			}
		} else {
			throw new ConfigurationException("Unable to determine Avro schema");
		}
	}

	protected OutputStream getOutputStream() {
		return out;
	}

	protected Schema getSchema() {
		return schema;
	}

	public static class Builder implements EventSerializer.Builder {

		@Override
		public EventSerializer build(Context context, OutputStream out) {
			BinaryEncodedAvroEventSerializer writer =
					new BinaryEncodedAvroEventSerializer(out);
			writer.configure(context);
			return writer;
		}

	}

	// //////////////////////////////////////////////////////////////////////////////////////

	public void afterCreate() throws IOException {
		// write the AVRO container format header
		dataFileWriter.create(getSchema(), getOutputStream());
	}

	public void afterReopen() throws IOException {
		// impossible to initialize DataFileWriter without writing the schema?
		throw new UnsupportedOperationException(
				"Avro API doesn't support append");
	}

	public void flush() throws IOException {
		dataFileWriter.flush();
	}

	@Override
	public void beforeClose() throws IOException {
		// no-op
	}

	@Override
	public boolean supportsReopen() {
		return false;
	}

}
