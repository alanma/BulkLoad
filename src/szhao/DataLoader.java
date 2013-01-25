package szhao;

import java.io.IOException;
import java.net.InetAddress;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import szhao.mr.BulkLoadDriver;

public class DataLoader extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int rcode = ToolRunner.run(new DataLoader(), args);
		System.exit(rcode);
	}
	
	
	@Override
	public int run(String[] arg0) throws Exception {
		// TODO Auto-generated method stub
		conf = getConf();
		parseOption(arg0);
		
		if (loadFileName != null) {
			System.err.println("Loading from File ["+loadFileName+"]");
			upstream = new FileUpstream(loadFileName);
		}
		hdfsPathToWrite = "/target/" + InetAddress.getLocalHost().getHostName() + "/date_here" + loadFileName;
		hFilePathToWrite = "/hfile/" + InetAddress.getLocalHost().getHostName() + "/date_here/";
		writeDataintoHDFS();
		System.err.println("\nLoaded file into HDFS");
		
		String[] nargs = {"-input", hdfsPathToWrite,
						"-output", hFilePathToWrite,
						"-hTableName", hTableName};
		FileSystem fs = FileSystem.get(conf);
		Path pt = new Path(hFilePathToWrite);
		if (fs.exists(pt)) {
			fs.delete(pt, true);
		}
		return ToolRunner.run(new BulkLoadDriver(), nargs);
	}
	
	@SuppressWarnings("static-access")
	private void parseOption(String[] args) throws ParseException {
		opts = new Options();
		opts.addOption(OptionBuilder.isRequired(false).hasArg()
				.create("loadFile"));
		opts.addOption(OptionBuilder.isRequired().hasArg().create("hTable"));
		CommandLineParser parser = new GnuParser();
		cli = parser.parse(opts, args);
		if (cli.hasOption("loadFile")) {
			loadFileName = cli.getOptionValue("loadFile");
		}
		hTableName = cli.getOptionValue("hTable");
		
	}
	
	private void writeDataintoHDFS() throws IOException {
		Path writePath = new Path(hdfsPathToWrite);
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(writePath)) {
			fs.delete(writePath, true);
			System.err.println("File ["+writePath+"] existing... Removed...");
		}
		SequenceFile.Writer writer = SequenceFile.createWriter(fs
				, conf, writePath, Text.class, MessageRecord.class, CompressionType.BLOCK);
		Text key = new Text();
		key.set("szhao.MessageRecord.class");
		MessageRecord record = new MessageRecord();
		try {
			while (upstream.getNextRecord(record)) {
				writer.append(key, record);
			}
			
		} finally {
			writer.close();
		}
	}
	
	private CommandLine cli;
	private Options opts;
	private Upstream upstream;
	private String loadFileName;
	private String hdfsPathToWrite;
	private String hFilePathToWrite;
	private String hTableName;
	private Configuration conf;
}
