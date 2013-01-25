package szhao.mr;


import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

public class BulkLoadDriver extends Configured implements Tool{

	@Override
	public int run(String[] arg0) throws Exception {
		// TODO Auto-generated method stub
		buildOpts(arg0);
		config = getConf();
		config.set("hbase.table.name", hBaseTableName);
		
		HBaseConfiguration.addHbaseResources(config);
		
		Job job = new Job(config, "BulkLoadJob");
		job.setJarByClass(BulkLoaderMapper.class);
		job.setMapperClass(BulkLoaderMapper.class);
		
		job.setMapOutputKeyClass(ImmutableBytesWritable.class);
		job.setMapOutputValueClass(KeyValue.class);
		
		job.setInputFormatClass(SequenceFileInputFormat.class);
		
		HTable hTable = new HTable(hBaseTableName);
		
		HFileOutputFormat.configureIncrementalLoad(job, hTable);
		FileInputFormat.addInputPath(job, hdfsInputPath);
		FileOutputFormat.setOutputPath(job, hdfsOutputPath);
		
		job.waitForCompletion(true);
		
		LoadIncrementalHFiles loader = new LoadIncrementalHFiles(config);
		loader.doBulkLoad(hdfsOutputPath, hTable);
		return 0;
	}
	
	@SuppressWarnings("static-access")
	private void buildOpts(String[] args) throws ParseException {
		opts = new Options();
		opts.addOption(OptionBuilder.hasArg().isRequired().create("input"));
		opts.addOption(OptionBuilder.hasArg().isRequired().create("output"));
		opts.addOption(OptionBuilder.hasArg().isRequired().create("hTableName"));
		
		CommandLineParser parser = new GnuParser();
		cli = parser.parse(opts, args);
		hdfsInputPath = new Path(cli.getOptionValue("input"));
		hdfsOutputPath = new Path(cli.getOptionValue("output"));
		hBaseTableName = cli.getOptionValue("hTableName");
	}
	
	private Options opts;
	private CommandLine cli;
	private String hBaseTableName;
	private Configuration config;
	private Path hdfsInputPath;
	private Path hdfsOutputPath;
}
