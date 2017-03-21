package com.asgn3;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

/*Created by Viseshprasad Rajendraprasad
vrajend1@uncc.edu
*/

public class PageRank extends Configured implements Tool {

	public static final Logger LOG = Logger.getLogger(PageRank.class);
	//public static String PAGE_COUNT_PATH;

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new PageRank(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {
		Job pageCountjob = Job.getInstance(getConf(), " pageCountjob ");

		Configuration configuration = pageCountjob.getConfiguration(); // create
																		// a
		// configuration
		// reference

		// configuration.setInt("totalDocuments", (int) fileCount);

		pageCountjob.setJarByClass(this.getClass());
		pageCountjob.setMapperClass(MapPageCount.class);
		pageCountjob.setReducerClass(ReducePageCount.class);

		FileInputFormat.addInputPath(pageCountjob, new Path(args[0]));
		FileOutputFormat.setOutputPath(pageCountjob, new Path(args[1]));

		// Explicitly set key and value types of map and reduce output
		pageCountjob.setOutputKeyClass(Text.class);
		pageCountjob.setOutputValueClass(Text.class);
		pageCountjob.setMapOutputKeyClass(Text.class);
		pageCountjob.setMapOutputValueClass(IntWritable.class);

		int success = pageCountjob.waitForCompletion(true) ? 0 : 1;

		if (success == 0) {
			
			//PAGE_COUNT_PATH = args[1];

			Job linkGraphJob = Job.getInstance(getConf(), "linkGraphJob");

			Configuration conf = linkGraphJob.getConfiguration();
			conf.set("PAGE_COUNT_PATH", args[1]);

			linkGraphJob.setJarByClass(this.getClass());

			linkGraphJob.setMapperClass(MapLinkGraph.class);

			linkGraphJob.setReducerClass(ReduceLinkGraph.class);

			// job2.setInputFormatClass(KeyValueTextInputFormat.class);

			FileInputFormat.addInputPath(linkGraphJob, new Path(args[0]));

			FileOutputFormat.setOutputPath(linkGraphJob, new Path(args[2]));

			// Explicitly set key and value types of map and reduce output
			linkGraphJob.setOutputKeyClass(Text.class);
			linkGraphJob.setOutputValueClass(Text.class);
			linkGraphJob.setMapOutputKeyClass(Text.class);
			linkGraphJob.setMapOutputValueClass(Text.class);

			success = linkGraphJob.waitForCompletion(true) ? 0 : 1;
		}

		return success;
	}

	public static class MapPageCount extends Mapper<LongWritable, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {
			context.write(word, one);
		}
	}

	public static class ReducePageCount extends Reducer<Text, IntWritable, Text, Text> {
		@Override
		public void reduce(Text word, Iterable<IntWritable> counts, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable count : counts) {
				sum += count.get(); // sums up word occurrences
			}
			context.write(new Text(word), new Text(String.valueOf(sum)));
		}
	}

	public static class MapLinkGraph extends Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {

			double PRinit = 0;
			String line = lineText.toString().trim(); //todo : do i need trim?
			String title = null;
			
			//Read pageCount
			FileSystem fs = FileSystem.get(context.getConfiguration());
			Path path = null;// = new Path(context.getConfiguration().get("PAGE_COUNT_PATH"));
			RemoteIterator<LocatedFileStatus> fileStatusListIterator = fs.listFiles(
		            new Path(context.getConfiguration().get("PAGE_COUNT_PATH")), true);
		    while(fileStatusListIterator.hasNext()){
		        LocatedFileStatus fileStatus = fileStatusListIterator.next();
		        path = new Path(fileStatus.getPath().toString());
		    }
			BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(path)));
            String pageCount;
            pageCount=br.readLine().trim();
            if (pageCount != null){
            	PRinit = 1 / Integer.parseInt(pageCount);
            }

            //get page title from line
			Pattern pattern0 = Pattern.compile("<title>(.*?)</title>");
			java.util.regex.Matcher matcher0 = pattern0.matcher(line);
			while (matcher0.find()) {
				title = matcher0.group(1);
			}

			//get outlinks
			Pattern pattern = Pattern.compile("<text xml:space=\"preserve\">(.*?)</text>");
			java.util.regex.Matcher matcher = pattern.matcher(line);
			String prAndURLList = Double.toString(PRinit) + ",";
			while (matcher.find()) {
				String str1 = matcher.group(1);
				Pattern pattern1 = Pattern.compile("\\[\\[(.*?)\\]\\]");
				java.util.regex.Matcher matcher1 = pattern1.matcher(str1);
				while (matcher1.find()) {
					String url = matcher1.group(1).replace("[[", "").replace("]]", "");
					prAndURLList +=  url + "#####";
				}
			}

			if(!title.isEmpty()&&!prAndURLList.isEmpty())
			context.write(new Text(title), new Text(prAndURLList));
		}
	}

	public static class ReduceLinkGraph extends Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text url, Iterable<Text> prAndURLList, Context context)
				throws IOException, InterruptedException {
			for (Text text : prAndURLList) {
				context.write(url, text);
			}
		}
	}
}
