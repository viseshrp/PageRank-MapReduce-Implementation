package com.asgn3;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
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
	public static final String PR_DELIMITER = ",,,,,";
	public static final String URL_SPLIT_DELIMITER = "url_split_delimiter";
	public static final String OUTLINK_LIST_DELIMITER = "#####";

	// public static String PAGE_COUNT_PATH;

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new PageRank(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {
		Job pageCountjob = Job.getInstance(getConf(), " pageCountjob ");

		// Configuration configuration = pageCountjob.getConfiguration(); //
		// create
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

		// PHASE-1
		if (success == 0) {

			// PAGE_COUNT_PATH = args[1];

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

		// PHASE-2
		if (success == 0) {

			for (int i = 0; i < 10; i++) {

				Job pageRankComputeJob = Job.getInstance(getConf(), "pageRankComputeJob");

				Configuration conf = pageRankComputeJob.getConfiguration();
				// conf.set("PAGE_COUNT_PATH", args[1]);

				pageRankComputeJob.setJarByClass(this.getClass());

				pageRankComputeJob.setMapperClass(MapPageRankCompute.class);

				pageRankComputeJob.setReducerClass(ReducePageRankCompute.class);

				// job2.setInputFormatClass(KeyValueTextInputFormat.class);

				if (i == 0) {
					FileInputFormat.addInputPath(pageRankComputeJob, new Path(args[2]));
					FileOutputFormat.setOutputPath(pageRankComputeJob, new Path(args[3]));
				} else {
					FileSystem filesystem = FileSystem.get(conf);
					String args3PathCopy = args[3] + "copy";

					// check if copy exists in filesystem before creating it
					if (filesystem.exists(new Path(args3PathCopy)))
						filesystem.delete(new Path(args3PathCopy), true);

					// create a copy of the output folder: args[3], delete old
					// copy and use the new copy as input.
					FileUtil.copy(filesystem, new Path(args[3]), filesystem, new Path(args3PathCopy), false, conf);
					filesystem.delete(new Path(args[3]), true);

					FileInputFormat.addInputPath(pageRankComputeJob, new Path(args3PathCopy));
					FileOutputFormat.setOutputPath(pageRankComputeJob, new Path(args[3]));
				}
				// Explicitly set key and value types of map and reduce output
				pageRankComputeJob.setOutputKeyClass(Text.class);
				pageRankComputeJob.setOutputValueClass(Text.class);
				pageRankComputeJob.setMapOutputKeyClass(Text.class);
				pageRankComputeJob.setMapOutputValueClass(Text.class);

				success = pageRankComputeJob.waitForCompletion(true) ? 0 : 1;
			}
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
			String line = lineText.toString().trim(); // todo : do i need trim?
			String mainURL = "";

			// Read pageCount
			FileSystem fs = FileSystem.get(context.getConfiguration());
			Path path = null;
			RemoteIterator<LocatedFileStatus> fileStatusListIterator = fs
					.listFiles(new Path(context.getConfiguration().get("PAGE_COUNT_PATH")), true);
			while (fileStatusListIterator.hasNext()) {
				LocatedFileStatus fileStatus = fileStatusListIterator.next();
				path = new Path(fileStatus.getPath().toString());
			}
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
			String pageCount;
			pageCount = br.readLine().trim();
			if (pageCount != null) {
				PRinit = 1 / Integer.parseInt(pageCount);
			}

			// get page title from line
			Pattern pattern0 = Pattern.compile("<title>(.*?)</title>");
			java.util.regex.Matcher matcher0 = pattern0.matcher(line);
			while (matcher0.find()) {
				mainURL = matcher0.group(1);
			}

			// get outlinks
			Pattern pattern = Pattern.compile("<text xml:space=\"preserve\">(.*?)</text>");
			java.util.regex.Matcher matcher = pattern.matcher(line);
			String prAndOutlinkList = Double.toString(PRinit) + ",,,,,";
			while (matcher.find()) {
				String str1 = matcher.group(1);
				Pattern pattern1 = Pattern.compile("\\[\\[(.*?)\\]\\]");
				java.util.regex.Matcher matcher1 = pattern1.matcher(str1);
				while (matcher1.find()) {
					String url = matcher1.group(1).replace("[[", "").replace("]]", "");
					prAndOutlinkList += url + OUTLINK_LIST_DELIMITER;
				}
			}

			if (!mainURL.isEmpty() && !prAndOutlinkList.isEmpty())
				context.write(new Text(mainURL), new Text(prAndOutlinkList));
		}
	}

	public static class ReduceLinkGraph extends Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text url, Iterable<Text> prAndOutlinkList, Context context)
				throws IOException, InterruptedException {
			for (Text text : prAndOutlinkList) {
				context.write(new Text(url.toString() + URL_SPLIT_DELIMITER), text);
			}
		}
	}

	public static class MapPageRankCompute extends Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {
			String[] urlPRAndOutlinkList = lineText.toString().split(URL_SPLIT_DELIMITER);
			String url = urlPRAndOutlinkList[0];
			String prAndOutlinkList = urlPRAndOutlinkList[1].trim();

			String[] prAndOutlinkArray = prAndOutlinkList.split(PR_DELIMITER);
			String pr = prAndOutlinkArray[0];
			String outLinkSet = prAndOutlinkArray[1];

			String[] outLinks = outLinkSet.split(OUTLINK_LIST_DELIMITER);

			for (String outlink : outLinks) {
				if (!outlink.isEmpty())
					context.write(new Text(url), new Text(String.valueOf(Double.valueOf(pr) / outLinks.length)));
			}
			context.write(new Text(url), new Text(prAndOutlinkList));
		}
	}

	public static class ReducePageRankCompute extends Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text url, Iterable<Text> iterable, Context context)
				throws IOException, InterruptedException {

			ArrayList<Text> iterableCopy = new ArrayList<>();
			String prAndOutlinkList = "";
			String outlinkList = "";
			double pRNew = 0;
			for (Text content : iterable) {
				iterableCopy.add(content);
			}

			for (int i = 0; i < iterableCopy.size(); i++) {
				// check if value is prAndURlist
				String str = iterableCopy.get(i).toString();
				if (str.contains(PR_DELIMITER))
					prAndOutlinkList = str; // should happen only once
				else
					pRNew += Double.valueOf(str);
			}

			if (!prAndOutlinkList.isEmpty()) {
				String[] linkGraphArray = prAndOutlinkList.split(PR_DELIMITER);
				outlinkList = linkGraphArray[1];
			}

			double pRNewDamped = (1 - 0.85) + 0.85 * pRNew;

			context.write(url, new Text(String.valueOf(pRNewDamped) + PR_DELIMITER + outlinkList));
		}
	}
}