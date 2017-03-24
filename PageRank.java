package com.asgn3;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

/*Created by Viseshprasad Rajendraprasad
vrajend1@uncc.edu
*/

public class PageRank extends Configured implements Tool {

	public static final Logger LOG = Logger.getLogger(PageRank.class);
	public static final String PR_DELIMITER = ",,,,,";
	public static final String OUTLINK_LIST_DELIMITER = "#####";

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new PageRank(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {

		Configuration conf1 = new Configuration();

		String intermediate_directory = "intermediate_dir";
		Path output_directory = new Path(args[1]);
		Path intermediate_path = new Path(intermediate_directory);

		FileSystem filesystem = FileSystem.get(conf1);

		try {
			if (filesystem.exists(output_directory)) {
				filesystem.delete(output_directory, true);
			}
			if (filesystem.exists(intermediate_path)) {
				filesystem.delete(intermediate_path, true);
			}
			filesystem.mkdirs(intermediate_path);

		} catch (IOException e) {
			e.printStackTrace();
		}

		Path page_count = new Path(intermediate_path, "page_count");

		Job pageCountjob = Job.getInstance(conf1, " pageCountjob ");

		pageCountjob.setJarByClass(this.getClass());
		pageCountjob.setMapperClass(MapPageCount.class);
		pageCountjob.setReducerClass(ReducePageCount.class);

		FileInputFormat.addInputPaths(pageCountjob, args[0]);
		FileOutputFormat.setOutputPath(pageCountjob, page_count);

		// Explicitly set key and value types of map and reduce output
		pageCountjob.setOutputKeyClass(Text.class);
		pageCountjob.setOutputValueClass(IntWritable.class);

		int success = pageCountjob.waitForCompletion(true) ? 0 : 1;

		System.out.println("=====PAGE COUNT COMPLETE=====");
		// PHASE-1
		if (success == 0) {

			System.out.println("=====BEGIN GENERATION OF LINK GRAPH=====");

			Job linkGraphJob = Job.getInstance(getConf(), "linkGraphJob");

			Configuration conf2 = linkGraphJob.getConfiguration();

			Path link_graph = new Path(intermediate_path, "link_graph");

			String line;
			int numOfLines = 1;

			try {
				FileSystem fs2 = FileSystem.get(conf2);
				Path p = new Path(page_count, "part-r-00000");

				BufferedReader br = new BufferedReader(new InputStreamReader(fs2.open(p)));
				while ((line = br.readLine()) != null) {
					if (line.trim().length() > 0) {
						System.out.println(line);
						String[] parts = line.split("\\s+");
						numOfLines = Integer.parseInt(parts[1]);
					}
				}
				br.close();
			} catch (Exception e) {
				e.printStackTrace();
			}

			conf2.set("numOfLines", String.valueOf(numOfLines));

			linkGraphJob.setJarByClass(this.getClass());

			linkGraphJob.setMapperClass(MapLinkGraph.class);

			linkGraphJob.setReducerClass(ReduceLinkGraph.class);

			// job2.setInputFormatClass(KeyValueTextInputFormat.class);

			FileInputFormat.addInputPaths(linkGraphJob, args[0]);

			FileOutputFormat.setOutputPath(linkGraphJob, link_graph);

			// Explicitly set key and value types of map and reduce output
			linkGraphJob.setOutputKeyClass(Text.class);
			linkGraphJob.setOutputValueClass(Text.class);
			linkGraphJob.setMapOutputKeyClass(Text.class);
			linkGraphJob.setMapOutputValueClass(Text.class);

			success = linkGraphJob.waitForCompletion(true) ? 0 : 1;

			// PHASE-2
			if (success == 0) {

				System.out.println("=====BEGIN PAGERANK COMPUTATION=====");

				for (int i = 1; i < 11; i++) {

					System.out.println("=========================Iteration number " + i + "=========================");
					Job pageRankComputeJob = Job.getInstance(getConf(), "pageRankComputeJob");

					// Configuration conf3 =
					// pageRankComputeJob.getConfiguration();

					pageRankComputeJob.setJarByClass(this.getClass());

					pageRankComputeJob.setMapperClass(MapPageRankCompute.class);

					pageRankComputeJob.setReducerClass(ReducePageRankCompute.class);

					Path intermediate_file_path = new Path(intermediate_path, "iter" + i);

					FileInputFormat.addInputPath(pageRankComputeJob, link_graph);
					FileOutputFormat.setOutputPath(pageRankComputeJob, intermediate_file_path);

					pageRankComputeJob.setInputFormatClass(KeyValueTextInputFormat.class);
					pageRankComputeJob.setOutputFormatClass(TextOutputFormat.class);

					/*
					 * if (i == 0) {
					 * FileInputFormat.addInputPath(pageRankComputeJob, new
					 * Path(args[2]));
					 * FileOutputFormat.setOutputPath(pageRankComputeJob, new
					 * Path(args[3])); } else { FileSystem filesystem =
					 * FileSystem.get(conf); String args3PathCopy = args[3] +
					 * "copy";
					 * 
					 * // check if copy exists in filesystem before creating it
					 * if (filesystem.exists(new Path(args3PathCopy)))
					 * filesystem.delete(new Path(args3PathCopy), true);
					 * 
					 * deleteDirectories(args3PathCopy);
					 * 
					 * // create a copy of the output folder: args[3], delete
					 * old // copy and use the new copy as input.
					 * FileUtil.copy(filesystem, new Path(args[3]), filesystem,
					 * new Path(args3PathCopy), false, conf);
					 * filesystem.delete(new Path(args[3]), true);
					 * 
					 * if(filesystem.exists(new Path(args[3])))
					 * filesystem.rename(new Path(args[3]), new
					 * Path(args3PathCopy));
					 * //copyFilesToAnotherDirectory(args[3], args3PathCopy);
					 * //deleteDirectories(args[3]);
					 */
					// FileInputFormat.addInputPath(pageRankComputeJob, new
					// Path(args3PathCopy));
					// FileOutputFormat.setOutputPath(pageRankComputeJob, new
					// Path(args[3]));
					// }

					// Explicitly set key and value types of map and reduce
					// output
					pageRankComputeJob.setOutputKeyClass(Text.class);
					pageRankComputeJob.setOutputValueClass(Text.class);

					// pageRankComputeJob.setMapOutputKeyClass(Text.class);
					// pageRankComputeJob.setMapOutputValueClass(Text.class);

					success = pageRankComputeJob.waitForCompletion(true) ? 0 : 1;

					link_graph = intermediate_file_path;
				}
				// hdfs.delete(intermediate_path, true);

			}

			if (success == 0) {

				System.out.println("=====BEGIN SORT=====");
				Job pageRankSortJob = Job.getInstance(getConf(), "pageRankSortJob");

				pageRankSortJob.setJarByClass(this.getClass());

				pageRankSortJob.setMapperClass(MapPageRankSort.class);

				pageRankSortJob.setReducerClass(ReducePageRankSort.class);

				FileInputFormat.addInputPath(pageRankSortJob, link_graph);
				FileOutputFormat.setOutputPath(pageRankSortJob, output_directory);
				pageRankSortJob.setNumReduceTasks(1);

				pageRankSortJob.setInputFormatClass(KeyValueTextInputFormat.class);
				pageRankSortJob.setOutputFormatClass(TextOutputFormat.class);

				pageRankSortJob.setSortComparatorClass(PageRankComparator.class);

				pageRankSortJob.setMapOutputKeyClass(DoubleWritable.class);
				pageRankSortJob.setMapOutputValueClass(Text.class);

				pageRankSortJob.setOutputKeyClass(Text.class);
				pageRankSortJob.setOutputValueClass(DoubleWritable.class);

				success = pageRankSortJob.waitForCompletion(true) ? 0 : 1;

			}

		}
		return success;
	}

	public static class MapPageCount extends Mapper<LongWritable, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {
			String line = lineText.toString();

			if (!line.isEmpty())
				context.write(word, one);
		}
	}

	public static class ReducePageCount extends Reducer<Text, IntWritable, Text, IntWritable> {
		@Override
		public void reduce(Text word, Iterable<IntWritable> counts, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable count : counts) {
				sum += count.get(); // sums up word occurrences
			}
			context.write(new Text("numOfLines"), new IntWritable(sum));
		}
	}

	public static class MapLinkGraph extends Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {

			String lineCount = context.getConfiguration().get("numOfLines");
			double PRinit = 1 / Double.parseDouble(lineCount);
			String line = lineText.toString().trim();
			String mainURL = "";

			try {
				// get page title from line
				Pattern pattern0 = Pattern.compile("<title>(.*?)</title>");
				java.util.regex.Matcher matcher0 = pattern0.matcher(line);
				while (matcher0.find()) {
					mainURL = matcher0.group(1);
				}

				// get outlinks
				Pattern pattern = Pattern.compile("<text(.*?)</text>");
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
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public static class ReduceLinkGraph extends Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text url, Iterable<Text> prAndOutlinkList, Context context)
				throws IOException, InterruptedException {
			for (Text text : prAndOutlinkList) {
				context.write(url, text);
			}
		}
	}

	public static class MapPageRankCompute extends Mapper<Text, Text, Text, Text> {

		public void map(Text offset, Text lineText, Context context) throws IOException, InterruptedException {

			String[] prAndOutlinkArray = lineText.toString().split(PR_DELIMITER);

			String pr = prAndOutlinkArray[0];
			String outLinkSet = "";

			if (prAndOutlinkArray.length == 2) {
				outLinkSet = prAndOutlinkArray[1];
			}

			if (outLinkSet.length() > 0) {
				String[] outLinks = outLinkSet.split(OUTLINK_LIST_DELIMITER);
				if (outLinks != null && outLinks.length > 0) {
					for (String outlink : outLinks) {
						if (!outlink.isEmpty() && !pr.isEmpty())
							context.write(new Text(outlink),
									new Text(String.valueOf(Double.parseDouble(pr) / (double) outLinks.length)));
					}
				}
			}

			if (!offset.toString().isEmpty() && !lineText.toString().isEmpty())
				context.write(offset, lineText);

		}
	}

	public static class ReducePageRankCompute extends Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text url, Iterable<Text> iterable, Context context)
				throws IOException, InterruptedException {

			boolean hasOriginalPRAndOutlinkList = false;
			String outlinkList = "";
			double pRNew = 0.0;
			for (Text content : iterable) {
				// check if value is prAndURlist
				String str = content.toString();
				if (str.contains(PR_DELIMITER)) {
					String[] linkGraphArray = str.split(PR_DELIMITER);

					if (linkGraphArray.length == 2)
						outlinkList = linkGraphArray[1];

					hasOriginalPRAndOutlinkList = true;
				} else {
					pRNew += Double.parseDouble(str);
				}
			}

			double pRNewDamped = (1 - 0.85) + 0.85 * pRNew;

			if (!url.toString().isEmpty() && hasOriginalPRAndOutlinkList)
				context.write(url, new Text(String.valueOf(pRNewDamped) + PR_DELIMITER + outlinkList));
		}
	}

	public static class MapPageRankSort extends Mapper<Text, Text, DoubleWritable, Text> {

		public void map(Text url, Text lineText, Context context) throws IOException, InterruptedException {
			String[] temp = lineText.toString().split(PR_DELIMITER);

			// use rank as key and url as value to help sort
			if (temp.length > 0)
				context.write(new DoubleWritable(Double.valueOf(temp[0])), url);

		}
	}

	public static class ReducePageRankSort extends Reducer<DoubleWritable, Text, Text, DoubleWritable> {
		@Override
		public void reduce(DoubleWritable rank, Iterable<Text> urls, Context context)
				throws IOException, InterruptedException {

			// loop through the list of urls for each value
			// in case multiple urls have same rank.
			for (Text url : urls) {
				context.write(url, rank);
			}
		}
	}

	// custom comparator for sorting rank values in descending order
	public static class PageRankComparator extends WritableComparator {

		public PageRankComparator() {
			super();
			// TODO Auto-generated constructor stub
		}

		@Override
		public int compare(byte[] arg0, int arg1, int arg2, byte[] arg3, int arg4, int arg5) {
			// TODO Auto-generated method stub

			double rank1 = WritableComparator.readDouble(arg0, arg1);
			double rank2 = WritableComparator.readDouble(arg3, arg4);
			if (rank1 > rank2) {
				return -1;
			} else if (rank1 < rank2) {
				return 1;
			}
			return 0;
		}
	}

}