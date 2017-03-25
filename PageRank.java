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

	public static final Logger LOG = Logger.getLogger(PageRank.class); //logger object for logging the MR jobs
	public static final String PR_DELIMITER = ",,,,,"; //delimiter for separating the page rank
	public static final String OUTLINK_LIST_DELIMITER = "#####"; //delimiter for separating the outlinks

/*
		Main method that runs the page rank class and calls the run() method which contains the 4 jobs chained one after the other.
*/
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new PageRank(), args); 
		System.exit(res);
	}

/*
		Driver method which contains the 4 jobs chained one after the other:
		- pageCountjob (initial job which counts the number of pages in the input)
		- linkGraphJob (job which generates link graph - URL/page title and its outlinks)
		- pageRankComputeJob (job which performs actual page rank computations taking initial page rank as 1/(number of pages))
		- pageRankSortJob (job which performs sorting of computed page ranks from previous job)
*/
	public int run(String[] args) throws Exception {

		Configuration conf1 = new Configuration(); //instantiate the configuration object for the first job

		/*create a name for the intermediate directory which will store the output of all intermediate processes
		this is the PARENT intermediate directory.
		You can find this under /user/username of HDFS
		Example: $hadoop fs -ls /user/vp/intermediate_dir
		*/
		String intermediate_directory = "intermediate_dir";
		Path output_directory = new Path(args[1]); //output directory which will store the final sorted ranks o/p
		Path intermediate_path = new Path(intermediate_directory); //intermediate directory which will store the output of intermediate processes

		FileSystem filesystem = FileSystem.get(conf1); //instantiate filesystem from current config object

		//check for existing intermediate and output dirs and delete them automatically.
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

		//create intermediate dir to store output of numberoflines or page count 
		//under the main intermediate_path
		Path page_count = new Path(intermediate_path, "page_count");
		//instantiate first page count job
		Job pageCountjob = Job.getInstance(conf1, " pageCountjob ");

		//set jars and corresponding classes
		pageCountjob.setJarByClass(this.getClass());
		pageCountjob.setMapperClass(MapPageCount.class);
		pageCountjob.setReducerClass(ReducePageCount.class);

		//set input path - the one passed as the first argument in the main "hadoop jar" execution command.
		FileInputFormat.addInputPaths(pageCountjob, args[0]);
		//set output path to page_count : a dir present under the intermediate dir created earlier which stores the page count o/p
		FileOutputFormat.setOutputPath(pageCountjob, page_count);

		// Explicitly set key and value types of reduce output
		pageCountjob.setOutputKeyClass(Text.class);
		pageCountjob.setOutputValueClass(IntWritable.class);

		//variable to store completion status of pageCountjob
		int success = pageCountjob.waitForCompletion(true) ? 0 : 1;

		System.out.println("=====PAGE COUNT COMPLETE=====");


		/*
		PHASE-1
		First check if the pageCountJob was a success and then proceed to creation of linkGraphJob.
		linkGraphJob  = (job which generates link graph - URL/page title and its outlinks)
		*/
		if (success == 0) {

			System.out.println("=====BEGIN GENERATION OF LINK GRAPH=====");

			//instantiate second linkGraphJob
			Job linkGraphJob = Job.getInstance(getConf(), "linkGraphJob");

			//get the configuration object for the second job from the job
			Configuration conf2 = linkGraphJob.getConfiguration();

			//create intermediate dir to store output of job, stores link graphs of all pages/urls.
			///under the main/parent intermediate_path
			Path link_graph = new Path(intermediate_path, "link_graph");

			//variable used to read the page count written to the page_count intermediate directory
			String line;

			 //variable used to store the page count written to the page_count intermediate directory
			int numOfLines = 1;

/*
Read, parse and store the page count written by the previous job to be used later
*/
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

			/*pass the numOfLines which is the pagecount or the number of lines read from the input file
			using the configuration object to be obtained later in the second / Linkgraph MR job.
			*/
			conf2.set("numOfLines", String.valueOf(numOfLines));

			//set jar, mapper and reducers
			linkGraphJob.setJarByClass(this.getClass());
			linkGraphJob.setMapperClass(MapLinkGraph.class);
			linkGraphJob.setReducerClass(ReduceLinkGraph.class);

			//set input path - the one passed as the first argument in the main "hadoop jar" execution command.
			FileInputFormat.addInputPaths(linkGraphJob, args[0]);

			//set output path to link_graph : a dir present under the intermediate dir 
			// created earlier which stores the link graph o/p of pages
			FileOutputFormat.setOutputPath(linkGraphJob, link_graph);

			// Explicitly set key and value types of map and reduce output
			linkGraphJob.setOutputKeyClass(Text.class);
			linkGraphJob.setOutputValueClass(Text.class);
			linkGraphJob.setMapOutputKeyClass(Text.class);
			linkGraphJob.setMapOutputValueClass(Text.class);

			//set variable according to job completion status
			success = linkGraphJob.waitForCompletion(true) ? 0 : 1;

		/*
		PHASE-2
		First check if the linkGraphJob was a success and then proceed to creation of pageRankComputeJob.
		pageRankComputeJob = (job which performs actual page rank computations taking initial page rank as 1/(number of pages))
		*/
			if (success == 0) {

				System.out.println("=====BEGIN PAGERANK COMPUTATION=====");

				/* Iteratively compute the page rank of the given pages for 10 iterations as required.
				*/
				for (int i = 1; i < 11; i++) {
					System.out.println("=========================Iteration number " + i + "=========================");

					//instantiate third pageRankComputeJob
					Job pageRankComputeJob = Job.getInstance(getConf(), "pageRankComputeJob");

					Configuration conf4 = pageRankComputeJob.getConfiguration();
					//set jar, mappers and reducers
					pageRankComputeJob.setJarByClass(this.getClass());
					pageRankComputeJob.setMapperClass(MapPageRankCompute.class);
					pageRankComputeJob.setReducerClass(ReducePageRankCompute.class);

					/*create intermediate dir to store output of pageRankComputeJob, which is a list of page ranks
					under the main/parent intermediate_path.
					We have one for each iteration to track the intermediate data.
					Example: /user/myname/intermediate_dir/iter1 for the first iteration
					*/
					Path intermediate_file_path = new Path(intermediate_path, "iter" + i);
					
					Path old_intermediate_file_path = new Path(intermediate_path, "iter" + String.valueOf(i - 2));


					//set input path which is the output of the linkGraphJob, containing all link graphs needed
					//to compute page rank.
					FileInputFormat.addInputPath(pageRankComputeJob, link_graph);

					//set output path which is an intermediate directory : intermediate_file_path defined above
					FileOutputFormat.setOutputPath(pageRankComputeJob, intermediate_file_path);

					FileSystem fs4 = FileSystem.get(conf4); //instantiate filesystem from current config object

					//check for existing intermediate and output dirs and delete them automatically.
					try {
						if (fs4.exists(old_intermediate_file_path)) {
							fs4.delete(old_intermediate_file_path, true);
						}
					} catch (IOException e) {
						e.printStackTrace();
					}

					//set format classes, basically to use the "URL" or page title as an offset instead of an actual offset
					//which is a wasted variable and is never used.
					pageRankComputeJob.setInputFormatClass(KeyValueTextInputFormat.class);
					pageRankComputeJob.setOutputFormatClass(TextOutputFormat.class);

					//set reducer output key and value classes
					pageRankComputeJob.setOutputKeyClass(Text.class);
					pageRankComputeJob.setOutputValueClass(Text.class);

					//set variable based on job completion status
					success = pageRankComputeJob.waitForCompletion(true) ? 0 : 1;

					//set the intermediate dir, example: iter1 as the input for the next iteration, say iter2.
					link_graph = intermediate_file_path;
				}
			}

		/*
		PHASE-3
		First check if the pageRankComputeJob was a success and then proceed to creation of pageRankSortJob.
		pageRankSortJob  = (job which performs sorting of computed page ranks from previous job)
		*/
			if (success == 0) {

				System.out.println("=====BEGIN SORT=====");

				//instantiate fourth pageRankSortJob
				Job pageRankSortJob = Job.getInstance(getConf(), "pageRankSortJob");

				Configuration conf5 = pageRankSortJob.getConfiguration();

				//set jar, mappers, reducers
				pageRankSortJob.setJarByClass(this.getClass());
				pageRankSortJob.setMapperClass(MapPageRankSort.class);
				pageRankSortJob.setReducerClass(ReducePageRankSort.class);


				//set input path which is the output of the last iteration, containing all computed page ranks needed
				// of all pages used to do sorting.
				// link_graph variable is set to the last iteration o/p dir in the previous job
				FileInputFormat.addInputPath(pageRankSortJob, link_graph);

				//set output path to the path passed as the second argument in the "hadoop jar" command
				FileOutputFormat.setOutputPath(pageRankSortJob, output_directory);

				//restrict reducers to one, which is the only way to get sorting done right.
				pageRankSortJob.setNumReduceTasks(1);

				Path old_intermediate_file_path = new Path(intermediate_path, "iter9");

				FileSystem fs4 = FileSystem.get(conf5); //instantiate filesystem from current config object

				//check for existing intermediate and output dirs and delete them automatically.
				try {
					if (fs4.exists(old_intermediate_file_path)) {
						fs4.delete(old_intermediate_file_path, true);
					}
				} catch (IOException e) {
					e.printStackTrace();
				}

				//set format classes, basically to use the "URL" or page title as an offset instead of an actual offset
				//which is a wasted variable and is never used.
				pageRankSortJob.setInputFormatClass(KeyValueTextInputFormat.class);
				pageRankSortJob.setOutputFormatClass(TextOutputFormat.class);

				//comparator class setting to use our custom descending sort comparator for page ranking
				pageRankSortJob.setSortComparatorClass(PageRankComparator.class);

				//set map and reduce key value types
				pageRankSortJob.setMapOutputKeyClass(DoubleWritable.class);
				pageRankSortJob.setMapOutputValueClass(Text.class);
				pageRankSortJob.setOutputKeyClass(Text.class);
				pageRankSortJob.setOutputValueClass(DoubleWritable.class);

				//record sorting job completion status
				success = pageRankSortJob.waitForCompletion(true) ? 0 : 1;

			}

		}
		return success;
	}

/*
Mapper class to compute number of pages/links/urls in input.
Use the same key to accumulate count in the reducer
*/
	public static class MapPageCount extends Mapper<LongWritable, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {
			String line = lineText.toString(); //a line is a page or a url/link

			if (!line.isEmpty())
				context.write(word, one);
		}
	}

/*
Reducer class to compute number of pages/links/urls in input.
Accumulate count using the same key in the reducer and write to output
*/
	public static class ReducePageCount extends Reducer<Text, IntWritable, Text, IntWritable> {
		@Override
		public void reduce(Text word, Iterable<IntWritable> counts, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable count : counts) {
				sum += count.get(); // sums up page count
			}
			context.write(new Text("numOfLines"), new IntWritable(sum));
		}
	}

/*
Mapper class to generate link graph for each url
*/
	public static class MapLinkGraph extends Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {

			//get the pagecount set in the config object of the linkGraphJob
			String lineCount = context.getConfiguration().get("numOfLines");
			//find initial pagerank to be used in the first iteration
			double PRinit = 1 / Double.parseDouble(lineCount);
			String line = lineText.toString().trim(); //get the page/line
			String mainURL = "";

			try {
				// get page title from line
				Pattern pattern0 = Pattern.compile("<title>(.*?)</title>");
				java.util.regex.Matcher matcher0 = pattern0.matcher(line);
				while (matcher0.find()) {
					mainURL = matcher0.group(1); //get the URL from the page title tags
				}

				// get outlinks from the line
				Pattern pattern = Pattern.compile("<text(.*?)</text>");
				java.util.regex.Matcher matcher = pattern.matcher(line);

				//create a string containing the initial page rank and the outlink list
				//for the URL found in title
				String prAndOutlinkList = Double.toString(PRinit) + ",,,,,";
				while (matcher.find()) {
					String str1 = matcher.group(1);
					Pattern pattern1 = Pattern.compile("\\[\\[(.*?)\\]\\]");
					java.util.regex.Matcher matcher1 = pattern1.matcher(str1);
					while (matcher1.find()) {
						String url = matcher1.group(1).replace("[[", "").replace("]]", "");

						//concatenate outlinks with a delimiter
						prAndOutlinkList += url + OUTLINK_LIST_DELIMITER;
					}
				}

				//write the URL and a concatenation of initial pagerank and the URL's outlinks
				if (!mainURL.isEmpty() && !prAndOutlinkList.isEmpty())
					context.write(new Text(mainURL), new Text(prAndOutlinkList));
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

/*
Reducer class for the linkGraphJob.
Does nothing, just lets the mapper output pass through
*/
	public static class ReduceLinkGraph extends Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text url, Iterable<Text> prAndOutlinkList, Context context)
				throws IOException, InterruptedException {
			for (Text text : prAndOutlinkList) {
				context.write(url, text);
			}
		}
	}

	/*
Mapper class to calculate page rank for each url
The "offset" here is the URL passed from the previous job or iteration, 
the lineText is the concatenation of pagerank and the URL's outlinks
	*/
	public static class MapPageRankCompute extends Mapper<Text, Text, Text, Text> {

		public void map(Text offset, Text lineText, Context context) throws IOException, InterruptedException {

			//get the concatenation of pagerank and the URL's outlinks
			//and separate them.
			String[] prAndOutlinkArray = lineText.toString().split(PR_DELIMITER);

			String pr = prAndOutlinkArray[0]; //current pagerank of URL
			String outLinkSet = ""; //outlink set of URL

			//check for array length to prevent ArrayIndexOutOfBounds exceptions.
			if (prAndOutlinkArray.length == 2) {
				outLinkSet = prAndOutlinkArray[1];
			}

			//check for array length to prevent ArrayIndexOutOfBounds exceptions.
			if (outLinkSet.length() > 0) {
				//split the string to get all outlinks.
				String[] outLinks = outLinkSet.split(OUTLINK_LIST_DELIMITER);
				if (outLinks != null && outLinks.length > 0) {
					/*
					for each outlink, output the outlink and the page rank divided by number of outlinks.
					i.e. distribute page rank to all outlinks of the main URL
					*/
					for (String outlink : outLinks) {
						if (!outlink.isEmpty() && !pr.isEmpty())
							context.write(new Text(outlink),
									new Text(String.valueOf(Double.parseDouble(pr) / (double) outLinks.length)));
					}
				}
			}

			//also output the mainURL (which is the offset) from the input and the initial string(lineText) containing
			//the pagerank and the outlink list as is, from the input
			if (!offset.toString().isEmpty() && !lineText.toString().isEmpty())
				context.write(offset, lineText);

		}
	}

	/*
Reducer class to calculate page rank for each url
The "offset" or "url" here is the URL passed from the mapper, 
the iterable has a list of values : 
one of them is a concatenation of pagerank and the URL's outlinks list;
rest of them are multiple values of pagerank/num-of-outlinks for a url/page
	*/
	public static class ReducePageRankCompute extends Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text url, Iterable<Text> iterable, Context context)
				throws IOException, InterruptedException {

			//boolean to check if the input to reducer has the original
			//concatenation of pagerank and the URL's outlinks list
			//initialize to false
			boolean hasOriginalPRAndOutlinkList = false;
			String outlinkList = "";

			//variable to store the sum of all values of pagerank/num-of-outlinks
			// for a url/page
			double pRNew = 0.0;
			//iterate through the iterable
			for (Text content : iterable) {
				String str = content.toString();

			/*check if the input to reducer has the original
			//concatenation of pagerank and the URL's outlinks list
			//if it does, split at the delimiter and get the outlink list
			//discard the page rank and set hasOriginalPRAndOutlinkList to true.
			or else, sum up the values of pagerank/num-of-outlinks for the url/page
			*/
				if (str.contains(PR_DELIMITER)) {
					String[] linkGraphArray = str.split(PR_DELIMITER);

					if (linkGraphArray.length == 2)
						outlinkList = linkGraphArray[1];

					hasOriginalPRAndOutlinkList = true;
				} else {
					pRNew += Double.parseDouble(str);
				}
			}

			//calculate pagerank with damping factor, d = 0.85 and previous undamped page rank
			// per formula : PR = 1-d + d * (sum of the values of pagerank/num-of-outlinks for the url/page)
			double pRNewDamped = (1 - 0.85) + 0.85 * pRNew;

			//write the url, the new calculated pagerank concatenated with delimiter and outlink list
			//to be given as input to the next iteration of this same job
			if (!url.toString().isEmpty() && hasOriginalPRAndOutlinkList)
				context.write(url, new Text(String.valueOf(pRNewDamped) + PR_DELIMITER + outlinkList));
		}
	}

	/*
Mapper class to sort URLs by page rank
The "offset" here is the URL passed from the last iteration of previous job, 
the lineText is the concatenation of pagerank and the URL's outlinks
	*/
	public static class MapPageRankSort extends Mapper<Text, Text, DoubleWritable, Text> {

		public void map(Text url, Text lineText, Context context) throws IOException, InterruptedException {
			/*
			split at the delimiter, discard outlinks, get the pagerank and write to output
			*/
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
			// in case multiple urls have same rank, sort and print
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