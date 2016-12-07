package lab.secondary;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ReduceSideJoin extends Configured implements Tool {
	final static String otherThanQuote = " [^\"] ";
	final static String quotedString = String.format(" \" %s* \" ", otherThanQuote);
	final static String regex = String.format(
			"(?x) " + // enable comments, ignore white spaces
					",                         " + // match a comma
					"(?=                       " + // start positive look ahead
					"  (                       " + // start group 1
					"    %s*                   " + // match 'otherThanQuote'
													// zero or more times
					"    %s                    " + // match 'quotedString'
					"  )*                      " + // end group 1 and repeat it
													// zero or more times
					"  %s*                     " + // match 'otherThanQuote'
					"  $                       " + // match the end of the
													// string
					")                         ", // stop positive look ahead
			otherThanQuote, quotedString, otherThanQuote);

	public static class UserJoinMapper extends Mapper<Object, Text, Text, Text> {
		private Text outkey = new Text();
		private Text outvalue = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			// Parse the input string into a nice map
			String[] separatedInput = value.toString().split("\\t");
			// String[] tagTokens = separatedInput[5].split(",");
			String userId = separatedInput[0];
			if (userId == null) {
				return;
			}
			// The foreign join key is the user ID
			outkey.set(userId);
			// Flag this record for the reducer and then output
			outvalue.set("A" + value.toString());
			context.write(outkey, outvalue);
		}
	}

	public static class CommentJoinMapper extends Mapper<Object, Text, Text, Text> {
		private Text outkey = new Text();
		private Text outvalue = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] separatedInput = value.toString().split("\\t");
			// String[] tagTokens = separatedInput[5].split(",");
			String userId = separatedInput[0];
			//System.out.println("B" + userId);
			if (userId == null) {
				return;
			}
			// The foreign join key is the user ID
			outkey.set(userId);
			// Flag this record for the reducer and then output
			outvalue.set("B" + value.toString());
			context.write(outkey, outvalue);
		}
	}

	public static class UserJoinReducer extends Reducer<Text, Text, Text, Text> {
		private static final Text EMPTY_TEXT = new Text("");
		private Text tmp = new Text();
		private ArrayList<Text> listA = new ArrayList<Text>();
		private ArrayList<Text> listB = new ArrayList<Text>();
		private String joinType = null;

		public void setup(Context context) {
			// Get the type of join from our configuration
			joinType = context.getConfiguration().get("join.type");
		}

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			// Clear our lists
			listA.clear();
			listB.clear();
			// iterate through all our values, binning each record based on what
			// it was tagged with. Make sure to remove the tag!
			while (values.iterator().hasNext()) {
				tmp = values.iterator().next();
				System.out.println(Character.toString((char) tmp.charAt(0)));
				if (Character.toString((char) tmp.charAt(0)).equals("A")) {

					System.out.println("here4");
					listA.add(new Text(tmp.toString().substring(1)));
				}
				if (Character.toString((char) tmp.charAt(0)).equals("B")) {
					System.out.println("here5");
					listB.add(new Text(tmp.toString().substring(1)));
				}
				System.out.println(tmp);
			}
			// Execute our join logic now that the lists are filled

			System.out.println("Size is "+listB.size());
			executeJoinLogic(context);
		}

		private void executeJoinLogic(Context context) throws IOException, InterruptedException {

			if (joinType.equalsIgnoreCase("inner")) {
				// If both lists are not empty, join A with B
				System.out.println("here3");
                                System.out.println(listA.size()+" and "+listB.size());
				if (!listA.isEmpty() && !listB.isEmpty()) {
					System.out.println("avee");
					for (Text A : listA) {
						//System.out.println("here1");
						for (Text B : listB) {
							//System.out.println("here2");
							context.write(A, B);
						}
					}
				}
			} else if (joinType.equalsIgnoreCase("leftouter")) {
				// For each entry in A,
				for (Text A : listA) {
					// If list B is not empty, join A and B
					if (!listB.isEmpty()) {
						for (Text B : listB) {
							context.write(A, B);
						}
					} else {
						// Else, output A by itself
						context.write(A, EMPTY_TEXT);
					}
				}
			} else if (joinType.equalsIgnoreCase("rightouter")) {
				// For each entry in B,
				for (Text B : listB) {
					// If list A is not empty, join A and B
					if (!listA.isEmpty()) {
						for (Text A : listA) {
							context.write(A, B);
						}
					} else {
						// Else, output B by itself
						context.write(EMPTY_TEXT, B);
					}
				}
			} else if (joinType.equalsIgnoreCase("fullouter")) {
				// If list A is not empty
				if (!listA.isEmpty()) {
					// For each entry in A
					for (Text A : listA) {
						// If list B is not empty, join A with B
						if (!listB.isEmpty()) {
							for (Text B : listB) {
								context.write(A, B);
							}
						} else {
							// Else, output A by itself
							context.write(A, EMPTY_TEXT);
						}
					}
				} else {
					// If list A is empty, just output B
					for (Text B : listB) {
						context.write(EMPTY_TEXT, B);
					}
				}
			} else if (joinType.equalsIgnoreCase("anti")) {
				// If list A is empty and B is empty or vice versa
				if (listA.isEmpty() ^ listB.isEmpty()) {
					// Iterate both A and B with null values
					// The previous XOR check will make sure exactly one of
					// these lists is empty and therefore the list will be
					// skipped
					for (Text A : listA) {
						context.write(A, EMPTY_TEXT);
					}
					for (Text B : listB) {
						context.write(EMPTY_TEXT, B);
					}
				}
			}
		}
	}

	public static void main(String[] args) throws Exception {
		System.exit(new ReduceSideJoin().run(args));
	}

	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		GenericOptionsParser parser = new GenericOptionsParser(conf, args);
		String[] otherArgs = parser.getRemainingArgs();

		Job job = new Job(conf, "ReduceSideJoin");
		job.setJarByClass(ReduceSideJoin.class);

		// Use MultipleInputs to set which input uses what mapper
		// This will keep parsing of each data set separate from a logical
		// standpoint
		// The first two elements of the args array are the two inputs
		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, UserJoinMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, CommentJoinMapper.class);
		job.getConfiguration().set("join.type", "inner");
		//job.setNumReduceTasks(0);
		job.setReducerClass(UserJoinReducer.class);

		job.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(job, new Path(args[2]));

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		return job.waitForCompletion(true) ? 0 : 2;
	}

	private void printUsage() {
		System.err.println("Usage: ReduceSideJoin <user_in> <comments_in> <join_type> <out>");
		ToolRunner.printGenericCommandUsage(System.err);
		System.exit(2);
	}

}