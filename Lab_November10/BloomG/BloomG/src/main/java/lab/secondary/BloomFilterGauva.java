package lab.secondary;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.google.common.base.Charsets;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import com.google.common.hash.Sink;
import java.util.Calendar;
import java.util.Date;
import java.util.TreeMap;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class BloomFilterGauva extends Configured implements Tool {
	public static class BloomFilterMapper extends Mapper<Object, Text, Text, NullWritable> {
		Funnel<Person> p = new Funnel<Person>() {

			public void funnel(Person person, Sink into) {
				// TODO Auto-generated method stub
				into.putInt(person.id);}

		};
		private BloomFilter<Person> friends = BloomFilter.create(p, 500, 0.1);

		@Override
		public void setup(Context context) throws IOException, InterruptedException {

			Person p1 = new Person(7,21);
                        Person p2 = new Person(7,22);
                        Person p3 = new Person(7,23);
			
			ArrayList<Person> friendsList = new ArrayList<Person>();
			friendsList.add(p1);
			friendsList.add(p2);
                        friendsList.add(p3);

			for (Person pr : friendsList) {
				friends.put(pr);
			}
		}
Calendar cal = Calendar.getInstance();
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String values[] = value.toString().split(",");
                        String test=values[1].trim();
    		//System.out.println("data is :"+test);
                String a="";
    		try{
                    
	if(values.length>5)
	{
	if(!values[4].trim().equals("trip_distance"))
	{
                if(test.length()>2)
    		{
    		String	test1=test.replaceAll("-", "/");
                          a=test1;
        	           Date date= new Date(test1);
    		cal.setTime(date);
    		int day = cal.get(Calendar.DAY_OF_WEEK);
                int time = cal.get(Calendar.HOUR_OF_DAY);
			
                        
			Person p = new Person(day,time);
			if (friends.mightContain(p)) {
                         
				context.write(value, NullWritable.get());
           
		}
        
       

	} }}
                }
                catch(Exception e)
                {
                    System.out.println("Aveeeeeee");
                     System.out.println(a);
                }
                }
                
        }

       public static class TokenizerMapper
       extends Mapper<LongWritable, Text, Text, IntWritable>{

	  // Reusable IntWritable for the count
	  private static final IntWritable one = new IntWritable(1);
	  

    public void map(LongWritable key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	
    String data[]=	value.toString().split(",");
    
    
    
    if(data.length>5)
	{
	if(!data[4].trim().equals("trip_distance"))
	{
		if(data[10].length()>8 && data[9].length()>8 )
		{
			
		String dataLat=data[10].trim().substring(0, 7);
		String dataLon=data[9].trim().substring(0,7);
                String key1 =dataLat+","+dataLon;
    
    	
    	 
 	    	context.write(new Text(key1), one);
 	     
 	    
 	    
 	  
      }
    }
        }
    }
       }
  

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();
    public TreeMap<String, Integer> topValue=new TreeMap<String, Integer>();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
    	int count=0;
    
     for(IntWritable value : values)
     {
    	count++; 
     }
     
     context.write(key,new IntWritable(count));
 	
 
  }
  }  
        
  public static class TopMapper
    extends Mapper<LongWritable, Text, NullWritable, Text>{

	  // Reusable IntWritable for the count

    	public TreeMap<Integer, Text> topValue=new TreeMap<Integer, Text>();
   

 public void map(LongWritable key, Text value, Context context
                 ) throws IOException, InterruptedException {
 	
 String data[]=	value.toString().split("\t");
 String count =data[1];

 topValue.put(Integer.parseInt(count), new Text(value));
 
 if(topValue.size()>5)
 {
	 
	  topValue.remove(topValue.firstKey());
 }
 
 
  
	  
   }
 
 public void cleanup(Context context) throws IOException, InterruptedException
 {
 	for(Text value:topValue.values())
 	{
 		//System.out.println("data"+topValue.get(topValue.firstKey()));
 		context.write(NullWritable.get(),value);
 	}
 }
 } 
  
  
   public static class TopReducer
    extends Reducer<NullWritable,Text,NullWritable,Text> {
 private IntWritable result = new IntWritable();
 public TreeMap<Integer, Text> topValue=new TreeMap<Integer, Text>();

 public void reduce(NullWritable key, Iterable<Text> values,
                    Context context
                    ) throws IOException, InterruptedException {
	 
	 for(Text value:values)
	 {
		// System.out.println(value.toString());
	 String data[]=	value.toString().split("\t");
	 String count =data[1];
	 topValue.put(Integer.parseInt(count), new Text(value));
	 
	 if(topValue.size()>5)
	 {
		  topValue.remove(topValue.firstKey());
	 }
	 }
	 for(Text value:topValue.values())
	 	{
	 		//System.out.println("r"+topValue.get(topValue.firstKey()));
	 		context.write(NullWritable.get(),value);
	 	}
}
    
    

  
    }
  
        
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new BloomFilterGauva(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Bloom Filter");
		job.setJarByClass(BloomFilterGauva.class);
		job.setMapperClass(BloomFilterMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setNumReduceTasks(0);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job,
				new Path(args[1]));
		job.waitForCompletion(true);
		
                
                Job job2 = Job.getInstance(conf, "word count");
    job2.setJarByClass(BloomFilterGauva.class);
    job2.setMapperClass(TokenizerMapper.class);
    //job.setCombinerClass(IntSumReducer.class);
    job2.setReducerClass(IntSumReducer.class);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(IntWritable.class);
    org.apache.hadoop.mapreduce.lib.input.TextInputFormat.addInputPath(job2, new Path(args[1]));
    FileOutputFormat.setOutputPath(job2, new Path(args[2]));
    job2.waitForCompletion(true);
    
    Job job1 = Job.getInstance(conf, "word count");
    job1.setJarByClass(BloomFilterGauva.class);
    job1.setMapperClass(TopMapper.class);
    //job.setCombinerClass(IntSumReducer.class);
    job1.setReducerClass(TopReducer.class);
    job1.setNumReduceTasks(1);
    job1.setOutputKeyClass(NullWritable.class);
    job1.setOutputValueClass(Text.class);
    org.apache.hadoop.mapreduce.lib.input.TextInputFormat.addInputPath(job1, new Path(args[2]));
    FileOutputFormat.setOutputPath(job1, new Path(args[3]));
    job1.waitForCompletion(true);
    return 1;
                
	}
}
