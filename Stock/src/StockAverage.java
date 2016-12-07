

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.NetworkInterface;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MultiFileInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class StockAverage {
	
	

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text companySym = new Text();
    private final static SimpleDateFormat format = new SimpleDateFormat("yyyy-");
    Calendar cal = Calendar.getInstance();
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	
    	
		
      
    	String data[]=value.toString().split(",");
    	
    	if(data.length>5)
    	{
    	if(!data[4].trim().equals("trip_distance"))
    	{
    		String test=data[1].trim();
    		//System.out.println("data is :"+test);
    		if(test.length()>2)
    		{
    		String	test1=test.replaceAll("-", "/");
    	
        	Date date= new Date(test1);
    		cal.setTime(date);
    		int day = cal.get(Calendar.DAY_OF_WEEK);
			
			Text key1 = new Text(String.valueOf(day));
			
			FloatWritable price=new FloatWritable(Float.parseFloat(data[18].trim()));
	    	
	    	context.write(key1, one);
    		
    		}
			
	
    	
    	
    	
    	}
    	}
        
      }
    }
  
  public static class hourMapper
  extends Mapper<Object, Text, Text, IntWritable>{

	  
private final static IntWritable one = new IntWritable(1);
private Text companySym = new Text();
private final static SimpleDateFormat format = new SimpleDateFormat("yyyy-");
Calendar cal = Calendar.getInstance();
public void map(Object key, Text value, Context context
               ) throws IOException, InterruptedException {
	
	Configuration conf=context.getConfiguration();
	int highestDay=Integer.parseInt(conf.get("highest"));
	
	
	
 
	String data[]=value.toString().split(",");
	
	if(data.length>5)
	{
	if(!data[4].trim().equals("trip_distance"))
	{
		String test=data[1].trim();
		//System.out.println("data is :"+test);
		if(test.length()>2)
		{
		String	test1=test.replaceAll("-", "/");
	
   	Date date= new Date(test1);
		cal.setTime(date);
		int day = cal.get(Calendar.DAY_OF_WEEK);
		int hour=cal.get(Calendar.HOUR);
		
		
		if(day==highestDay)
		{
		
		Text key1 = new Text(String.valueOf(hour));
		
		
   	context.write(key1, one);
		}
		
		}
		

	
	
	
	}
	}
   
 }
}
  
  public static class hourSumReducer
  extends Reducer<Text,IntWritable,Text,IntWritable> {
private FloatWritable result = new FloatWritable();
public TreeMap<Integer, Integer> topValue=new TreeMap<Integer, Integer>();


public void reduce(Text key, Iterable<IntWritable> values,
                  Context context
                  ) throws IOException, InterruptedException {
 float sum = 0;
 int count =0;
 for (IntWritable val : values) {
  
   count++;
 }
 topValue.put(new Integer(key.toString()), count);
 if(topValue.size()>10)
 {
	  topValue.remove(topValue.firstKey());
 }
 


 
}

public void cleanup(Context context) throws IOException, InterruptedException
{
	for(Integer value:topValue.descendingMap().keySet())
	{
		context.write(new Text(value.toString()),new IntWritable(topValue.get(value)));
	}
}










}
  

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private FloatWritable result = new FloatWritable();
    public TreeMap<Integer, Integer> topValue=new TreeMap<Integer, Integer>();
    

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      float sum = 0;
      int count =0;
      for (IntWritable val : values) {
       
        count++;
      }
      topValue.put(new Integer(key.toString()), count);
      if(topValue.size()>1)
      {
    	  topValue.remove(topValue.firstKey());
      }
      
     
     
      
    }
    
    public void cleanup(Context context) throws IOException, InterruptedException
    {
    	for(Integer value:topValue.descendingMap().keySet())
    	{
    		context.write(new Text(value.toString()),new IntWritable(1));
    	}
    }
    
    
    
 
    

    
   
    
    
  }
  
  
  
  

  public static void main(String[] args) throws Exception {
   

	  Configuration conf = new Configuration();
	  
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(StockAverage.class);
    job.setMapperClass(TokenizerMapper.class);
    //job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
   job.waitForCompletion(true);
   
  
   try{
       Path pt=new Path("hdfs://localhost:54310"+args[1]+"/part-r-00000");
       FileSystem fs = FileSystem.get(new Configuration());
       BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
       String line;
       System.out.println("hiiii");
       line=br.readLine();
       String [] data = line.split("\t");
       String day=data[0];
       System.out.println(day);
       
       conf.set("highest", day);
      
       
       Job job1 = Job.getInstance(conf, "word count");
       job1.setJarByClass(StockAverage.class);
       job1.setMapperClass(hourMapper.class);
       //job.setCombinerClass(IntSumReducer.class);
       job1.setReducerClass(hourSumReducer.class);
       job1.setOutputKeyClass(Text.class);
       job1.setOutputValueClass(IntWritable.class);
       FileInputFormat.addInputPath(job1, new Path(args[0]));
       FileOutputFormat.setOutputPath(job1, new Path(args[2]));
       job1.waitForCompletion(true);
       
      
}catch(Exception e){
	
	System.out.println(e);
}
    
    
  }

}

