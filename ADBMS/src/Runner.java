import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
;

/**
 *
 * @author Avee Arora
 */
public class Runner {
    
    
    private static String readAll(Reader rd) throws IOException {
    StringBuilder sb = new StringBuilder();
    int cp;
    while ((cp = rd.read()) != -1) {
      sb.append((char) cp);
    }
    return sb.toString();
  }

  public static JSONObject readJsonFromUrl(String url) throws IOException, JSONException {
      InputStream is = new URL(url).openStream();
    try {
        BufferedReader rd = new BufferedReader(new InputStreamReader(is, Charset.forName("UTF-8")));
      String jsonText = readAll(rd);
      JSONObject json = new JSONObject(jsonText);
      return json;
    } finally {
      is.close();
    }
  }
  

  
  
  public static class TokenizerMapper
  extends Mapper<Object, Text, IntWritable, IntWritable>{

private final static IntWritable one = new IntWritable(1);
private Text companySym = new Text();
private final static SimpleDateFormat format = new SimpleDateFormat("yyyy-");
Calendar cal = Calendar.getInstance();
public void map(Object key, Text value, Context context
               ) throws IOException, InterruptedException {
	
	Configuration conf=context.getConfiguration();
	String lat=conf.get("lat").substring(0,7);
	String lon=conf.get("lon").substring(0,7);
	
 
	String data[]=value.toString().split(",");
	
	if(data.length>5)
	{
	if(!data[4].trim().equals("trip_distance"))
	{
		if(data[10].length()>8 && data[9].length()>8 )
		{
			
		String dataLat=data[10].trim().substring(0, 7);
		String dataLon=data[9].trim().substring(0,7);
		//System.out.println("data is :"+test);
		if(dataLat.equals(lat)&&dataLon.equals(lon))
		{
			String test=data[1].trim();
			//System.out.println("data is :"+test);
			
			String	test1=test.replaceAll("-", "/");
		
	   	Date date= new Date(test1);
			cal.setTime(date);
			
			int hour=cal.get(Calendar.HOUR);
		
		String keyFinal=lat+","+lon;
	
	
   	
   	context.write(new IntWritable(hour), one);
		
		}
		

	
		}
	
	}
	}
   
 }
}


public static class IntSumReducer
  extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {
private FloatWritable result = new FloatWritable();

public void reduce(IntWritable key, Iterable<IntWritable> values,
                  Context context
                  ) throws IOException, InterruptedException {
	float sum = 0;
	 int count =0;
	 for (IntWritable val : values) {
	  
	   count++;
	 }
	 context.write(key, new IntWritable(count));


}
}





public static void main(String[] args) throws Exception {

	 JSONObject json =readJsonFromUrl("https://maps.googleapis.com/maps/api/geocode/json?address=Times+Square&key=AIzaSyDVa8kWYg1NVX10TYmTRNcdDNmxfq67q5w");
		
	  
	 
	  //JSONObject json = readJsonFromUrl("http://query.yahooapis.com/v1/public/yql?q=select%20*%20from%20yahoo.finance.historicaldata%20where%20symbol%20=%20%22YHOO%22%20and%20startDate%20=%20%222016-01-01%22%20and%20endDate%20=%20%222016-04-05%22&format=json&env=http://datatables.org/alltables.env");
	System.out.println(json.toString());
	JSONArray jar = json.getJSONArray("results");
	String lat =json.getJSONArray("results").getJSONObject(0).getJSONObject("geometry").getJSONObject("location").getString("lat");
	String lon =json.getJSONArray("results").getJSONObject(0).getJSONObject("geometry").getJSONObject("location").getString("lng");
	
	

 Configuration conf = new Configuration();
 
 conf.set("lat", lat);
 conf.set("lon", lon);
 
Job job = Job.getInstance(conf, "word count");
job.setJarByClass(Runner.class);
job.setMapperClass(TokenizerMapper.class);
//job.setCombinerClass(IntSumReducer.class);
job.setReducerClass(IntSumReducer.class);
job.setMapOutputKeyClass(IntWritable.class);
job.setMapOutputValueClass(IntWritable.class);
job.setOutputKeyClass(IntWritable.class);
job.setOutputValueClass(IntWritable.class);
FileInputFormat.addInputPath(job, new Path(args[0]));
FileOutputFormat.setOutputPath(job, new Path(args[1]));
System.exit(job.waitForCompletion(true) ? 0 : 1);
}

    
}
