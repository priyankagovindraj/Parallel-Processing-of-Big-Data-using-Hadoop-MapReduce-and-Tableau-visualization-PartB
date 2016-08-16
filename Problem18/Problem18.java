import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Map;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
public class Problem18 {
static Map<String, Float> check = new HashMap<String, Float>();
  public static class TokenizerMapper1
       extends Mapper<Object, Text, Text, FloatWritable>{

    public void map(Object key, Text value, Context context
 ) throws IOException, InterruptedException {
     try {
        String [] itr1 = value.toString().split(",");
      StringBuilder sb = new StringBuilder();
	String year=itr1[1].split(" ")[1];
	sb.append(year);
	if(check.containsKey(year)){
		check.put(year,check.get(year)+(float)1);
	}
	else{
	check.put(year,(float)1);}

	if(Integer.parseInt(itr1[7])>Integer.parseInt(itr1[8])){
       context.write(new Text(sb.toString()), new FloatWritable(1));}

//}
       }catch(Exception e) {
      }
}
    }

  public static class IntSumReducer1
  extends Reducer<Text,FloatWritable,Text,FloatWritable> {
    private FloatWritable result = new FloatWritable();

    public void reduce(Text key, Iterable<FloatWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      float sum = 0;
	float count=0;
      for (FloatWritable val : values) {
        sum += val.get();
      }
String key1=key.toString();
float counter=check.get(key1);
float d=sum/counter;
 result.set(d);
      context.write(key, result);
    }
  }

  
 public static void main(String[] args) throws Exception {
Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "problem1");
    job.setJarByClass(Problem18.class);
    job.setMapperClass(TokenizerMapper1.class);
    job.setCombinerClass(IntSumReducer1.class);
    job.setReducerClass(IntSumReducer1.class);
    job.setOutputKeyClass(Text.class);

    job.setOutputValueClass(FloatWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

