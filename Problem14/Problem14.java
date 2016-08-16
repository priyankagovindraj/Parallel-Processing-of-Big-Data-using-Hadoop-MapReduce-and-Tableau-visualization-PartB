import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Map;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
public class Problem14 {
static Map<String, Integer> hm = new HashMap<String, Integer>();
  public static class TokenizerMapper1
       extends Mapper<Object, Text, Text, IntWritable>{

    public void map(Object key, Text value, Context context
 ) throws IOException, InterruptedException {
     try {
        String [] itr1 = value.toString().split(",");
      StringBuilder sb = new StringBuilder();
	if(itr1[4].equals("Unknown"))return;
	String dates[]=itr1[4].split("-");
	String start_date=dates[0].trim();
	String end_dates=dates[1].trim();
	SimpleDateFormat timingFormat = new SimpleDateFormat("h:mma");
	 Date date1 = timingFormat.parse(start_date);
	String s = timingFormat.format(date1);
	sb.append(s);
       context.write(new Text(sb.toString()), new IntWritable(1));
//}
       }catch(Exception e) {
      }
}
    }

  public static class IntSumReducer1
  extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
	int count=0;
      for (IntWritable val : values) {
        sum += val.get();
      }

 result.set(sum);
      context.write(key, result);
    }
  }

  
 public static void main(String[] args) throws Exception {
Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "problem1");
    job.setJarByClass(Problem14.class);
    job.setMapperClass(TokenizerMapper1.class);
    job.setCombinerClass(IntSumReducer1.class);
    job.setReducerClass(IntSumReducer1.class);
    job.setOutputKeyClass(Text.class);

    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

