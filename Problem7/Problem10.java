
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Problem10 {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{
private static final IntWritable one = new IntWritable(1);
    public void map(Object key, Text value, Context context
 ) throws IOException, InterruptedException {
     try {
        String [] itr1 = value.toString().split(",");
      StringBuilder sb = new StringBuilder();
      StringBuilder sb1 = new StringBuilder();
      String []  itr = itr1[1].split(" ");
       //if((Integer.parseInt(itr1[8]) > Integer.parseInt(itr1[7])) && (!itr1[2].equals("Unknown") && !itr1[2].equals("Arr"))) {
      
       sb.append(itr[1]);
       //sb1.append(itr1[1]);
       //sb.append("_");
      // sb.append(itr1[2]);
       context.write(new Text(sb.toString()), one);
//}
       }catch(Exception e) {

      }
}
    }

  public static class IntSumReducer
  extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

  public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
       //StringBuilder sb = new StringBuilder();     
      int sum = 0;
      for (IntWritable val : values) {
	//sb.append(val);       
       sum += val.get();
      }
 result.set(sum);
      context.write(key, result);
    }
  }
 public static void main(String[] args) throws Exception {
Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "problem10");
    job.setJarByClass(Problem10.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

