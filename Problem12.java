import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Problem12 {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    public void map(Object key, Text value, Context context
 ) throws IOException, InterruptedException {
     try {
        String [] itr1 = value.toString().split(",");
//StringBuilder sb = new StringBuilder();
      //String []  itr = itr1[2].split(" ");
      if((!itr1[2].equals("Unknown") && !itr1[2].equals("Arr Arr"))) {
//       sb.append(itr1[6]);
       //sb.append("_");
       //sb.append(itr1[2]);
       context.write(new Text(itr1[2].trim()), new IntWritable(Integer.parseInt(itr1[7])));
}
       }catch(Exception e) {

      }
}
    }

  public static class IntSumReducer
  extends Reducer<Text,IntWritable,Text,IntWritable> {
//    private IntWritable result = new IntWritable();
    private TreeMap<Integer, String> top10 = new TreeMap<Integer,String>();
    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      Integer sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }

 top10.put(sum, key.toString());

        if(top10.size() > 10){
         top10.remove(top10.firstKey());
}

// result.set(sum);
//      context.write(key, new IntWritable(sum));
    }
   protected void cleanup(Context context) throws IOException, InterruptedException{
        for (Integer t : top10.keySet()){
         context.write(new Text(top10.get(t)),new IntWritable(t));
}
  }
}
 public static void main(String[] args) throws Exception {
Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "problem12");
    job.setJarByClass(Problem12.class);
    job.setMapperClass(TokenizerMapper.class);
//    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

