 
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class HashtagCounterDriver extends Configured implements Tool{

  public static void main(String[] args) throws Exception {
	  int res= ToolRunner.run(new HashtagCounterDriver(), args);
	  System.exit(res);
    
  }
  public int run(String[] args) throws Exception{
	  Job job =Job.getInstance(getConf(),"WorldCounter");
	  job.setJarByClass(this.getClass());
	  
	  FileInputFormat.addInputPath(job,new Path(args[0]));
	  FileOutputFormat.setOutputPath(job, new Path(args[1]));
	  
	  job.setMapperClass(HashtagCounterMapper.class);
	  job.setReducerClass(HashtagCounterReducer.class);
	  
	  job.setOutputKeyClass(Text.class);
	  job.setOutputValueClass(IntWritable.class);
	  
	  return job.waitForCompletion(true) ?0 : 1;
  }
}

