import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class BSE { 
	public static class CountMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] words = value.toString().trim().split("-");
            if(words.length<7||words==null){
            	return;
            }
            try{
            	float d1 = Float.parseFloat(words[3]);
                float d2 = Float.parseFloat(words[6]);
                float d3 = (d2-d1)/d1 *100;
                String s = words[0]+"/"+words[1]+"/"+words[2]+"\t\t";
                if(d3>5.0||(d3<(-5))){
                	context.write(new Text(s),new FloatWritable(d3));
                }
            }
            catch(Exception e){
            	return;
            }
            
           
        }
    }
    public static class CountReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
        @Override
        public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
            float sum = 0;
            for (FloatWritable value : values) {
                sum += value.get();
            }
            context.write(key, new FloatWritable(sum));
        }    
  }
  public static void main(String[] args) throws Exception {
	  Configuration configuration = new Configuration();
      Job job = Job.getInstance(configuration, "Word Count");
      job.setJarByClass(BSE.class);
      job.setMapperClass(BSE.CountMapper.class);
      job.setCombinerClass(BSE.CountReducer.class);
      job.setReducerClass(BSE.CountReducer.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(FloatWritable.class);
      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));
      System.exit(job.waitForCompletion(true)? 0 : 1);
  }
}