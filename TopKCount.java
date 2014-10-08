import java.io.IOException;
import java.util.*;
        
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
        
public class TopKCount {
        
 public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(-1);
    private Text word = new Text();
        
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);
        while (tokenizer.hasMoreTokens()) {
            word.set(tokenizer.nextToken());
            if (word.toString().length() == 7){
            context.write(word, one);
            }
        }
    }
 } 
        
 public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

    public void reduce(Text key, Iterable<IntWritable> values, Context context) 
      throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        context.write(key, new IntWritable(sum));
    }
 }

 public static class Mapnext extends Mapper<LongWritable, Text, IntWritable, Text> {
        
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String values[] = line.split("\t");
        context.write(new IntWritable(Integer.parseInt(values[1])), new Text(values[0]));
    }
 } 
        
 public static class Reducenext extends Reducer<IntWritable, Text, Text, IntWritable> {
    int n = 0;
    public void reduce(IntWritable key, Iterable<Text> values, Context context) 
      throws IOException, InterruptedException {
        for (Text val : values) {
            n+=1;
            if(n<101) {
                int s = key.get();
                context.write(val, new IntWritable(s*-1));
            }
            else  {
                break;
            }
        }
    }
 }
        
 public static void main(String[] args) throws Exception {
    Configuration conf1 = new Configuration();
        
    Job firstjob = new Job(conf1, "topkcount");
    
    firstjob.setOutputKeyClass(Text.class);
    firstjob.setOutputValueClass(IntWritable.class);
        
    firstjob.setMapperClass(Map.class);
    firstjob.setReducerClass(Reduce.class);
        
    firstjob.setInputFormatClass(TextInputFormat.class);
    firstjob.setOutputFormatClass(TextOutputFormat.class);

    firstjob.setNumReduceTasks(1);
    firstjob.setJarByClass(TopKCount.class);
        
    FileInputFormat.addInputPath(firstjob, new Path(args[0]));
    FileOutputFormat.setOutputPath(firstjob, new Path(args[1]));

    Configuration conf2 = new Configuration();
    
    Job nextjob = new Job(conf2, "topkcount");
    
    nextjob.setOutputKeyClass(IntWritable.class);
    nextjob.setOutputValueClass(Text.class);
        
    nextjob.setMapperClass(Mapnext.class);
    nextjob.setReducerClass(Reducenext.class);
        
    nextjob.setInputFormatClass(TextInputFormat.class);
    nextjob.setOutputFormatClass(TextOutputFormat.class);

    nextjob.setNumReduceTasks(1);
    nextjob.setJarByClass(TopKCount.class);
        
    FileInputFormat.addInputPath(nextjob, new Path(args[1]));
    FileOutputFormat.setOutputPath(nextjob, new Path(args[2]));

    firstjob.submit();
    if(firstjob.waitForCompletion(true)) {
        nextjob.submit();
        nextjob.waitForCompletion(true);
    }
        
 }       
}
