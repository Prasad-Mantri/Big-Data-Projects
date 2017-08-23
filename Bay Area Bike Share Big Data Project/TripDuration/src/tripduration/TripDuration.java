/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package tripduration;

import com.opencsv.CSVParser;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author prasad
 */
public class TripDuration {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // TODO code application logic here
         Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Trips Duration Distribution");
        job.setJarByClass(TripDuration.class);
        job.setMapperClass(TripDuration_Mapper.class);
        //job.setCombinerClass(TripsPerOriginPoint_Reducer.class);
        job.setReducerClass(TripDuration_Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
     public static class TripDuration_Mapper extends Mapper<Object, Text, Text, IntWritable> {
         
         IntWritable outvalue = new IntWritable(1);
         Text tripDuration = new Text();
        private CSVParser csvR = new CSVParser(',', '"');

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
             String[] recLine = this.csvR.parseLine(value.toString());
             
              if(!(recLine[0].equalsIgnoreCase("id")))
               {
                  if(Integer.parseInt(recLine[1])<=3600)
                      tripDuration.set("0-1 Hr");
                  else if((Integer.parseInt(recLine[1])>3600) && (Integer.parseInt(recLine[1])<=10800))
                      tripDuration.set("1-3 Hrs");
                  else if((Integer.parseInt(recLine[1])>10800) && (Integer.parseInt(recLine[1])<=21600))
                      tripDuration.set("3-6 Hrs");
                  else if((Integer.parseInt(recLine[1])>21600) && (Integer.parseInt(recLine[1])<=32400))
                      tripDuration.set("6-9 Hrs");
                  else
                      tripDuration.set("More than 9 hrs");
               }  else{ return;}
               context.write(tripDuration,outvalue);
              }  
    }
            
      

public static class TripDuration_Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {

          IntWritable finalCount = new IntWritable();
          
        @Override        
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count=0;
           for (IntWritable val : values) {
                 count++;
            } 
            finalCount.set(count);
            context.write(key,finalCount);
          }

        
        }
    
    
    
}
