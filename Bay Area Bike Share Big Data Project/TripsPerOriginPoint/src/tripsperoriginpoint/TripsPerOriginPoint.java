/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package tripsperoriginpoint;

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
public class TripsPerOriginPoint {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // TODO code application logic here
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Trips Per Origin Point");
        job.setJarByClass(TripsPerOriginPoint.class);
        job.setMapperClass(TripsPerOriginPoint_Mapper.class);
        //job.setCombinerClass(TripsPerOriginPoint_Reducer.class);
        job.setReducerClass(TripsPerOriginPoint_Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    
    public static class TripsPerOriginPoint_Mapper extends Mapper<Object, Text, Text, IntWritable> {
         
         IntWritable outvalue = new IntWritable(1);
         Text tripOrigin = new Text();
        private CSVParser csvR = new CSVParser(',', '"');

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
             String[] recLine = this.csvR.parseLine(value.toString());
             
              if(!(recLine[0].equalsIgnoreCase("id")))
               {
                  tripOrigin.set(recLine[3]);  
               }  else{ return;}
               context.write(tripOrigin,outvalue);
              }  
    }
            
      

public static class TripsPerOriginPoint_Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {

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
