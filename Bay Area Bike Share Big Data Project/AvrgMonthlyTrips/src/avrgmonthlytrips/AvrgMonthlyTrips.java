/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package avrgmonthlytrips;

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
public class AvrgMonthlyTrips {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // TODO code application logic here
         // TODO code application logic here
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Average Monthly Trips Part 1");
        job.setJarByClass(AvrgMonthlyTrips.class);
        job.setMapperClass(AvrgMonthlyTrips_Mapper.class);
        
        job.setReducerClass(AvrgMonthlyTrips_Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        boolean complete = job.waitForCompletion(true);
        
         Configuration conf1 = new Configuration();
         Job job1 = Job.getInstance(conf1,"Average Monthly Trips Part 2");
         
         if(complete){
         job1.setJarByClass(AvrgMonthlyTrips.class);
         job1.setMapperClass(AvrgMonthlyTrips_Mapper1.class);
         job1.setReducerClass(AvrgMonthlyTrips_Reducer1.class);
         job1.setOutputKeyClass(Text.class);
         job1.setOutputValueClass(IntWritable.class);
               
        
         FileInputFormat.addInputPath(job1, new Path(args[1]));
         FileOutputFormat.setOutputPath(job1, new Path(args[2]));
        
        System.exit(job1.waitForCompletion(true) ? 0 : 1);
    }
    
  }   
    public static class AvrgMonthlyTrips_Mapper extends Mapper<Object, Text, Text, IntWritable> {
         
         IntWritable outvalue = new IntWritable(1);
         Text MonthYear = new Text();
        private CSVParser csvR = new CSVParser(',', '"');

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
             String[] recLine = this.csvR.parseLine(value.toString());
             
              if(!(recLine[0].equalsIgnoreCase("id")))
               {
                   String mon = recLine[2].substring(1,2);
                   
                   if(mon.equalsIgnoreCase("/"))
                       mon = recLine[2].substring(0,1);
                   else 
                       mon = recLine[2].substring(0,2);
                   String[] interM = recLine[2].split(" ");
                   String year = interM[0].substring((interM[0].length()-4),interM[0].length());
                   
                   MonthYear.set(mon+' '+year);
               }  else{ return;}
               context.write(MonthYear,outvalue);
              }  
    }
            
      

public static class AvrgMonthlyTrips_Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {

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
    

public static class AvrgMonthlyTrips_Mapper1 extends Mapper<Object, Text, Text, IntWritable> {
         
         IntWritable outvalue = new IntWritable();
         Text Month = new Text();
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
             String recLine[] = value.toString().split("\\t"); 
             
                   String mon = recLine[0].substring(0,2);
                   Month.set(mon);                  
                   outvalue.set(Integer.parseInt(recLine[1]));
               
               context.write(Month,outvalue);
              }  
    }
            
      

public static class AvrgMonthlyTrips_Reducer1 extends Reducer<Text, IntWritable, Text, IntWritable> {

          IntWritable finalavrg = new IntWritable();
          
        @Override        
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count=0;
            int sum =0;
           for (IntWritable val : values) {
                 sum = sum + val.get();
                 count++;
            } 
            finalavrg.set((Integer)sum/count);
            context.write(key,finalavrg);
          }

        
        }
    
}
