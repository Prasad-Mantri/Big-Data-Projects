/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package delayedflightsairports;

import com.opencsv.CSVParser;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
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
public class DelayedFlightsAirports {

   public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // TODO code application logic here
        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf, "Delayed Flights Airports");
        job1.setJarByClass(DelayedFlightsAirports.class);
        job1.setMapperClass(ProjectAnalysis3_Mapper1.class);
       // job.setCombinerClass(ProjectAnalysis3_Reducer1.class);
        job1.setReducerClass(ProjectAnalysis3_Reducer1.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));
       // System.exit(job.waitForCompletion(true) ? 0 : 1);
        
        boolean complete = job1.waitForCompletion(true);
         
         Configuration conf2 = new Configuration();
         Job job2 = Job.getInstance(conf2,"Monthly Average");
         
         if(complete){
         job2.setJarByClass(DelayedFlightsAirports.class);
         job2.setMapperClass(ProjectAnalysis3_Mapper2.class);
         job2.setMapOutputKeyClass(Text.class);
         job2.setMapOutputValueClass(IntWritable.class);
         
         job2.setReducerClass(ProjectAnalysis3_Reducer2.class);
         job2.setOutputKeyClass(Text.class);
         job2.setOutputValueClass(IntWritable.class);
         
         FileInputFormat.addInputPath(job2, new Path(args[1]));
         FileOutputFormat.setOutputPath(job2, new Path(args[2]));
         System.exit(job2.waitForCompletion(true) ? 0 : 1);
         }
        
    }
    
    
     public static class ProjectAnalysis3_Mapper1 extends Mapper<Object, Text, Text, IntWritable> {
         Text flghtTime = new Text();
         IntWritable outvalue = new IntWritable(1);
         
        private CSVParser csvR = new CSVParser(',', '"');

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
             String[] recLine = this.csvR.parseLine(value.toString());
             
              if(!(recLine[0].equalsIgnoreCase("Year")))
               {
                if ((recLine[2].isEmpty()) || (recLine[9].isEmpty()) || (recLine[19].isEmpty())) {
                    return;
                    }
                }
        
              if (!(recLine[19].equalsIgnoreCase("DEP_DELAY_NEW")))
              {
                  float delay = Float.parseFloat(recLine[19]);
                 // System.out.println(delay);
                  if((delay>0) & (delay!=0))
                  {  
                   context.write(new Text(recLine[9]+","+recLine[2]),outvalue);
                  }
              }
              }
            
     }      
            
   
     
   public static class ProjectAnalysis3_Reducer1 extends Reducer<Text, IntWritable, Text, IntWritable> {

          IntWritable finalValue = new IntWritable();
          
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count=0;
           for (IntWritable val : values) {
                 count++;
            } 
            finalValue.set(count);                 
           context.write(key,finalValue);
          }

        
        }
      
   public static class ProjectAnalysis3_Mapper2 extends Mapper<Object, Text, Text, IntWritable> {
         Text flghtTime = new Text();
         IntWritable outvalue = new IntWritable();
                
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String recLine[] = value.toString().split("\\t");  
             
              
                if ((recLine[0].isEmpty()) || (recLine[1].isEmpty())) {
                    return;
                    }
                        
              if ((recLine.length==2))
              {
                 String substrng[] = recLine[0].split(","); 
                 int val= Integer.parseInt(recLine[1]);
                 outvalue.set(val);
                context.write(new Text(substrng[0]),outvalue);
              }
            
     }      
            
   } 
     
   public static class ProjectAnalysis3_Reducer2 extends Reducer<Text, IntWritable, Text, IntWritable> {

          IntWritable finalValue = new IntWritable();
          
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
           int count=0;
           int sum =0;  
           int avg =0;
            for (IntWritable val : values) {
               sum =sum+val.get();
               count++;
              }
            
              avg = sum/count;
            System.out.println(sum + "" +count+""+avg );  
            finalValue.set(avg);                 
           context.write(key,finalValue);
          }
        
        }
      
}
    

