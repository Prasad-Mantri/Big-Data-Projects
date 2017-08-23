/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package peakhours;

import com.opencsv.CSVParser;
import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
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
public class PeakHours {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // TODO code application logic here
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Peak Hours Job 1");
        job.setJarByClass(PeakHours.class);
        job.setMapperClass(PeakHours_Mapper.class);
        
        job.setReducerClass(PeakHours_Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        boolean complete = job.waitForCompletion(true);
        
         Configuration conf1 = new Configuration();
         Job job1 = Job.getInstance(conf1,"Peak Hours Job 2");
         
         if(complete){
         job1.setJarByClass(PeakHours.class);
         job1.setMapperClass(PeakHours_Mapper1.class);
         job1.setReducerClass(PeakHours_Reducer1.class);
         job1.setOutputKeyClass(Text.class);
         job1.setOutputValueClass(NullWritable.class);
         job1.setMapOutputKeyClass(NullWritable.class);
         job1.setMapOutputValueClass(Text.class);
        
         FileInputFormat.addInputPath(job1, new Path(args[1]));
         FileOutputFormat.setOutputPath(job1, new Path(args[2]));
        
        System.exit(job1.waitForCompletion(true) ? 0 : 1);
    }
        
    }
     public static class PeakHours_Mapper extends Mapper<Object, Text, Text, IntWritable> {
         
         IntWritable outvalue = new IntWritable(1);
         Text Hour = new Text();
        private CSVParser csvR = new CSVParser(',', '"');

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
             String[] recLine = this.csvR.parseLine(value.toString());
             
              if(!(recLine[0].equalsIgnoreCase("id")))
               {
                  
                  String[] interM = recLine[2].split(" ");
                  String hour = interM[1].substring(1,2);
                                   
                   if(hour.equalsIgnoreCase(":"))
                       hour = interM[1].substring(0,1);
                   else 
                       hour = interM[1].substring(0,2);
                  Hour.set(hour);
               }  else{ return;}
               context.write(Hour,outvalue);
              }  
    }
            
      

public static class PeakHours_Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {

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
    
public static class PeakHours_Mapper1 extends Mapper<Object, Text, NullWritable, Text> {
         Text hourCount = new Text();
                     
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String recLine[] = value.toString().split("\\t");  
                          
                              
              if ((recLine.length==2))
              {
                  
                 hourCount.set(recLine[0]+"--"+recLine[1]);
                 
               context.write(NullWritable.get(),hourCount);
              }
            
 
        }  
      
   } 
     
   public static class PeakHours_Reducer1 extends Reducer<NullWritable, Text, Text, NullWritable> {

         IntWritable finalValue = new IntWritable();
         private TreeMap<Integer, Text> top5hours = new TreeMap<>();
          
        @Override
        protected void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
           
            for (Text val : values) {
               String recLine[] = val.toString().split("--");
               String hour = recLine[0];
               int count = Integer.parseInt(recLine[1]);
               Text Hour=new Text(hour);
                System.out.println(count);
                if(!(top5hours.containsValue(Hour)))
                    top5hours.put(count, Hour);
              } 
             int top =5;
             
            for (Map.Entry e: top5hours.descendingMap().entrySet()) {
                finalValue.set((int)e.getKey());
                System.out.println(e.getKey());
                top--;
                if(!(top<0))
                {        
                context.write(new Text(e.getKey().toString()+" "+(e.getValue().toString())),NullWritable.get());
                }
            }

        }
      
    
}
}
