/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package flightsperseason;

import com.opencsv.CSVParser;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
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
public class FlightsPerSeason {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Flights Per Season");
        job.setJarByClass(FlightsPerSeason.class);
        job.setMapperClass(ProjectAnalysis5_Mapper1.class);
       // job.setCombinerClass(ProjectAnalysis5_Reducer1.class);
        job.setReducerClass(ProjectAnalysis5_Reducer1.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    
    public static class ProjectAnalysis5_Mapper1 extends Mapper<Object, Text, Text, IntWritable> {
         
         IntWritable outvalue = new IntWritable(1);
         Text season = new Text();
        private CSVParser csvR = new CSVParser(',', '"');

        @Override
        protected void map(Object key, Text value, Mapper.Context context) throws IOException, InterruptedException {
             String[] recLine = this.csvR.parseLine(value.toString());
             
              if(!(recLine[0].equalsIgnoreCase("Year")))
               {
                if (recLine[2].isEmpty()) {
                    return;
                    }
               
        
               if (!(recLine[2].isEmpty())){
                  int mont = Integer.parseInt(recLine[2]);
                  if((mont>=1) && (mont<=4))
                  {
                      season.set("Spring");
                  }else if((mont>=5) && (mont<=8))  
                  {
                      season.set("Summer");
                  }else if((mont>=9) && (mont<=12))  {
                         season.set("Fall");
                  }else{
                      return;
                  }
               }   
               }  
                   context.write(season,outvalue);
                 
              }  
    }
            
      

public static class ProjectAnalysis5_Reducer1 extends Reducer<Text, IntWritable, Text, IntWritable> {

          IntWritable finalValue = new IntWritable();
          
        @Override        
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count=0;
           for (IntWritable val : values) {
                 count++;
            } 
           finalValue.set(count); 
            if((key.toString().equals("Fall") || (key.toString().equals("Summer")) || (key.toString().equals("Spring"))))
             context.write(key,finalValue);
          }

        
        }
}
