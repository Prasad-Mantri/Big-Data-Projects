/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package custumorsubscriber;

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
public class CustumorSubscriber {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // TODO code application logic here
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Customer Subscriber");
        job.setJarByClass(CustumorSubscriber.class);
        job.setMapperClass(CustumorSubscriber_Mapper.class);
        //job.setCombinerClass(TripsPerOriginPoint_Reducer.class);
        job.setReducerClass(CustumorSubscriber_Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    
    public static class CustumorSubscriber_Mapper extends Mapper<Object, Text, Text, IntWritable> {
         
         IntWritable outvalue = new IntWritable(1);
         Text yearCustType = new Text();
        private CSVParser csvR = new CSVParser(',', '"');

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
             String[] recLine = this.csvR.parseLine(value.toString());
             
              if(!(recLine[0].equalsIgnoreCase("id")))
               {
                  
                  String[] interM = recLine[2].split(" ");
                  String year = interM[0].substring((interM[0].length()-4),interM[0].length());
                  yearCustType.set(year+" "+recLine[9]);
               }  else{ return;}
               context.write(yearCustType,outvalue);
              }  
    }
            
      

public static class CustumorSubscriber_Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {

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
