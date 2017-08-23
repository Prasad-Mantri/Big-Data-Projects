/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package fastestflights;

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
public class FastestFlights {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // TODO code application logic here
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Fastest flights ");
        job.setJarByClass(FastestFlights.class);
        job.setMapperClass(ProjectAnalysis2_Mapper.class);
        job.setReducerClass(ProjectAnalysis2_Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    
    
     public static class ProjectAnalysis2_Mapper extends Mapper<Object, Text, Text, Text> {
         Text flghtTime = new Text();
         IntWritable outvalue = new IntWritable(1);
         
        private CSVParser csvR = new CSVParser(',', '"');
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Parse the input string into a nice map
            String[] recLine = this.csvR.parseLine(value.toString());
                       
        if (!(recLine[0].equalsIgnoreCase("Year")))
        {
            if ((recLine[2].isEmpty()) || (recLine[6].isEmpty()) || (recLine[9].isEmpty()) || (recLine[13].isEmpty()) || (recLine[30].isEmpty())) {
                return;
            }
            
       
            flghtTime.set(recLine[6]+","+recLine[30]);
            context.write(new Text(recLine[9]+","+recLine[13]+","+recLine[2]),flghtTime);
         
        }
            
        }      
            
   }
     
   public static class ProjectAnalysis2_Reducer extends Reducer<Text, Text, Text, Text> {

          Text finalValue = new Text();
          float fastflght=Float.MAX_VALUE;
          String flightCarrier; 
          float flghtTime;
          
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
          
        for (Text val : values) {
             String[] recLine = val.toString().split(",");
             if(recLine.length==2)
             {
                 flghtTime=Float.parseFloat(recLine[1]);
             
                if(flghtTime<fastflght) 
                 {
                   fastflght = flghtTime;
                   flightCarrier=recLine[0];
                 }
             }
              }
                
       // finalValue.set(flightCarrier + " " + flghtTime);
        //System.out.println(key.toString()+"---"+ flightCarrier + " " + flghtTime);
        context.write(key,new Text(flightCarrier + " " + flghtTime));
        }
      
   
}

    
}
