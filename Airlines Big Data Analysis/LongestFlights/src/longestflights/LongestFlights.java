/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package longestflights;

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
public class LongestFlights {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Longest Duration Flights");
        job.setJarByClass(LongestFlights.class);
        job.setMapperClass(ProjectAnalysis6_Mapper1.class);
       // job.setCombinerClass(ProjectAnalysis5_Reducer1.class);
        job.setReducerClass(ProjectAnalysis6_Reducer1.class);
        //job.setNumReduceTasks(0);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    
    public static class ProjectAnalysis6_Mapper1 extends Mapper<Object, Text, NullWritable, Text> {
         
         IntWritable outvalue = new IntWritable();
         Text flightDetails = new Text();
        private CSVParser csvR = new CSVParser(',', '"');

        @Override
        protected void map(Object key, Text value, Mapper.Context context) throws IOException, InterruptedException {
             String[] recLine = this.csvR.parseLine(value.toString());
             
              if(!(recLine[0].equalsIgnoreCase("Year")))
               {
                if(((recLine[9].isEmpty()) && (recLine[13].isEmpty()) && (recLine[30].isEmpty()))) {
                    return;
                    }
               
                String org = recLine[9];
                String dest = recLine[13];
                String duration = recLine[30];        
                flightDetails.set(org + "--"+ dest+";" + duration);
                   System.out.println(org + "--"+ dest+";" + duration);
                //int duration = Integer.parseInt(recLine[30]);
                //outvalue.set(duration);
               }   
                 context.write(NullWritable.get(),flightDetails);
                 
              }  
    }
            
      

public static class ProjectAnalysis6_Reducer1 extends Reducer<NullWritable,Text,NullWritable, Text> {

         IntWritable finalValue = new IntWritable();
         private TreeMap<Float, Text> longest20Flights = new TreeMap<>();
          
        @Override
        protected void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
           
            for (Text val : values) {
               String recLine[] = val.toString().split(";");
               String details = recLine[0];
               
               if((recLine.length==2))
               {
                 
               float count = Float.parseFloat(recLine[1]);
               Text orgDest=new Text(details);
                System.out.println(count);
                if(!(longest20Flights.containsValue(orgDest)))
                    longest20Flights.put(count, orgDest);
               }

          } 
             int top =20;
            //System.out.println(sum + "" +count+""+avg );  
            for (Map.Entry e: longest20Flights.descendingMap().entrySet()) {
                //finalValue.set((flo)e.getKey());
                System.out.println(e.getValue().toString()+ " " + e.getKey().toString());
                top--;
                if(!(top<0))
                {        
                context.write(NullWritable.get(),new Text((e.getValue().toString())+ " "+e.getKey().toString()+" minutes"));
                }
            }

        }
      
    

        
        }
    
 
}
