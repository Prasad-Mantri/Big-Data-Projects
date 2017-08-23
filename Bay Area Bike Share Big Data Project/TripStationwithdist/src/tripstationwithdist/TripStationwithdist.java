/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package tripstationwithdist;

import com.opencsv.CSVParser;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
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
public class TripStationwithdist {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // TODO code application logic here
          Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Trips Per Origin Point");
        job.setJarByClass(TripStationwithdist.class);
        job.setMapperClass(TripStationwithdist_Mapper.class);
       
        job.setReducerClass(TripStationwithdist_Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
         DistributedCache.addCacheFile((new Path("/BigDataProject/BikeSharedata/station.csv")).toUri(), job.getConfiguration());
        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    public static class TripStationwithdist_Mapper extends Mapper<Object, Text, Text, IntWritable> {
         
         IntWritable outvalue = new IntWritable(1);
         Text tripOrigin = new Text();
           Map<String,String> citylist = new HashMap<>(); 
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            
            String code;
             String city;
             
            try {
                 
                Path[] files = DistributedCache.getLocalCacheFiles(context.getConfiguration());
                citylist.clear();
              if (files != null && files.length > 0) {

                    for (Path myfile : files) {

                        try {
                           
                          if(myfile.getName().equals("station.csv"))
                           {   
                            BufferedReader bufferedReader = new BufferedReader(new FileReader(myfile.toString()));
                            String line = bufferedReader.readLine();
                            while ((line) != null) {
                                String[] mytokens = this.csvR.parseLine(line);
                                 code = String.valueOf(mytokens[1]);
                                 city = String.valueOf(mytokens[5]);
                                 citylist.put(code,city);
                                 line = bufferedReader.readLine();
                                }
                           }
                        } catch (IOException ex) {
                            System.err.println("Exception to read  file: " + ex.getMessage());
                        }
                      } 
                    
                    if(citylist.isEmpty())
                    {
                        System.out.println("HashMap is empty!!!");
                    }
                    //}
                }
            } catch (IOException ex) {
                System.err.println("Exception for mapper setup: " + ex.getMessage());
            }

            
        }
         
         
        private CSVParser csvR = new CSVParser(',', '"');

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
             String[] recLine = this.csvR.parseLine(value.toString());
             
              if(!(recLine[0].equalsIgnoreCase("id")))
               {
                    
                  String city = citylist.get(recLine[3]);
                  tripOrigin.set(recLine[3] +" "+city);
               }  else{ return;}
               context.write(tripOrigin,outvalue);
              }  
    }
            
      

public static class TripStationwithdist_Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {

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
