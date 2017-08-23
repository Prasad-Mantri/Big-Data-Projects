/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package topdestbymonth;

import com.opencsv.CSVParser;
import java.io.IOException;
import java.util.Map.Entry;
import java.util.SortedMap;
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
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 *
 * @author prasad
 */
public class TopDestByMonth {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf, "Top Destination Citys");
        job1.setJarByClass(TopDestByMonth.class);
        job1.setMapperClass(ProjectAnalysis4_Mapper1.class);
        //job.setCombinerClass(ProjectAnalysis3_Reducer1.class);
       // job1.setGroupingComparatorClass(MonthGroupComparator.class);
        job1.setReducerClass(ProjectAnalysis4_Reducer1.class);
        job1.setOutputKeyClass(MonthDestWritable.class);
        job1.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));
       boolean complete = job1.waitForCompletion(true);
         
         Configuration conf2 = new Configuration();
         Job job2 = Job.getInstance(conf2,"Top Destination Citys-bin");
         
         if(complete){
         job2.setJarByClass(TopDestByMonth.class);
         job2.setMapperClass(ProjectAnalysis4_Mapper2.class);
         job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(NullWritable.class);
           job2.setNumReduceTasks(0);
        
        MultipleOutputs.addNamedOutput(job2, "Monthbins", TextOutputFormat.class, Text.class, NullWritable.class);

        MultipleOutputs.setCountersEnabled(job2, true);      
                
         FileInputFormat.addInputPath(job2, new Path(args[1]));
         FileOutputFormat.setOutputPath(job2, new Path(args[2]));
         //System.exit(job2.waitForCompletion(true) ? 0 : 1);
         boolean complete1 = job2.waitForCompletion(true);
         
         Configuration conf3 = new Configuration();
         Job job3 = Job.getInstance(conf3,"Top Destination Citys-partition");
         
         if(complete1){
         job3.setJarByClass(TopDestByMonth.class);
         job3.setMapperClass(ProjectAnalysis4_Mapper3.class);
         job3.setReducerClass(ProjectAnalysis4_Reducer3.class);
         job3.setMapOutputKeyClass(NullWritable.class);
         job3.setMapOutputValueClass(Text.class);
         job3.setOutputKeyClass(Text.class);
         job3.setOutputValueClass(NullWritable.class);
         job3.setPartitionerClass(MonthPartitioner.class);
         //job3.setNumReduceTasks(12);
         MonthPartitioner.setMinMonth(job3, 0);
        
         FileInputFormat.addInputPath(job3, new Path(args[2]));
         FileOutputFormat.setOutputPath(job3, new Path(args[3]));
         System.exit(job3.waitForCompletion(true) ? 0 : 1);
         
         }
         
         }
    }
    
    public static class ProjectAnalysis4_Mapper1 extends Mapper<Object, Text, MonthDestWritable, IntWritable> {
         
         IntWritable outvalue = new IntWritable(1);
         
        private CSVParser csvR = new CSVParser(',', '"');

        @Override
        protected void map(Object key, Text value, Mapper.Context context) throws IOException, InterruptedException {
             String[] recLine = this.csvR.parseLine(value.toString());
             
              if(!(recLine[0].equalsIgnoreCase("Year")))
               {
                if ((recLine[2].isEmpty()) || (recLine[14].isEmpty())) {
                    return;
                    }
                }
        
              if (!(recLine[14].equalsIgnoreCase("DEST_CITY_NAME")))
              {
                  //float delay = Float.parseFloat(recLine[14]);
                 // System.out.println(delay);
                  String city = recLine[14];
                  MonthDestWritable mdw = new MonthDestWritable(recLine[2],city);
                                    
                   context.write(mdw,outvalue);
                 
              }
              }
            
     }  

public static class ProjectAnalysis4_Reducer1 extends Reducer<MonthDestWritable, IntWritable, MonthDestWritable, IntWritable> {

          IntWritable finalValue = new IntWritable();
          
        @Override        
        protected void reduce(MonthDestWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count=0;
           for (IntWritable val : values) {
                 count++;
            } 
            finalValue.set(count);                 
           context.write(key,finalValue);
          }

        
        }

public static class ProjectAnalysis4_Mapper2 extends Mapper<Object, Text, Text, IntWritable> {
         Text flghtTime = new Text();
         IntWritable outvalue = new IntWritable();
           private MultipleOutputs<Text, NullWritable> multops;
           
         @Override
    protected void setup(Context context) {
        // Create a new MultipleOutputs using the context object
        multops = new MultipleOutputs(context);
    }
                
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String recLine[] = value.toString().split("\\t");  
             
              
                if ((recLine[0].isEmpty()) || (recLine[1].isEmpty())) {
                    return;
                    }
                        
              if ((recLine.length==2))
              {
                 String substrng[] = recLine[0].split(":"); 
                 String mont= substrng[0];
                // outvalue.set(val);
                  mont = mont+" Month bin";
                multops.write("Monthbins", value, NullWritable.get(), mont);
                //context.write(new Text(substrng[0]),outvalue);
              }
            
     }    
          @Override
        protected void cleanup(Mapper.Context context) throws IOException,
            InterruptedException {
        // Close multiple outputs!
        multops.close();
    }
            
   } 
     
  public static class ProjectAnalysis4_Mapper3 extends Mapper<Object, Text, NullWritable, Text> {
         Text cityCount = new Text();
         IntWritable month = new IntWritable();
              
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String recLine[] = value.toString().split("\\t");  
                          
                if ((recLine[0].isEmpty()) || (recLine[1].isEmpty())) {
                    return;
                    }
                        
              if ((recLine.length==2))
              {
                 String substrng[] = recLine[0].split(":"); 
                 cityCount.set(substrng[1]+"--"+recLine[1]);
                 month.set(Integer.parseInt(substrng[0]));
                // top5Dest.put(count,)
               context.write(NullWritable.get(),cityCount);
              }
            
 
        }  
      
   } 
     
   public static class ProjectAnalysis4_Reducer3 extends Reducer<NullWritable, Text, Text, NullWritable> {

         IntWritable finalValue = new IntWritable();
         private TreeMap<Integer, Text> top50Dest = new TreeMap<>();
          
        @Override
        protected void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
           
            for (Text val : values) {
               String recLine[] = val.toString().split("--");
               String city = recLine[0];
               int count = Integer.parseInt(recLine[1]);
               Text cityName=new Text(city);
                System.out.println(count);
                if(!(top50Dest.containsValue(cityName)))
                    top50Dest.put(count, cityName);
               

          } 
             int top =50;
            //System.out.println(sum + "" +count+""+avg );  
            for (Entry e: top50Dest.descendingMap().entrySet()) {
                finalValue.set((int)e.getKey());
                System.out.println(e.getKey());
                top--;
                if(!(top<0))
                {        
                context.write(new Text(e.getKey().toString()+"::"+(e.getValue().toString())),NullWritable.get());
                }
            }

        }
      
    
}
}