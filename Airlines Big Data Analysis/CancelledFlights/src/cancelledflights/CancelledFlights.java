/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cancelledflights;

import com.opencsv.CSVParser;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 *
 * @author prasad
 */
public class CancelledFlights {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Cancelled Flights");
        job.setJarByClass(CancelledFlights.class);
        
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, ProjectAnalysis7_Mapper1.class);
        
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, ProjectAnalysis7_Mapper2.class);

        job.getConfiguration().set("join.type", "inner");
        job.setReducerClass(ProjectAnalysis7_Reducer1.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(args[2]));

      
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

         System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
  
    public static class ProjectAnalysis7_Mapper1 extends Mapper<Object, Text, Text, Text> {

        private Text outkey = new Text();
        private Text outvalue = new Text();

         private CSVParser csvR = new CSVParser(',', '"');
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            
            String[] recLine = this.csvR.parseLine(value.toString());
             
              if(!(recLine[0].equalsIgnoreCase("Year")))
               {
                if(((recLine[9].isEmpty()) && (recLine[13].isEmpty()))) {
                    return;
                }
             if (recLine[27].equals("A") || recLine[27].equals("B") || recLine[27].equals("C") || recLine[27].equals("D"))
                      outkey.set(recLine[27]);
             else
                 return;
            
            outvalue.set("M" + recLine[9]+"::"+recLine[13]);
            context.write(outkey,outvalue);
        }
     }
    }
    public static class ProjectAnalysis7_Mapper2 extends Mapper<Object, Text, Text, Text> {

         private Text outkey = new Text();
         private Text outvalue = new Text();

         private CSVParser csvR = new CSVParser(',', '"');

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
             String[] recLine = this.csvR.parseLine(value.toString());
             
              if(!(recLine[0].equalsIgnoreCase("Code")))
               {
                if(((recLine[0].isEmpty()) && (recLine[1].isEmpty()))) {
                    return;
                }
                         
            outkey.set(recLine[0]);
            outvalue.set("R" + recLine[1]);
            context.write(outkey, outvalue);
               }
        }
    }
    
     
    
  public static class ProjectAnalysis7_Reducer1 extends Reducer<Text, Text, Text, Text> {

        private static final Text EMPTY_TEXT = new Text("");
        private Text tmp = new Text();
        private ArrayList<Text> listM = new ArrayList<Text>();
        private ArrayList<Text> listR = new ArrayList<Text>();

        private String joinType = null;

        @Override
        public void setup(Context context) {
            // Get the type of join from our configuration
            joinType = context.getConfiguration().get("join.type");
        }

      
        
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
           
            listM.clear();
            listR.clear();

            while (values.iterator().hasNext()) {
                tmp = values.iterator().next();
               
                if (Character.toString((char) tmp.charAt(0)).equals("M")) {

                    
                    listM.add(new Text(tmp.toString().substring(1)));
                }
                if (Character.toString((char) tmp.charAt(0)).equals("R")) {
                    
                    listR.add(new Text(tmp.toString().substring(1)));
                }

                
            }
           executeJoinLogic(context);
            
        }
        
        private void executeJoinLogic(Context context) throws IOException, InterruptedException {

            
            if (joinType.equalsIgnoreCase("inner")) {
               
                if (!listM.isEmpty() && !listR.isEmpty()) {
                   
                    for (Text M : listM) {
                        System.out.println(M.toString());
                        for (Text R : listR) {
                            System.out.println(R.toString());

                            context.write(M, R);

                        }
                    }
                }
            }
        }

    }  
    
    
} 

    

