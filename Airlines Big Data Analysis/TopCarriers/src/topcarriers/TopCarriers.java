/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package topcarriers;

import com.opencsv.CSVParser;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author prasad
 */
public class TopCarriers {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException {
        // TODO code application logic here
        Configuration conf = new Configuration();
        FileSystem fsm=FileSystem.get(conf);
        FileSystem loct=FileSystem.getLocal(conf);
        
        Path inpDir= new Path(args[0]);
        Path intermDir= new Path(args[1]);
        try{
        FileStatus[] inpFiles=loct.listStatus(inpDir);
        FSDataOutputStream outf=fsm.create(intermDir);
        
        for(int i=0;i<inpFiles.length;i++){
            FSDataInputStream in=loct.open(inpFiles[i].getPath());
            byte buffer[]=new byte[265];
            int readBytes=0;
            while((readBytes=in.read(buffer))>0){
                outf.write(buffer,0,readBytes);
            }
            in.close();
        }
        outf.close();
        }catch(IOException e){
            e.printStackTrace();
    }
        Job job = Job.getInstance(conf, "Carrier Countlist");
        job.setJarByClass(TopCarriers.class);
        job.setMapperClass(ProjectAnalysis1_Mapper.class);
        job.setCombinerClass(ProjectAnalysis1_Reducer.class);
        job.setReducerClass(ProjectAnalysis1_Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
      //  job.setNumReduceTasks(0);
        //job.addCacheFile(new Path("hdfs://localhost:9000/AirlineInput2016/Support_Data/UNIQUE_CARRIERS.csv").toUri());
        DistributedCache.addCacheFile((new Path("/Support_Data/UNIQUE_CARRIERS.csv")).toUri(), job.getConfiguration());
        FileInputFormat.addInputPath(job,intermDir);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    
    
     public static class ProjectAnalysis1_Mapper extends Mapper<Object, Text, Text, IntWritable> {
         Text outkey = new Text();
         IntWritable outvalue = new IntWritable(1);
            Map<String,String> carrierlist = new HashMap<>(); 
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            
            String code;
             String desc;
             
            try {
                 
                Path[] files = DistributedCache.getLocalCacheFiles(context.getConfiguration());
                carrierlist.clear();
              if (files != null && files.length > 0) {

                    for (Path myfile : files) {

                        try {
                           
                          if(myfile.getName().equals("UNIQUE_CARRIERS.csv"))
                           {   
                            BufferedReader bufferedReader = new BufferedReader(new FileReader(myfile.toString()));
                            String line = bufferedReader.readLine();
                            while ((line) != null) {
                                String[] mytokens = this.csvR.parseLine(line);
                                 code = String.valueOf(mytokens[0]);
                                 desc = String.valueOf(mytokens[1]);
                                 carrierlist.put(code,desc);
                                 line = bufferedReader.readLine();
                                }
                           }
                        } catch (IOException ex) {
                            System.err.println("Exception while reading  file: " + ex.getMessage());
                        }
                      } 
                    
                    if(carrierlist.isEmpty())
                    {
                        System.out.println("HashMap is empty!!!");
                    }
                    //}
                }
            } catch (IOException ex) {
                System.err.println("Exception in mapper setup: " + ex.getMessage());
            }

            
        }
         
         

        private CSVParser csvR = new CSVParser(',', '"');
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Parse the input string into a nice map
            String[] recLine = this.csvR.parseLine(value.toString());
                       
        if (!(recLine[0].equalsIgnoreCase("Year")))
        {
            String carrierId = recLine[6];
            if (carrierId == null) {
                return;
            }
          
          //  System.out.println("In mapper setting key as " + carrierId);
           String carrier = carrierlist.get(carrierId);;
              outkey.set(carrier); 
            context.write(outkey,outvalue);
         
        }
            
        }      
            
   }
     
   public static class ProjectAnalysis1_Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable cnt = new IntWritable();
    private Text out1 = new Text();
    
         private CSVParser csvR = new CSVParser(',', '"');
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
          Text outkey1 = new Text();
            int ad = 0;
           
        for (IntWritable val : values) {
            ad += val.get();
        }
        
        cnt.set(ad);
//        outkey1.set(key.toString());
//        System.out.println("key"+ key.toString());
//       String carrier = carrierlist.get(key.toString());
//          out1.set(carrier);
//        System.out.println("Carriervalue"+carrier);
//        
//        //out.set(carrier + key.toString());
//         if (carrier.isEmpty())
//            {   
//         //  outkey.set(carrier);
//        // outkey1.set(carrier);
//             context.write(outkey1, cnt);
//            } 
//           else 
//              context.write(out1, cnt);
           context.write(key,cnt);
        }
    
   
        }    
     
}