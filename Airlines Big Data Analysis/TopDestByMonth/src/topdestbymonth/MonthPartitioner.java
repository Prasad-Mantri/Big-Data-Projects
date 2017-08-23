/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package topdestbymonth;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 *
 * @author prasad
 */
public class MonthPartitioner extends Partitioner<IntWritable, Text> implements Configurable {

    private static final String MIN_MONTH = "min.month";

    private Configuration conf = null;
    private int minMonth = 0;

    public int getPartition(IntWritable key, Text value, int numPartitions) {
        return key.get();
    }

    public Configuration getConf() {
        return conf;
    }

    public void setConf(Configuration conf) {
        this.conf = conf;
        minMonth = conf.getInt(MIN_MONTH, 0);
    }

    public static void setMinMonth(Job job, int minMonth) {
        job.getConfiguration().setInt(MIN_MONTH, minMonth);
    }   
}
