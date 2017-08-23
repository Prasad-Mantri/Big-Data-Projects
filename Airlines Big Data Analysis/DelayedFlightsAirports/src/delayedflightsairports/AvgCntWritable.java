/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package delayedflightsairports;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

/**
 *
 * @author prasad
 */
public class AvgCntWritable implements Writable {
    
    private String avrg;
    private String count;
    
    public AvgCntWritable()
    {
        
    }
    
    public AvgCntWritable(String avrg, String count)
    {
        this.avrg = avrg;
        this.count = count;
    }

    
    
     @Override
    public void write(DataOutput d) throws IOException {
        WritableUtils.writeString(d, avrg);
        WritableUtils.writeString(d, count);
    }

    @Override
    public void readFields(DataInput di) throws IOException {
        avrg = WritableUtils.readString(di);
        count = WritableUtils.readString(di);
    }

    public String getAvrg() {
        return avrg;
    }

    public void setAvrg(String avrg) {
        this.avrg = avrg;
    }

    public String getCount() {
        return count;
    }

    public void setCount(String count) {
        this.count = count;
    }
    
      public String toString()
    {
        return avrg.toString();
    }
    
}
