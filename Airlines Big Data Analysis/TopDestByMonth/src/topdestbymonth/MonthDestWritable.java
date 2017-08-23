/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package topdestbymonth;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

/**
 *
 * @author prasad
 */
public class MonthDestWritable implements Writable,WritableComparable<MonthDestWritable> {
    
    private String month;
    private String dest;
    
    public MonthDestWritable()
    {
        
    }
    
    public MonthDestWritable(String month, String dest)
    {
        this.month = month;
        this.dest = dest;
    }

    
    
     @Override
    public void write(DataOutput d) throws IOException {
        WritableUtils.writeString(d, month);
        WritableUtils.writeString(d, dest);
    }

    @Override
    public void readFields(DataInput di) throws IOException {
        month = WritableUtils.readString(di);
        dest = WritableUtils.readString(di);
    }

   
    
      public String toString()
    {
        return month + ":" + dest;
    }

    public String getMonth() {
        return month;
    }

    public void setMonth(String month) {
        this.month = month;
    }

    public String getDest() {
        return dest;
    }

    public void setDest(String dest) {
        this.dest = dest;
    }

    @Override
    public int compareTo(MonthDestWritable o) {
        int result = month.compareTo(o.month);
        if(result ==0)
        {
            result = dest.compareTo(o.dest);
        }
        return result;
    }
    
}
