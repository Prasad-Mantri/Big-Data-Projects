/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package fastestflights;

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
public class OrgDestMonthWritable implements Writable,WritableComparable<OrgDestMonthWritable> {
    private String origin;
    private String dest;
    private String month;
    
    public OrgDestMonthWritable(){

     }
     public OrgDestMonthWritable(String origin, String dest, String month){
           this.origin = origin;
           this.dest = dest;
           this.month = month;
     }
    
    
    @Override
    public void write(DataOutput d) throws IOException {
        WritableUtils.writeString(d, origin);
        WritableUtils.writeString(d, dest);
        WritableUtils.writeString(d, month);
    }

    @Override
    public void readFields(DataInput di) throws IOException {
       origin = WritableUtils.readString(di);
        dest = WritableUtils.readString(di);
        month = WritableUtils.readString(di);
    }

    public String getOrigin() {
        return origin;
    }

    public void setOrigin(String origin) {
        this.origin = origin;
    }

    public String getDest() {
        return dest;
    }

    public void setDest(String dest) {
        this.dest = dest;
    }

    public String getMonth() {
        return month;
    }

    public void setMonth(String month) {
        this.month = month;
    }

    @Override
    public String toString() {
        return origin + " ," + dest + " ," + month ;
    }

 

    @Override
    public int compareTo(OrgDestMonthWritable o) {
        int result = origin.compareTo(o.origin);
        if(result ==0)
        {            
            result = dest.compareTo(o.dest);
        }else{
            
            result = month.compareTo(o.month);
          }
        
        return result;
    }
    
    
    
    
}
