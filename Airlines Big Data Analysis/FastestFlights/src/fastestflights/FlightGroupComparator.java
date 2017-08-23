/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package fastestflights;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 *
 * @author prasad
 */
public class FlightGroupComparator extends WritableComparator {
    
    protected FlightGroupComparator()
    {
        super(OrgDestMonthWritable.class, true);
    }
    
    @Override
    public int compare(WritableComparable w1, WritableComparable w2)
    {
        OrgDestMonthWritable ow1 = (OrgDestMonthWritable) w1;
        OrgDestMonthWritable ow2 = (OrgDestMonthWritable) w2;
        
           return (ow1.getMonth().compareTo(ow2.getMonth()));
    }
}
