/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package topdestbymonth;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 *
 * @author prasad
 */
public class MonthGroupComparator extends WritableComparator {
    
    protected MonthGroupComparator()
    {
        super(MonthDestWritable.class, true);
    }
    
    @Override
    public int compare(WritableComparable w1, WritableComparable w2)
    {
        MonthDestWritable cw1 = (MonthDestWritable) w1;
        MonthDestWritable cw2 = (MonthDestWritable) w2;
        
           return (cw1.getMonth().compareTo(cw2.getMonth()));
    }
}
