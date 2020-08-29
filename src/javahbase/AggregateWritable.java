/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package javahbase;

/**
 *
 * @author nouran
 */
import org.apache.hadoop.io.Writable;
import com.google.gson.Gson;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AggregateWritable implements Writable{
    private static Gson gson=new Gson();

    private AggregateData aggregateData= new AggregateData();
    
    public AggregateWritable(){}
    
    public AggregateWritable(AggregateData aggregateData){
        super();
        this.aggregateData=aggregateData;
    }
    public AggregateData getAggregateData(){
        return aggregateData;
    }

    @Override
    public void write(DataOutput d) throws IOException {
        d.writeLong(aggregateData.getOrderbelow5000());
        d.writeLong(aggregateData.getOrderbelow10000());
        d.writeLong(aggregateData.getOrderbelow20000());
        d.writeLong(aggregateData.getOrderabove20000());
        d.writeLong(aggregateData.getTotalOrder());
        //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void readFields(DataInput di) throws IOException {
        aggregateData.setOrderbelow5000(di.readLong());
        aggregateData.setOrderbelow10000(di.readLong());
        aggregateData.setOrderbelow20000(di.readLong());
        aggregateData.setOrderabove20000(di.readLong());
        aggregateData.setTotalOrder(di.readLong());
        //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    @Override
    public String toString(){
    return gson.toJson(aggregateData);
    }
}

