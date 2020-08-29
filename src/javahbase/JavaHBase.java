/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package javahbase;

import java.net.URI;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author nouran
 */
public class JavaHBase extends Configured implements Tool {

    /**
     * @param args the command line arguments
     */
    public final static Logger logger= LoggerFactory.getLogger(JavaHBase.class);
    
    static SimpleDateFormat sdf= new SimpleDateFormat("yyyy-MM-dd");
    
    public static final class JavaHBasePartitioner extends Partitioner<Text, AggregateWritable>{
        @Override
        public int getPartition(Text key, AggregateWritable value, int numPartitions){
            return Math.abs(key.toString().hashCode()%numPartitions);
        }
    }
    
    public static void main(String[] args) throws Exception{
        // TODO code application logic here
        ToolRunner.run(new Configuration(), new JavaHBase(), args);
        System.exit(1);
    }
    public static int runMRJobs(String[] args) throws FileNotFoundException,IllegalArgumentException,IOException,ClassNotFoundException,InterruptedException{
        
        Configuration conf= new Configuration();
        
        ControlledJob mrJob1=new ControlledJob(conf);
        Job job=mrJob1.getJob();
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(AggregateWritable.class);
        job.setJarByClass(JavaHBase.class);
        
        job.setReducerClass(JavaHBaseReducer.class);
        
        FileInputFormat.setInputDirRecursive(job, true);
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, TransactionMapper.class);
        FileSystem filesystem =FileSystem.get(job.getConfiguration());
        RemoteIterator<LocatedFileStatus> files=filesystem.listFiles(new Path(args[1]), true);
        
        while(files.hasNext()){
            job.addCacheArchive(files.next().getPath().toUri());
        }
        FileOutputFormat.setOutputPath(job, new Path(args[2]+"/"+Calendar.getInstance().getTimeInMillis()));
        job.setNumReduceTasks(5);
        job.setPartitionerClass(JavaHBasePartitioner.class);
        
        
        return job.waitForCompletion(true)?0:1;
        
}
    public static class TransactionMapper extends Mapper<LongWritable,Text,Text,AggregateWritable>{
    
        @Override
        protected void map(LongWritable key,Text value,
                Mapper<LongWritable,Text,Text,AggregateWritable>.Context context)
                throws IOException, InterruptedException
        {
            String line=value.toString().replace("\"","");
            
            if(line.indexOf("transaction")!=-1){return;}
            String split[]=line.split(",");
            Transaction transaction= new Transaction();
            transaction.setTxId(split[0]);
            transaction.setCustomerId(Long.parseLong(split[1]));
            transaction.setMerchantId(Long.parseLong(split[2]));
            transaction.setTimestamp(split[3]);
            transaction.setInvoiceNumber(split[4].trim());
            transaction.setInvoiceAmount(Float.parseFloat(split[5]));
            transaction.setSegment(split[6].trim());
        
            AggregateData aggregateData = new AggregateData();
            AggregateWritable aggregateWritable=new AggregateWritable(aggregateData);
            
            if(transaction.getInvoiceAmount()<=5000){
            aggregateData.setOrderbelow5000(1l);}
            else if(transaction.getInvoiceAmount()<=10000){
            aggregateData.setOrderbelow10000(1l);}
            else if(transaction.getInvoiceAmount()<=20000){
            aggregateData.setOrderbelow20000(1l);}
            else if(transaction.getInvoiceAmount()>20000){
            aggregateData.setOrderabove20000(1l);}
            
            aggregateData.setTotalOrder(1l);
            
            String outputKey = HBaseIdMap.get((transaction.getMerchantId().toString())+"-"+(split[3].trim().split(" ")[0].trim()));
            
            context.write(new Text(outputKey),aggregateWritable);
            
        }
        
    }
    private static class JavaHBaseReducer extends Reducer<Text,AggregateWritable,Text,AggregateWritable>{
        public void reduce(Text key,Iterable<AggregateWritable> values,Context context)
                throws IOException,InterruptedException{
            AggregateData aggregateData=new AggregateData();
            AggregateWritable aggregateWritable=new AggregateWritable(aggregateData);
            
            for(AggregateWritable val: values){
                aggregateData.setOrderabove20000(aggregateData.getOrderabove20000()+val.getAggregateData().getOrderabove20000());
                aggregateData.setOrderabove20000(aggregateData.getOrderbelow20000()+val.getAggregateData().getOrderbelow20000());
                aggregateData.setOrderabove20000(aggregateData.getOrderbelow10000()+val.getAggregateData().getOrderbelow10000());
                aggregateData.setOrderabove20000(aggregateData.getOrderbelow5000()+val.getAggregateData().getOrderbelow5000());
                
                aggregateData.setTotalOrder(val.getAggregateData().getTotalOrder()+aggregateData.getTotalOrder());
                
            }
            context.write(key,aggregateWritable);
        }
    }
    
    private static Map<String,String> HBaseIdMap=new HashMap<String, String>();
    
        protected void setup(Mapper<LongWritable,Text,Text,AggregateWritable>.Context context) throws IOException,InterruptedException
    {
            URI[] paths= context.getCacheArchives();
            if(paths != null)
            {
                for(URI path:paths){
                    loadHBaseInCash(path.toString(),context.getConfiguration());
                }
            }

            super.setup(context);
    }
    private void loadHBaseInCash(String file,Configuration conf){
    
        logger.info("File Name"+file);
        String strRead;
        BufferedReader br=null;
        try{
           FileSystem filesystem=FileSystem.get(conf);
            FSDataInputStream open=filesystem.open(new Path(file));
            br = new BufferedReader(new InputStreamReader(open));
            while((strRead=br.readLine())!=null){
                String line=strRead.toString().replace("\"","");
                String splitarray[]=line.split(",");
                HBaseIdMap.put(splitarray[0].toString(),splitarray[2].toString());
            }
        
        } catch (Exception ex) {
            logger.error(ex.toString());
        }
        finally{
            try{
                if(br!=null){br.close();}
            }
            catch (Exception ex) {
            logger.error(ex.toString());
        }
        }
    }
    
    @Override
    public int run(String[] strings) throws Exception {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    
   
}
