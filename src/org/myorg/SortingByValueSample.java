package org.myorg;

import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

import org.myorg.KeyWritable;

public class SortingByValueSample {
    
    public static final String DELIM = "\t";
    
 
    
    public static class MyPartitioner<K, V> extends Partitioner<K, V> {
        public int getPartition(K key, V value, int numPartitions) {           
            return (key.hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }
    
    public static class MyGroupingComparator implements RawComparator<KeyWritable> {
        
        protected static final WritableComparator textComparator = new Text.Comparator();

        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {

            int intSize = Integer.SIZE / Byte.SIZE;
            return textComparator.compare(b1, s1, l1-intSize, b2, s2, l2-intSize);
        }

        public int compare(KeyWritable o1, KeyWritable o2) {
            return o1.getRealKey().compareTo(o2.getRealKey());
        }
    }
    
    public static class MySortComparator implements RawComparator<KeyWritable> {
        protected static final WritableComparator intComparator = new IntWritable.Comparator();
        protected static final WritableComparator textComparator = new Text.Comparator();

        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
                     
            int intSize = Integer.SIZE / Byte.SIZE;
            int nameRet = textComparator.compare(b1, s1, l1-intSize, b2, s2, l2-intSize);
            
            if (nameRet == 0) {
                return intComparator.compare(b1, s1+l1-intSize, intSize, b2, s2+l2-intSize, intSize);
            } else {
                return nameRet;                
            }
        }

        public int compare(KeyWritable o1, KeyWritable o2) {
            int nameRet =  o1.getRealKey().compareTo(o2.getRealKey());
            
            if (nameRet == 0) {
                return o1.getVal() - o2.getVal();
            } else {
                return nameRet;
            }
        }
    }

    public static class Map extends Mapper<LongWritable, Text, KeyWritable, IntWritable> {
        private KeyWritable outKey = new KeyWritable();
        private Random rd = new Random();

        public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            while (tokenizer.hasMoreTokens()) {
                int rdNum = rd.nextInt();
                outKey.set(rdNum, tokenizer.nextToken());
                context.write(outKey, new IntWritable(rdNum));
            }
        }
    }

    public static class Reduce extends Reducer<KeyWritable, IntWritable, Text, IntWritable> {
        
        public void reduce(KeyWritable key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
            int sum = 0;
            System.err.println("key: " + key.getRealKey());
            for (IntWritable val : values) {
                System.err.println("  val: " + val.toString());
                sum += 1;
            }
            context.write(new Text(key.getRealKey()), new IntWritable(sum));
        }
        
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] remainArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        
        if (remainArgs.length != 2) {
            System.err.println("Usage: SortingByValueSample <input> <output>");
            System.exit(1);
        }
        
        Job job = new Job(conf, "SortingByValueSample");
        job.setJarByClass(SortingByValueSample.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        
        job.setMapOutputKeyClass(KeyWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        
        job.setNumReduceTasks(4);
        job.setPartitionerClass(MyPartitioner.class);
        job.setSortComparatorClass(MySortComparator.class);
        job.setGroupingComparatorClass(MyGroupingComparator.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        FileSystem.get(conf).delete(new Path(remainArgs[1]), true);

        FileInputFormat.setInputPaths(job, new Path(remainArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(remainArgs[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
 