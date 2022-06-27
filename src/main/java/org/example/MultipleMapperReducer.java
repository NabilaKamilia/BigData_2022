package org.example;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MultipleMapperReducer {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "Perbandingan Produksi Tanaman Buah Sayuran Job");
        job.setMapperClass(ProduksiMapper.class);
        System.out.println(new Path(args[0]));
        System.out.println(new Path(args[1]));
        MultipleInputs.addInputPath(job, new Path(args[0]),
                TextInputFormat.class, TanamanMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]),
                TextInputFormat.class, ProduksiMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(ProduksiReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    private static class TanamanMapper
            extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String txt = value.toString();
            String[] tokens = txt.split(",");
            String id = tokens[0].trim();
            String jenisTanaman = tokens[1].trim();
            if (jenisTanaman.compareTo("Jenis Tanaman") != 0)
                context.write(new Text(id), new Text(jenisTanaman));
        }
    }

    private static class ProduksiMapper
            extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String txt = value.toString();
            String[] tokens = txt.split(",");
            String luaspanen = tokens[0];
            String id = tokens[1].trim();
            String produksi = tokens[2].trim();
            if (produksi.compareTo("Produksi") != 0)
                context.write(new Text(id), new Text(produksi));
        }
    }

    private static class ProduksiReducer
            extends Reducer<Text, Text, Text, IntWritable> {
        private final IntWritable result = new IntWritable();
        private Text tanamanName = new Text("unknown");

        public void reduce(Text key, Iterable<Text> values,
                           Context context) throws IOException, InterruptedException {
            int sum = 0;
            int n = 0;
//            tanamanName = new Text("jenistanaman-" + key.toString());
            for (Text val : values) {
                String strVal = val.toString();
                if (strVal.length() <= 3) {
                    sum += Integer.parseInt(strVal);
                    n += 1;
                } else {
                    tanamanName = new Text(strVal);
                }
            }

//            if (n == 0) n = 1;
//            result.set(sum / n);
//            context.write(tanamanName, result);

            //Inner Join
//            if (n != 0 && tanamanName.toString().compareTo("Unknown") != 0) {
//                result.set(sum / n);
//                context.write(tanamanName, result);
//            }

            //Left anti Join
//            if (n == 0) {
//                if (n == 0 ) n =1;
//                result.set(sum / n);
//                context.write(tanamanName, result);
//            }

            //left Outer Join
//            if (tanamanName.toString().compareTo("Unknown") != 0){
//                if (n == 0) n = 1;
//                result.set(sum / n);
//                context.write(tanamanName, result);
//            }

            //Right Outer Join
//            if (n != 0) {
//                result.set(sum / n);
//                context.write(tanamanName, result);
//            }

            //Full Outer Join
            if (n == 0) n = 1;
            result.set(sum / n);
            context.write(tanamanName, result);
        }
    }
}
