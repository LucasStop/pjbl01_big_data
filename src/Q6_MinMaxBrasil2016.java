import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

public class Q6_MinMaxBrasil2016 {

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        Path input = new Path(files[0]);
        Path output = new Path(files[1]);

        Job j = new Job(c, "q6-minmax-brasil-2016");
        j.setJarByClass(Q6_MinMaxBrasil2016.class);
        j.setMapperClass(Map.class);
        j.setCombinerClass(Combine.class);
        j.setReducerClass(Reduce.class);

        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(Q6_MinMaxWritable.class);

        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

    public static class Map extends Mapper<LongWritable, Text, Text, Q6_MinMaxWritable> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {
            String linha = value.toString();

            if (linha.startsWith("country_or_area")) return;

            String[] campos = linha.split(";");
            if (campos.length < 10) return;

            String pais = campos[0].trim();
            String ano = campos[1].trim();
            String precoStr = campos[5].trim();

            if (pais.equals("Brazil") && ano.equals("2016") && !precoStr.isEmpty()) {
                try {
                    double preco = Double.parseDouble(precoStr);
                    con.write(new Text("Brazil"), new Q6_MinMaxWritable(preco, preco));
                } catch (NumberFormatException e) {
                    // dado invalido, ignorar
                }
            }
        }
    }

    public static class Combine extends Reducer<Text, Q6_MinMaxWritable, Text, Q6_MinMaxWritable> {
        public void reduce(Text key, Iterable<Q6_MinMaxWritable> values, Context con)
                throws IOException, InterruptedException {
            double min = Double.MAX_VALUE;
            double max = -Double.MAX_VALUE;

            for (Q6_MinMaxWritable val : values) {
                if (val.getMin() < min) min = val.getMin();
                if (val.getMax() > max) max = val.getMax();
            }

            con.write(key, new Q6_MinMaxWritable(min, max));
        }
    }

    public static class Reduce extends Reducer<Text, Q6_MinMaxWritable, Text, Q6_MinMaxWritable> {
        public void reduce(Text key, Iterable<Q6_MinMaxWritable> values, Context con)
                throws IOException, InterruptedException {
            double min = Double.MAX_VALUE;
            double max = -Double.MAX_VALUE;

            for (Q6_MinMaxWritable val : values) {
                if (val.getMin() < min) min = val.getMin();
                if (val.getMax() > max) max = val.getMax();
            }

            con.write(key, new Q6_MinMaxWritable(min, max));
        }
    }
}
