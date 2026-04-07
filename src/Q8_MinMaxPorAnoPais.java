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

public class Q8_MinMaxPorAnoPais {

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        Path input = new Path(files[0]);
        Path output = new Path(files[1]);

        Job j = new Job(c, "q8-minmax-por-ano-pais");
        j.setJarByClass(Q8_MinMaxPorAnoPais.class);
        j.setMapperClass(Map.class);
        j.setCombinerClass(Combine.class);
        j.setReducerClass(Reduce.class);

        j.setOutputKeyClass(Q8_YearCountryWritable.class);
        j.setOutputValueClass(Q8_MinMaxAmountWritable.class);

        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

    public static class Map extends Mapper<LongWritable, Text, Q8_YearCountryWritable, Q8_MinMaxAmountWritable> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {
            String linha = value.toString();

            if (linha.startsWith("country_or_area")) return;

            String[] campos = linha.split(";");
            if (campos.length < 10) return;

            String pais = campos[0].trim();
            String ano = campos[1].trim();
            String amountStr = campos[8].trim();

            if (!pais.isEmpty() && !ano.isEmpty() && !amountStr.isEmpty()) {
                try {
                    double amount = Double.parseDouble(amountStr);
                    con.write(
                        new Q8_YearCountryWritable(ano, pais),
                        new Q8_MinMaxAmountWritable(amount, amount)
                    );
                } catch (NumberFormatException e) {
                    // dado invalido, ignorar
                }
            }
        }
    }

    public static class Combine extends Reducer<Q8_YearCountryWritable, Q8_MinMaxAmountWritable, Q8_YearCountryWritable, Q8_MinMaxAmountWritable> {
        public void reduce(Q8_YearCountryWritable key, Iterable<Q8_MinMaxAmountWritable> values, Context con)
                throws IOException, InterruptedException {
            double min = Double.MAX_VALUE;
            double max = -Double.MAX_VALUE;

            for (Q8_MinMaxAmountWritable val : values) {
                if (val.getMin() < min) min = val.getMin();
                if (val.getMax() > max) max = val.getMax();
            }

            con.write(key, new Q8_MinMaxAmountWritable(min, max));
        }
    }

    public static class Reduce extends Reducer<Q8_YearCountryWritable, Q8_MinMaxAmountWritable, Q8_YearCountryWritable, Q8_MinMaxAmountWritable> {
        public void reduce(Q8_YearCountryWritable key, Iterable<Q8_MinMaxAmountWritable> values, Context con)
                throws IOException, InterruptedException {
            double min = Double.MAX_VALUE;
            double max = -Double.MAX_VALUE;

            for (Q8_MinMaxAmountWritable val : values) {
                if (val.getMin() < min) min = val.getMin();
                if (val.getMax() > max) max = val.getMax();
            }

            con.write(key, new Q8_MinMaxAmountWritable(min, max));
        }
    }
}
