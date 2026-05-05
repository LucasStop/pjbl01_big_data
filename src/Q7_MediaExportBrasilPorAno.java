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

public class Q7_MediaExportBrasilPorAno {

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        Path input = new Path(files[0]);
        Path output = new Path(files[1]);

        Job j = new Job(c, "q7-media-export-brasil-por-ano");
        j.setJarByClass(Q7_MediaExportBrasilPorAno.class);
        j.setMapperClass(Map.class);
        j.setCombinerClass(Combine.class);
        j.setReducerClass(Reduce.class);

        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(SumCountWritable.class);

        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

    public static class Map extends Mapper<LongWritable, Text, Text, SumCountWritable> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {
            String linha = value.toString();

            if (linha.startsWith("country_or_area")) return;

            String[] campos = linha.split(";");
            if (campos.length < 10) return;

            String pais = campos[0].trim();
            String ano = campos[1].trim();
            String fluxo = campos[4].trim();
            String precoStr = campos[5].trim();

            if (pais.equals("Brazil") && fluxo.equals("Export") && !ano.isEmpty() && !precoStr.isEmpty()) {
                try {
                    double preco = Double.parseDouble(precoStr);
                    con.write(new Text(ano), new SumCountWritable(preco, 1));
                } catch (NumberFormatException e) {
                    // dado invalido, ignorar
                }
            }
        }
    }

    public static class Combine extends Reducer<Text, SumCountWritable, Text, SumCountWritable> {
        public void reduce(Text key, Iterable<SumCountWritable> values, Context con)
                throws IOException, InterruptedException {
            double somaTotal = 0.0;
            int contTotal = 0;

            for (SumCountWritable val : values) {
                somaTotal += val.getSum();
                contTotal += val.getCount();
            }

            con.write(key, new SumCountWritable(somaTotal, contTotal));
        }
    }

    public static class Reduce extends Reducer<Text, SumCountWritable, Text, Text> {
        public void reduce(Text key, Iterable<SumCountWritable> values, Context con)
                throws IOException, InterruptedException {
            double somaTotal = 0.0;
            int contTotal = 0;

            for (SumCountWritable val : values) {
                somaTotal += val.getSum();
                contTotal += val.getCount();
            }

            double media = contTotal > 0 ? somaTotal / contTotal : 0.0;
            con.write(key, new Text(String.format("%.2f", media)));
        }
    }
}
