import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * Orquestrador do PjBL01 — executa as 8 questões em sequência e imprime
 * cada pergunta seguida da respectiva resposta.
 *
 * Uso:
 *
 * Se outputBase for omitido, usa "output".
 */
public class RunAll {

    private static final String[] PERGUNTAS = {
        "Numero de transacoes envolvendo o Brasil.",
        "Numero de transacoes por ano.",
        "Numero de transacoes por categoria.",
        "Numero de transacoes por tipo de fluxo (flow).",
        "Valor medio das transacoes por ano somente no Brasil.",
        "Transacao mais cara e mais barata no Brasil em 2016.",
        "Valor medio das transacoes por ano (Export) no Brasil.",
        "Transacao com maior e menor amount, por ano e pais."
    };

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        Logger.getRootLogger().setLevel(Level.WARN);
        // Silencia avisos repetitivos do Hadoop em modo local
        Logger.getLogger("org.apache.hadoop.util.NativeCodeLoader").setLevel(Level.ERROR);
        Logger.getLogger("org.apache.hadoop.mapreduce.JobResourceUploader").setLevel(Level.ERROR);
        Logger.getLogger("org.apache.hadoop.metrics2.impl.MetricsSystemImpl").setLevel(Level.ERROR);
        Logger.getLogger("org.apache.hadoop.metrics2.impl.MetricsConfig").setLevel(Level.ERROR);

        String input;
        if (args.length >= 1) {
            input = args[0];
        } else {
            input = autoDetectarCsv();
            if (input == null) {
                System.err.println("Uso: RunAll <input.csv> [outputBase]");
                System.err.println("Nenhum .csv encontrado em " + new File(".").getAbsolutePath());
                System.exit(1);
            }
            System.out.println("==> CSV auto-detectado: " + input);
        }
        String outputBase = args.length > 1 ? args[1] : "output";

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        // Limpa outputs anteriores
        for (int i = 1; i <= 8; i++) {
            Path p = new Path(outputBase + "/q" + i);
            if (fs.exists(p)) fs.delete(p, true);
        }

        long inicio = System.currentTimeMillis();

        executar(fs, outputBase, 1, "Transacoes Brasil",          out -> runQ1(conf, input, out));
        executar(fs, outputBase, 2, "Transacoes por ano",         out -> runQ2(conf, input, out));
        executar(fs, outputBase, 3, "Transacoes por categoria",   out -> runQ3(conf, input, out));
        executar(fs, outputBase, 4, "Transacoes por fluxo",       out -> runQ4(conf, input, out));
        executar(fs, outputBase, 5, "Media Brasil por ano",       out -> runQ5(conf, input, out));
        executar(fs, outputBase, 6, "MinMax Brasil 2016",         out -> runQ6(conf, input, out));
        executar(fs, outputBase, 7, "Media Export Brasil/ano",    out -> runQ7(conf, input, out));
        executar(fs, outputBase, 8, "MinMax amount por ano/pais", out -> runQ8(conf, input, out));

        long fim = System.currentTimeMillis();

        System.out.println();
        System.out.println("====================================================");
        System.out.println("            RESULTADOS DO PJBL01 - MAPREDUCE        ");
        System.out.println("====================================================");
        for (int i = 1; i <= 8; i++) {
            System.out.println();
            System.out.println("Pergunta " + i + ": " + PERGUNTAS[i - 1]);
            System.out.println("Resposta:");
            printOutput(fs, outputBase + "/q" + i, i == 8 ? 15 : Integer.MAX_VALUE);
        }
        System.out.println();
        System.out.println("====================================================");
        System.out.printf("Tempo total: %.1f s%n", (fim - inicio) / 1000.0);
        System.out.println("Resultados completos em: " + outputBase + "/q*/part-r-00000");
        System.out.println("====================================================");
    }

    // ---------- Configuração de cada Job ----------

    private static void runQ1(Configuration conf, String in, String out) throws Exception {
        Job j = Job.getInstance(conf, "q1-transacoes-brasil");
        j.setJarByClass(RunAll.class);
        j.setMapperClass(Q1_TransacoesBrasil.Map.class);
        j.setReducerClass(Q1_TransacoesBrasil.Reduce.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(j, new Path(in));
        FileOutputFormat.setOutputPath(j, new Path(out));
        if (!j.waitForCompletion(true)) throw new RuntimeException("Q1 falhou");
    }

    private static void runQ2(Configuration conf, String in, String out) throws Exception {
        Job j = Job.getInstance(conf, "q2-transacoes-por-ano");
        j.setJarByClass(RunAll.class);
        j.setMapperClass(Q2_TransacoesPorAno.Map.class);
        j.setReducerClass(Q2_TransacoesPorAno.Reduce.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(j, new Path(in));
        FileOutputFormat.setOutputPath(j, new Path(out));
        if (!j.waitForCompletion(true)) throw new RuntimeException("Q2 falhou");
    }

    private static void runQ3(Configuration conf, String in, String out) throws Exception {
        Job j = Job.getInstance(conf, "q3-transacoes-por-categoria");
        j.setJarByClass(RunAll.class);
        j.setMapperClass(Q3_TransacoesPorCategoria.Map.class);
        j.setReducerClass(Q3_TransacoesPorCategoria.Reduce.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(j, new Path(in));
        FileOutputFormat.setOutputPath(j, new Path(out));
        if (!j.waitForCompletion(true)) throw new RuntimeException("Q3 falhou");
    }

    private static void runQ4(Configuration conf, String in, String out) throws Exception {
        Job j = Job.getInstance(conf, "q4-transacoes-por-fluxo");
        j.setJarByClass(RunAll.class);
        j.setMapperClass(Q4_TransacoesPorFluxo.Map.class);
        j.setReducerClass(Q4_TransacoesPorFluxo.Reduce.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(j, new Path(in));
        FileOutputFormat.setOutputPath(j, new Path(out));
        if (!j.waitForCompletion(true)) throw new RuntimeException("Q4 falhou");
    }

    private static void runQ5(Configuration conf, String in, String out) throws Exception {
        Job j = Job.getInstance(conf, "q5-media-brasil-por-ano");
        j.setJarByClass(RunAll.class);
        j.setMapperClass(Q5_MediaBrasilPorAno.Map.class);
        j.setReducerClass(Q5_MediaBrasilPorAno.Reduce.class);
        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(SumCountWritable.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(j, new Path(in));
        FileOutputFormat.setOutputPath(j, new Path(out));
        if (!j.waitForCompletion(true)) throw new RuntimeException("Q5 falhou");
    }

    private static void runQ6(Configuration conf, String in, String out) throws Exception {
        Job j = Job.getInstance(conf, "q6-minmax-brasil-2016");
        j.setJarByClass(RunAll.class);
        j.setMapperClass(Q6_MinMaxBrasil2016.Map.class);
        j.setCombinerClass(Q6_MinMaxBrasil2016.Combine.class);
        j.setReducerClass(Q6_MinMaxBrasil2016.Reduce.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(Q6_MinMaxWritable.class);
        FileInputFormat.addInputPath(j, new Path(in));
        FileOutputFormat.setOutputPath(j, new Path(out));
        if (!j.waitForCompletion(true)) throw new RuntimeException("Q6 falhou");
    }

    private static void runQ7(Configuration conf, String in, String out) throws Exception {
        Job j = Job.getInstance(conf, "q7-media-export-brasil-por-ano");
        j.setJarByClass(RunAll.class);
        j.setMapperClass(Q7_MediaExportBrasilPorAno.Map.class);
        j.setCombinerClass(Q7_MediaExportBrasilPorAno.Combine.class);
        j.setReducerClass(Q7_MediaExportBrasilPorAno.Reduce.class);
        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(SumCountWritable.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(j, new Path(in));
        FileOutputFormat.setOutputPath(j, new Path(out));
        if (!j.waitForCompletion(true)) throw new RuntimeException("Q7 falhou");
    }

    private static void runQ8(Configuration conf, String in, String out) throws Exception {
        Job j = Job.getInstance(conf, "q8-minmax-por-ano-pais");
        j.setJarByClass(RunAll.class);
        j.setMapperClass(Q8_MinMaxPorAnoPais.Map.class);
        j.setCombinerClass(Q8_MinMaxPorAnoPais.Combine.class);
        j.setReducerClass(Q8_MinMaxPorAnoPais.Reduce.class);
        j.setOutputKeyClass(Q8_YearCountryWritable.class);
        j.setOutputValueClass(Q8_MinMaxAmountWritable.class);
        FileInputFormat.addInputPath(j, new Path(in));
        FileOutputFormat.setOutputPath(j, new Path(out));
        if (!j.waitForCompletion(true)) throw new RuntimeException("Q8 falhou");
    }

    // ---------- Helper de execução com progresso ----------

    @FunctionalInterface
    private interface JobRunnable {
        void run(String outputDir) throws Exception;
    }

    private static void executar(FileSystem fs, String outputBase, int n, String nome, JobRunnable r) throws Exception {
        String dir = outputBase + "/q" + n;
        String txt = outputBase + "/q" + n + "_resultado.txt";
        System.out.printf("%n==> [%d/8] Executando: %s%n", n, nome);
        long t0 = System.currentTimeMillis();
        r.run(dir);
        copiarResultado(fs, dir, txt);
        System.out.printf("    OK em %.1fs  ->  %s%n", (System.currentTimeMillis() - t0) / 1000.0, txt);
    }

    private static void copiarResultado(FileSystem fs, String dir, String destTxt) throws Exception {
        Path src = new Path(dir + "/part-r-00000");
        if (!fs.exists(src)) return;
        // Le via Hadoop FS, escreve via Java NIO puro para evitar arquivos .crc
        java.nio.file.Path dst = Paths.get(destTxt);
        try (InputStream in = fs.open(src);
             OutputStream out = Files.newOutputStream(dst,
                     StandardOpenOption.CREATE,
                     StandardOpenOption.TRUNCATE_EXISTING)) {
            byte[] buf = new byte[8192];
            int n;
            while ((n = in.read(buf)) > 0) out.write(buf, 0, n);
        }
    }

    // ---------- Auto-detecção do CSV ----------

    private static String autoDetectarCsv() {
        File dir = new File(".");
        File[] csvs = dir.listFiles((d, nome) -> nome.toLowerCase().endsWith(".csv"));
        if (csvs == null || csvs.length == 0) return null;
        if (csvs.length > 1) {
            System.err.println("Múltiplos .csv encontrados — passe o caminho como argumento:");
            for (File c : csvs) System.err.println("  - " + c.getName());
            return null;
        }
        return csvs[0].getPath();
    }

    // ---------- Leitura e impressão dos resultados ----------

    private static void printOutput(FileSystem fs, String dirPath, int maxLines) throws Exception {
        Path dir = new Path(dirPath);
        FileStatus[] files = fs.listStatus(dir);
        int count = 0;
        for (FileStatus f : files) {
            String nome = f.getPath().getName();
            if (!nome.startsWith("part-")) continue;
            try (BufferedReader r = new BufferedReader(new InputStreamReader(fs.open(f.getPath())))) {
                String linha;
                while ((linha = r.readLine()) != null) {
                    if (count >= maxLines) {
                        System.out.println("  ... (truncado; veja " + dirPath + "/part-r-00000)");
                        return;
                    }
                    System.out.println("  " + linha);
                    count++;
                }
            }
        }
    }
}
