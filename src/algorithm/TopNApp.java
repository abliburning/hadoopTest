package algorithm;

import java.io.IOException;
import java.net.URI;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TopNApp {
	static final String INPUT_PATH = "hdfs://centos:9000/sort";
	static final String OUTPUT_PATH = "hdfs://centos:9000/out";
    public static class TopTenMapper extends
            Mapper<Object, Text, NullWritable, IntWritable> {
        private TreeMap<Integer, String> repToRecordMap = new TreeMap<Integer, String>();

        public void map(Object key, Text value, Context context) {
            int N = 10; //默认为Top10
            N = Integer.parseInt(context.getConfiguration().get("N"));
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                repToRecordMap.put(Integer.parseInt(itr.nextToken()), " ");
                if (repToRecordMap.size() > N) {
                    repToRecordMap.remove(repToRecordMap.firstKey());
                }
            }
        }

        

        protected void cleanup(Context context) {
            for (Integer i : repToRecordMap.keySet()) {
                try {
                    context.write(NullWritable.get(), new IntWritable(i));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static class TopTenReducer extends
            Reducer<NullWritable, IntWritable, NullWritable, IntWritable> {
        private TreeMap<Integer, String> repToRecordMap = new TreeMap<Integer, String>();

        public void reduce(NullWritable key, Iterable<IntWritable> values,
                Context context) throws IOException, InterruptedException {
            int N = 10; //默认为Top10
            N = Integer.parseInt(context.getConfiguration().get("N"));
            for (IntWritable value : values) {
                repToRecordMap.put(value.get(), " ");
                if (repToRecordMap.size() > N) {
                    repToRecordMap.remove(repToRecordMap.firstKey());
                }
            }
            for (Integer i : repToRecordMap.descendingMap().keySet()) {
                context.write(NullWritable.get(), new IntWritable(i));
            }
        }

    }

    public static void main(String[] args) throws Exception {
    	Configuration conf = new Configuration();
		final FileSystem fileSystem = FileSystem.get(new URI(INPUT_PATH), conf);
		final Path outPath = new Path(OUTPUT_PATH);
		if (fileSystem.exists(outPath)) {
			fileSystem.delete(outPath, true);
		}
//        if (args.length != 3) {
//            throw new IllegalArgumentException(
//                    "!!!!!!!!!!!!!! Usage!!!!!!!!!!!!!!: hadoop jar <jar-name> "
//                    + "TopN.TopN "
//                    + "<the value of N>"
//                    + "<input-path> "
//                    + "<output-path>");
//        }
        conf.set("N", "5");
        final Job job = new Job(conf, TopKApp.class.getSimpleName());
        job.setJobName("TopN");
        FileInputFormat.setInputPaths(job, INPUT_PATH);
        FileOutputFormat.setOutputPath(job, outPath);
        job.setJarByClass(TopNApp.class);
        job.setMapperClass(TopTenMapper.class);
        job.setReducerClass(TopTenReducer.class);
        job.setNumReduceTasks(1);

        job.setMapOutputKeyClass(NullWritable.class);// map阶段的输出的key
        job.setMapOutputValueClass(IntWritable.class);// map阶段的输出的value

        job.setOutputKeyClass(NullWritable.class);// reduce阶段的输出的key
        job.setOutputValueClass(IntWritable.class);// reduce阶段的输出的value

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}