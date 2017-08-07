package algorithm;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class TopKApp {

	static final String INPUT_PATH = "hdfs://centos:9000/sort";
	static final String OUTPUT_PATH = "hdfs://centos:9000/out";

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		final FileSystem fileSystem = FileSystem.get(new URI(INPUT_PATH), conf);
		final Path outPath = new Path(OUTPUT_PATH);
		if (fileSystem.exists(outPath)) {
			fileSystem.delete(outPath, true);
		}

		final Job job = new Job(conf, TopKApp.class.getSimpleName());
		// 1.1指定读取的文件在哪里
		FileInputFormat.setInputPaths(job, INPUT_PATH);
		// 指定如何对输入的文件进行格式化，把输入的每一行解析成键值对
		job.setInputFormatClass(TextInputFormat.class);

		// 1.2指定自定义的mapper类
		job.setMapperClass(MyMapper.class);
		// 设定map输出的键值对的类型
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(NullWritable.class);

		// 1.3分区
		job.setPartitionerClass(HashPartitioner.class);
		// 有一个Reduce任务在运行
		job.setNumReduceTasks(1);

		// 1.4 TODO 排序、分组

		// 1.5 TODO 归约

		// 2.2指定自定义的reduce类
		job.setReducerClass(MyReducer.class);
		// 指定reduce输出的类型
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(NullWritable.class);

		// 2.3 指定输出到哪里
		FileOutputFormat.setOutputPath(job, outPath);
		// 指定输出文件的格式化类
		job.setOutputFormatClass(TextOutputFormat.class);

		// 把job交给JobTacker运行
		job.waitForCompletion(true);
	}

	
	/*
	 * this is changed
	 */
	static class MyMapper extends
			Mapper<LongWritable, Text, LongWritable, NullWritable> {
		long max = Long.MIN_VALUE;

		protected void map(LongWritable k1, Text v1, Context context)
				throws IOException, InterruptedException {
			final long temp = Long.parseLong(v1.toString());
			if (temp > max) {
				max = temp;
			}
		}

		protected void cleanup(	
				org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, LongWritable, NullWritable>.Context context)
				throws IOException, InterruptedException {
			context.write(new LongWritable(max), NullWritable.get());
		};

	}

	static class MyReducer extends
			Reducer<LongWritable, NullWritable, LongWritable, NullWritable> {
		long max = Long.MIN_VALUE;
		protected void reduce(LongWritable k2, Iterable<NullWritable> v2,
				Context context) throws IOException, InterruptedException {
			long temp = k2.get();
			if(temp > max){
				max = temp;
			}
		}
		
		protected void cleanup(org.apache.hadoop.mapreduce.Reducer<LongWritable,NullWritable,LongWritable,NullWritable>.Context context) throws IOException ,InterruptedException {
			context.write(new LongWritable(max), NullWritable.get());
		};
	}
}
