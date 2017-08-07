package group;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URI;

import mapReduce.WordCount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class GroupAPP {

	static final String INPUT_PATH = "hdfs://centos:9000/sort";
	static final String OUTPUT_PATH = "hdfs://centos:9000/out";

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		final FileSystem fileSystem = FileSystem.get(new URI(INPUT_PATH), conf);
		final Path outPath = new Path(OUTPUT_PATH);
		if (fileSystem.exists(outPath)) {
			fileSystem.delete(outPath, true);
		}

		final Job job = new Job(conf, WordCount.class.getSimpleName());
		// 1.1ָ����ȡ���ļ�������
		FileInputFormat.setInputPaths(job, INPUT_PATH);
		// ָ����ζ�������ļ����и�ʽ�����������ÿһ�н����ɼ�ֵ��
		job.setInputFormatClass(TextInputFormat.class);

		// 1.2ָ���Զ����mapper��
		job.setMapperClass(MyMapper.class);
		// �趨map����ļ�ֵ�Ե�����
		job.setMapOutputKeyClass(NewK2.class);
		job.setMapOutputValueClass(LongWritable.class);

		// 1.3����
		job.setPartitionerClass(HashPartitioner.class);
		// ��һ��Reduce����������
		job.setNumReduceTasks(1);

		// 1.4 TODO ���򡢷���
		job.setGroupingComparatorClass(MyGroupingComparator.class);
		// 1.5 TODO ��Լ

		// 2.2ָ���Զ����reduce��
		job.setReducerClass(MyReducer.class);
		// ָ��reduce���������
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(LongWritable.class);

		// 2.3 ָ�����������
		FileOutputFormat.setOutputPath(job, outPath);
		// ָ������ļ��ĸ�ʽ����
		job.setOutputFormatClass(TextOutputFormat.class);

		// ��job����JobTacker����
		job.waitForCompletion(true);
	}

	static class MyMapper extends
			Mapper<LongWritable, Text, NewK2, LongWritable> {
		protected void map(
				LongWritable key,
				Text value,
				org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, NewK2, LongWritable>.Context context)
				throws java.io.IOException, InterruptedException {
			final String[] splited = value.toString().split("\t");
			final NewK2 k2 = new NewK2(Long.parseLong(splited[0]),
					Long.parseLong(splited[1]));
			final LongWritable v2 = new LongWritable(Long.parseLong(splited[1]));
			context.write(k2, v2);
		};
	}

	static class MyReducer extends
			Reducer<NewK2, LongWritable, LongWritable, LongWritable> {
		protected void reduce(
				NewK2 k2,
				java.lang.Iterable<LongWritable> v2s,
				org.apache.hadoop.mapreduce.Reducer<NewK2, LongWritable, LongWritable, LongWritable>.Context context)
				throws java.io.IOException, InterruptedException {
			long min = Long.MAX_VALUE;
			for (LongWritable v2 : v2s) {
				if (v2.get() < min) {
					min = v2.get();
				}
			}
			context.write(new LongWritable(k2.first), new LongWritable(
					min));
		};
	}

	static class MyGroupingComparator implements RawComparator<NewK2> {

		@Override
		public int compare(NewK2 arg0, NewK2 arg1) {
			// TODO Auto-generated method stub
			return (int) (arg0.first - arg1.first);
		}

		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			// TODO Auto-generated method stub
			return WritableComparator.compareBytes(b1, s1, 8, b2, s2, 8);
		}

	}

	static class NewK2 implements WritableComparable<NewK2> {
		Long first;
		Long second;

		public NewK2() {
		}

		public NewK2(long first, long second) {
			this.first = first;
			this.second = second;
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			this.first = in.readLong();
			this.second = in.readLong();
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeLong(first);
			out.writeLong(second);
		}

		/**
		 * ��k2��������ʱ������ø÷���. ����һ�в�ͬʱ�����򣻵���һ����ͬʱ���ڶ�������
		 */
		@Override
		public int compareTo(NewK2 o) {
			final long minus = o.first - this.first;
			if (minus != 0) {
				return (int) minus;
			}
			return (int) (o.second - this.second);
		}

		@Override
		public int hashCode() {
			return this.first.hashCode() + this.second.hashCode();
		}

		@Override
		public boolean equals(Object obj) {
			if (!(obj instanceof NewK2)) {
				return false;
			}
			NewK2 oK2 = (NewK2) obj;
			return (this.first == oK2.first) && (this.second == oK2.second);
		}
	}
}
