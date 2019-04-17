package hadoop.bigdata.mars;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class InMapperAverageProblem {

	public static class TokenizerMapper extends Mapper<Object, Text, Text, DoubleWritable> {
		MapWritable myMap = new MapWritable();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			String lines[] = value.toString().split("\\r?\\n");
			for (String line : lines) {
				String first = Util.giveMeFirstQuantity(line);
				Double last = Util.giveMeLastQuantity(line);
				if (first == null || last == null)
					continue;
				Text itext = new Text(first);
				if (!myMap.containsKey(itext))
					myMap.put(itext, new DoubleWritable((last)));
				else {
					double d = Double.parseDouble(myMap.get(itext).toString());
					myMap.put(itext, new DoubleWritable(d + last));
				}
			}

		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			System.out.println("Mapper: Emitting the hashmap:	");
			for (Writable w : myMap.keySet()) {
				double wcount = Double.parseDouble(myMap.get(w).toString());
				System.out.println("Mapper: Emitting the hashmap:	"+w.toString()+" "+wcount);
				context.write((Text) w, new DoubleWritable(wcount));
			}
		}
	}

	public static class IntSumReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
		private DoubleWritable result = new DoubleWritable();

		public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
				throws IOException, InterruptedException {
			double sum = 0;
			double count = 0;
			for (DoubleWritable val : values) {
				sum += val.get();
				count++;
			}
			result.set(sum / count);// calculating average
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "average problem with in-mapper combining approach");
		job.setJarByClass(InMapperAverageProblem.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
