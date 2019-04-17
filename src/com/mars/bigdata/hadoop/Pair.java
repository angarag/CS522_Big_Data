package com.mars.bigdata.hadoop;

import java.io.IOException;
import java.util.Arrays;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//Computing relative frequencies
public class Pair {
	private static final String pair_char = "*";

	public static class TokenizerMapper extends Mapper<Object, Text, Text, DoubleWritable> {

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			String lines[] = value.toString().split(" ");
			for (int i = 0; i < lines.length; i++) {
				String line = lines[i];
				String[] neighbors = Util.giveMeMyNeighbors(lines, i);
				if (!line.equals("") && neighbors != null) {
					for (String n : neighbors) {
						if (!n.equals("")) {
							double one = 1.0;
							System.out.println("Pair:" + line + "," + pair_char + "," + n);
							context.write(new Text(new String(line + "," + pair_char)), new DoubleWritable(one));
							context.write(new Text(new String(line + "," + n)), new DoubleWritable(one));
						}
					}
				}
			}
		}
	}

	public static class IntSumReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

		double sum = 0.0;

		public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
				throws IOException, InterruptedException {
			double s = 0.0;
			String[] pair = key.toString().split(",");

			for (DoubleWritable val : values) {
				s += val.get();
			}
			if (pair.length > 1 && pair[1] != null && pair[1].equals(pair_char)) {
				sum = s;
			} 
			if(sum!=0)
				context.write(key, new DoubleWritable(s/sum*1.0));
			System.out.println(key + "=" + Arrays.toString(pair) + " sum:" + sum + " s:"+s+" v of pair:" + pair_char);
			// Text itext = new Text(pair[0]);
			// if (pair.length > 1 && pair[1] != null) {
			// if (!myMap.containsKey(key))
			// myMap.put(key, new DoubleWritable(s));
			// else {
			// double d = Double.parseDouble(myMap.get(itext).toString());
			// myMap.put(key, new DoubleWritable(d + s));
			// }
			// }
			// for (Writable w : myMap.keySet()) {
			// String ikey = w.toString();
			// String[] ipair = ikey.toString().split(",");
			// if (pair.length > 1 && pair[1] != null && !pair[1].equals(pair_char)) {
			// double wcount = Double.parseDouble(myMap.get(w).toString());
			// double totalcount = Double.parseDouble(myMap.get(new Text(ipair[0] + "," +
			// pair_char)).toString());
			// context.write((Text) w, new DoubleWritable(wcount / totalcount));
			// }
			// }
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Pair problem");
		job.setJarByClass(Pair.class);
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
