package com.mars.bigdata.hadoop;

import java.io.IOException;
import java.util.Arrays;

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

//Computing relative frequencies
public class PairStripes {

	private static final Text const_txt = new Text("mars");

	public static class TokenizerMapper extends Mapper<Object, Text, Text, MapWritable> {
		// Mapper is as in Pair
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			String lines[] = value.toString().split(" ");
			for (int i = 0; i < lines.length; i++) {
				String line = lines[i];
				String[] neighbors = Util.giveMeMyNeighbors(lines, i);
				if (!line.equals("") && neighbors != null) {
					for (String n : neighbors) {
						if (!n.equals("")) {
							double one = 1.0;
							MapWritable map = new MapWritable();
							map.put(const_txt, new DoubleWritable(one));
							context.write(new Text(new String(line + "," + n)), map);
						}
					}
				}
			}
		}
	}

	// reducer is as in Stripes
	public static class IntSumReducer extends Reducer<Text, MapWritable, Text, MapWritable> {

		double sum = 0.0;
		String prev_key_u = "";
		MapWritable myMap = new MapWritable();

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			System.out.println("Emitting the hashmap of the last key:	"+prev_key_u);
			context.write(new Text(prev_key_u), myMap);
			
		}
		// Key is in tuple format: (u,v)
		public void reduce(Text key, Iterable<MapWritable> values, Context context)
				throws IOException, InterruptedException {
			double s = 0.0;

			String[] pair = key.toString().split(",");
			System.out.println(Arrays.toString(pair));
			if ((!pair[0].equals(prev_key_u) && !prev_key_u.equals(""))) {
				for (Writable w : myMap.keySet()) {
					DoubleWritable dw = (DoubleWritable) myMap.get(w);
					myMap.put(w, new DoubleWritable(dw.get() / sum));
				}
				Util.printMap(prev_key_u, myMap);
				context.write(new Text(prev_key_u), myMap);
				myMap = new MapWritable();
				sum = 0.0;
			}
			if (pair.length > 1) {
				for (MapWritable val : values) {
					for (Writable w : val.keySet()) {
						s += ((DoubleWritable) val.get(w)).get();
					}
				}
				sum += s;
				Text key_v = new Text(pair[1]);
				DoubleWritable dw = new DoubleWritable(s);
				if (!myMap.containsKey(key_v))
					myMap.put(key_v, dw);
				else {
					double d = ((DoubleWritable) myMap.get(key_v)).get();
					myMap.put(key_v, new DoubleWritable(d + s));
				}
			}

			prev_key_u = pair[0];
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Pair problem");
		job.setJarByClass(PairStripes.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(MapWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
