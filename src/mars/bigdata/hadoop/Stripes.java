package mars.bigdata.hadoop;

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

//Computing relative frequencies
public class Stripes {

	public static class TokenizerMapper extends Mapper<Object, Text, Text, MapWritable> {
		double one = 1.0;

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String lines[] = value.toString().split(" ");
			for (int i = 0; i < lines.length; i++) {
				MapWritable myMap = new MapWritable();
				String line = lines[i];
				String[] neighbors = Util.giveMeMyNeighbors(lines, i);
				if (!line.equals("") && neighbors != null) {
					for (String n : neighbors) {
						if (!n.equals("")) {
							Text t = new Text(n);
							if (!myMap.containsKey(t)) {
								myMap.put(new Text(n), new DoubleWritable(one));
							} else {
								double d = ((DoubleWritable) myMap.get(t)).get();
								myMap.put(new Text(n), new DoubleWritable(d + one));
							}
						}
					}
					System.out.print("Mapper output:	");
					Util.printMap(line, myMap);
					context.write(new Text(line), myMap);
				}
			}

		}
	}

	public static class IntSumReducer extends Reducer<Text, MapWritable, Text, MapWritable> {

		public void reduce(Text key, Iterable<MapWritable> maps, Context context)
				throws IOException, InterruptedException {
			System.out.print("Reducer output:	");
			MapWritable combined = new MapWritable();
			double total_map_size = 0.0;
			for (MapWritable map : maps) {
				for (Writable w : map.keySet()) {
					total_map_size += ((DoubleWritable) map.get(w)).get();
				}
				combined = Util.combineMaps(combined, map);
			}
			for (Writable w : combined.keySet()) {
				DoubleWritable dw = (DoubleWritable) combined.get(w);
				combined.put(w, new DoubleWritable(dw.get() / total_map_size));
			}
			Util.printMap(key.toString(), combined);
			context.write(key, combined);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "stripes problem");
		job.setJarByClass(Stripes.class);
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
