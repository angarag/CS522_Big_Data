package com.mars.bigdata.hadoop;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;

public class Util {
	public static String[] giveMeMyNeighbors(String[] lines, int index) {
		List<String> str = new ArrayList<>();
		int counter = 0;
		for (int i = index; i < lines.length; i++) {
			if (!lines[i].equals(lines[index]))
				str.add(lines[i]);
			else
				counter++;
			if (counter == 2)
				break;
		}
		if (str == null || str.size() < 1)
			return null;
		else
			return str.toArray(new String[str.size()]);
	}

	public static Double giveMeLastQuantity(String input) {
		if (input.lastIndexOf(" ") != -1) {

			try {

				String str = input.substring(input.lastIndexOf(" ") + 1);
				double val = Double.parseDouble(str);
				return val;
			} catch (Exception e) {
				return null;
			}
		} else
			return null;
	}

	public static String giveMeFirstQuantity(String input) {
		String[] arr = input.split(" ");
		if (arr == null)
			return null;
		else
			return arr[0];
	}

	public static MapWritable combineMaps(MapWritable map1, MapWritable map2) {
		MapWritable result = map1;
		if (map1 == null)
			result = new MapWritable();
		for (Writable w : map2.keySet()) {
			DoubleWritable dw = (DoubleWritable) map2.get(w);
			if (!result.containsKey(w))
				result.put(w, dw);
			else {
				double d = ((DoubleWritable) map1.get(w)).get();
				result.put(w, new DoubleWritable(d + dw.get()));
			}
		}
		return result;
	}

	public static void printMap(String key, MapWritable myMap) {
		System.out.print(key + ": {");
		for (Writable w : myMap.keySet()) {
			System.out.print(w + "=" + myMap.get(w).toString() + " ");
		}
		System.out.println("}");

	}

	public static void main(String[] args) {
		String s = "64.242.88.10 - - [07/Mar/2004:16:05:49 -0800] \"GET /twiki/bin/edit/Main/Double_bounce_sender?topicparent=Main.ConfigurationVariables HTTP/1.1\" 401 12846\n";
		// s="hahah 22";
		String line = giveMeFirstQuantity(s);
		System.out.println(line);
		double d = giveMeLastQuantity(s);
		System.out.println(d);
		s = "B11 C31 D76 A12 B11 C31 D76 C31 A10 B12 D76 C31";
		for (int i = 0; i < s.split(" ").length; i++) {
			System.out.println(s.split(" ")[i] + ":" + Arrays.toString(giveMeMyNeighbors(s.split(" "), i)));
		}
	}
}
