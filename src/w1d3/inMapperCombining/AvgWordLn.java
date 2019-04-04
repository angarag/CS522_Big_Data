package w1d3.inMapperCombining;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import w1d2.wordCount.GroupByPair;
import w1d2.wordCount.Pair;

public class AvgWordLn {
	int m;
	int r;
	List<Mapper_avgWordLn> mappers;
	List<Reducer> reducers;
	int[] nums;

	public AvgWordLn(int m, int r) {
		this.m = m;
		this.r = r;
		this.mappers = new ArrayList<Mapper_avgWordLn>(this.m);
		this.reducers = new ArrayList<Reducer>(this.r);
		for (int i = 0; i < m; i++) {
			this.mappers.add(new Mapper_avgWordLn());
		}
		for (int i = 0; i < r; i++) {
			this.reducers.add(new Reducer());
		}
		System.out.println("Number of Input-Splits: " + m);
		System.out.println("Number of Reducers: " + r);
	}

	public static void main(String[] args) {
		AvgWordLn nameNode = new AvgWordLn(4, 3);
		List<String> strs = new ArrayList<>();
		strs.add("Art is beautiful and life enhancing. However it pays very little. \n" + 
				"Many artists have a hard life.");
		strs.add("Sun is there every day. Moon comes every day. \n" + 
				"Let us live every day as the best day so far.");
		strs.add("Meditation is very important for the development of consciousness. \n" + 
				"So let us meditate every day.");
		strs.add("Earth is blue if you look from outer space. Mars is red. Moon is yellow. \n" + 
				"Sun is white. What a wonderful world.");
		for (int i = 0; i < nameNode.m; i++) {
			System.out.println("Mapper " + i + " Input");
			String input = strs.get(i);
			System.out.println(input);
			nameNode.mappers.get(i).generatePairsFromString(input);
		}
		for (int i = 0; i < nameNode.m; i++) {
			System.out.println("Mapper " + i + " Output");
			nameNode.mappers.get(i).printPairs();
		}
		for (int i = 0; i < nameNode.m; i++) {
			GroupByPair temp = nameNode.mappers.get(i).emit();
			while (temp != null) {
				int reducer_id = nameNode.getPartition((String) temp.getKey());
				System.out.println("Pairs sent from Mapper " + i + " Reducer " + reducer_id);
				System.out.println(temp);
				nameNode.reducers.get(reducer_id).receivePair(temp);
				temp = nameNode.mappers.get(i).emit();
			}
		}
		for (int i = 0; i < nameNode.r; i++) {
			System.out.println("Reducer " + i + " Input");
			nameNode.reducers.get(i).print();
		}
		for (int i = 0; i < nameNode.r; i++) {
			System.out.println("Reducer " + i + " Output");
			Pair emit = nameNode.reducers.get(i).emit();
			while (emit != null) {
				System.out.println(emit);
				emit = nameNode.reducers.get(i).emit();
			}
		}

	}

	public int getPartition(String key) {
		int result = 0;
		result = Math.abs(key.hashCode() % this.r);
		return result;
	}

}
