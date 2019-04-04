package w1d3.inMapperCombining;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import w1d2.wordCount.GroupByPair;
import w1d2.wordCount.Pair;
import w1d2.wordCount.Util;

public class Mapper_avgWordLn {
	HashMap<String, List<Integer>> list;
	HashMap<String, Pair> emits;

	public Mapper_avgWordLn() {
		this.list = new HashMap<>();
		this.emits = new HashMap<>();
	}

	public Mapper_avgWordLn(String line) {
		this.generatePairsFromString(line);
	}

	public GroupByPair emit() {
		if (this.emits.size() < 1)
			return null;
		String key = this.emits.keySet().stream().findFirst().get();
		Pair sum_count_pair = this.emits.get(key);
		this.emits.remove(key);
		List<Pair> values = new ArrayList<>();
		values.add(new Pair((int) sum_count_pair.getKey(),(int) sum_count_pair.getValue()));
		GroupByPair emit = new GroupByPair(key, values);
		return emit;
	}

	public static void main(String[] args) {
		Mapper_avgWordLn mapper = new Mapper_avgWordLn();
		mapper.generatePairsFromFile(Util.FILENAME);
		System.out.println("Printing the Key-Value pairs:");
		mapper.printPairs();
	}

	public void printPairs() {
		for (String key : this.emits.keySet()) {
			Pair sum_count_pair = this.emits.get(key);
			System.out.println(key + "-" + sum_count_pair);
		}
	}

	public void generatePairsFromFile(String FILENAME) {
		List<String> words = Util.extractWordsFromFile(FILENAME);
		this.helperForEmits(words);
	}

	public void generatePairsFromString(String line) {
		List<String> words = Util.extractWordsFromString(line);
		this.helperForEmits(words);
	}

	private void helper(List<String> words) {
		List<Pair> lp = words.stream().map(w -> new Pair(String.valueOf(w.charAt(0)), w.length()))
				.collect(Collectors.toList());
		for (Pair p : lp) {
			if (this.list.containsKey(p.getKey())) {
				List<Integer> temp = this.list.get(p.getKey());
				temp.add((Integer) p.getValue());
				this.list.put((String) p.getKey(), temp);
			} else {
				List<Integer> temp = new ArrayList<>();
				temp.add((Integer) p.getValue());
				this.list.put((String) p.getKey(), temp);
			}
		}
	}

	private void helperForEmits(List<String> words) {
		List<Pair> lp = words.stream().map(w -> new Pair(String.valueOf(w.charAt(0)), w.length()))
				.collect(Collectors.toList());
		for (Pair p : lp) {
			if (this.emits.containsKey(p.getKey())) {
				Pair temp = this.emits.get(p.getKey());// sum,count pair
				int new_sum = (int) temp.getKey() + (int) p.getValue();
				int new_count = 1 + (int) temp.getValue();
				temp.setKey(new_sum);
				temp.setValue(new_count);
				this.emits.put((String) p.getKey(), temp);
			} else {
				Pair temp = new Pair((int) p.getValue(), 1);
				this.emits.put((String) p.getKey(), temp);
			}
		}
	}
}
