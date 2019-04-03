package w1d3.wordCount.inMapperCombining;

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

public class Mapper {
	HashMap<String, Integer> list;

	public Mapper() {
		this.list = new HashMap<>();
	}

	public Mapper(String line) {
		this.generatePairsFromString(line);
	}

	public Pair emit() {
		if (this.list.size() < 1)
			return null;
		String key = this.list.keySet().stream().findFirst().get();
		Integer value = this.list.get(key);
		this.list.remove(key);
		return new Pair(key, value);
	}

	public static void main(String[] args) {
		Mapper mapper = new Mapper();
		mapper.generatePairsFromFile(Util.FILENAME);
		System.out.println("Printing the Key-Value pairs:");
		mapper.printPairs();
	}

	public void printPairs() {
		for (String key : this.list.keySet()) {
			Integer value = this.list.get(key);
			System.out.println(key + "-" + value);
		}
	}

	public void generatePairsFromFile(String FILENAME) {
		List<String> words = Util.extractWordsFromFile(FILENAME);
		this.helper(words);
	}

	public void generatePairsFromString(String line) {
		List<String> words = Util.extractWordsFromString(line);
		this.helper(words);
	}

	private void helper(List<String> words) {
		List<Pair> lp = words.stream().map(w -> new Pair(w, 1)).collect(Collectors.toList());
		for (Pair p : lp) {
			if (this.list.containsKey(p.getKey())) {
				this.list.put((String) p.getKey(), (int) this.list.get(p.getKey()) + (int) p.getValue());
			} else {
				this.list.put((String) p.getKey(), (int) p.getValue());
			}
		}
	}

}
