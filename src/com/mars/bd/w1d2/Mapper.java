package com.mars.bd.w1d2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public class Mapper {
	List<Pair> list;

	public Mapper() {
		this.list = new ArrayList<>();
	}

	public Mapper(String line) {
		this.generatePairsFromString(line);
	}

	public Pair emit() {
		Pair p = null;
		if (this.list.size() > 0)
			p = this.list.remove(0);
		return p;
	}

	public static void main(String[] args) {
		Mapper mapper=new Mapper();
		mapper.generatePairsFromFile(Util.FILENAME);
		mapper.sortPairList();
		System.out.println("Printing the Key-Value pairs:");
		mapper.printPairs();
	}

	public void printPairs() {
		this.list.forEach(System.out::println);
	}

	public void generatePairsFromFile(String FILENAME) {
		List<String> words = Util.extractWordsFromFile(FILENAME);
		this.list = words.stream().map(w -> new Pair(w, 1)).collect(Collectors.toList());
	}

	public void generatePairsFromString(String line) {
		List<String> words = Util.extractWordsFromString(line);
		this.list = words.stream().map(w -> new Pair(w, 1)).collect(Collectors.toList());
	}
	public void sortPairList() {
		Collections.sort(this.list);
	}
}
