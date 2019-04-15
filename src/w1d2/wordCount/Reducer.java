package w1d2.wordCount;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Reducer {
	private List<GroupByPair> glist;
	private List<Pair> plist;

	public Reducer(List<Pair> list) {
		this.glist = getGroupPairs(list);
		this.plist = null;// Pair list is not needed as sortedlist is given
	}

	public Reducer() {
		this.glist = new ArrayList<>();
		this.plist = new ArrayList<>();
	}

	public void receivePairs(List<Pair> list) {
		this.glist.addAll(getGroupPairs(list));
	}

	public void receivePair(Pair p) {
		this.plist.add(p);
		this.glist = this.getGroupPairs(this.plist);
	}
	//added for W1D3 - AvgWordLength problem

	public void receivePair(GroupByPair p) {
		this.glist.add(p);
	}
	public static void main(String[] args) {
		Mapper mapper=new Mapper();
		mapper.generatePairsFromFile(Util.FILENAME);
		Reducer r = new Reducer(mapper.list);
		System.out.println("3. Print the GroupByPair list:");
		r.print();
		System.out.println("4. Iterate through GroupBy list “sum” the values and print out (key, sum) pairs:");
		r.glist.forEach(g -> System.out.println(g.toStringSum()));
		System.out.println("5. Additional print statements:");
		r.print();
	}

	public void sortByKey() {
		if (this.glist.size() < 1)
			return;
		List<GroupByPair> sortedList = new ArrayList<>();
		Collections.sort(this.glist);
		GroupByPair prev = new GroupByPair(this.glist.get(0));
		GroupByPair temp = new GroupByPair(prev.getKey());
		for (GroupByPair gp : this.glist) {
			if (!gp.equals(prev)) {
				sortedList.add(temp);
				prev = gp;
				temp = new GroupByPair(prev.getKey());
			}
			prev.addValues(gp.getValues());
		}
		this.glist = sortedList;
	}

	public static List<GroupByPair> getGroupPairs(List<Pair> pairs) {
		Collections.sort(pairs);
		List<GroupByPair> list = new ArrayList<>();
		if (pairs.size() < 1)
			return null;
		Pair prev = pairs.get(0);
		GroupByPair temp = new GroupByPair(prev.getKey());
		for (Pair p : pairs) {
			if (!p.equals(prev)) {
				list.add(temp);
				prev = p;
				temp = new GroupByPair(prev.getKey());
			}
			temp.addValue(p.getValue());
		}
		return list;
	}

	public String emit() {
		String emit = null;
		if (this.glist.size() > 0) {
			emit = this.glist.get(0).toStringSum();
			this.glist.remove(0);
		}
		return emit;
	}

	public void print() {
		this.glist.forEach(System.out::println);
	}
}
