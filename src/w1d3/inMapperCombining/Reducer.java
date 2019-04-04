package w1d3.inMapperCombining;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;

import w1d2.wordCount.GroupByPair;
import w1d2.wordCount.Pair;

public class Reducer<K, V> {
	private HashMap<String, List<Pair>> list;

	public Reducer() {
		this.list = new HashMap<>();
	}
	// added for W1D3 - AvgWordLength problem

	public void receivePair(GroupByPair p) {
		if (this.list.containsKey(p.getKey())) {
			List<Pair> plist = this.list.get(p.getKey());
			plist.addAll(p.getValues());
			this.list.put((String) p.getKey(), plist);
		} else {
			List<Pair> plist = new ArrayList<>();
			plist.addAll(p.getValues());
			this.list.put((String) p.getKey(), plist);
		}
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

	public Pair emit() {
		Pair pair = null;
		if (this.list.size() > 0) {
			Object[] keys=this.list.keySet().toArray();
			Arrays.sort(keys);
			List<Pair> lp = this.list.get(keys[0]);
			double average = 0.0;
			int sum = 0;
			int count = 0;
			for (Pair p : lp) {
				sum+=(int)p.getKey();
				count+=(int)p.getValue();
			}
			this.list.remove(keys[0]);
			if (count != 0)
				average = ((double)sum / count) * 1.0;
			pair=new Pair((String)keys[0],average);
		}
		return pair;
	}

	public void print() {
		Object[] keys=this.list.keySet().toArray();
		Arrays.sort(keys);
		for (Object key : keys) {
			List<Pair> lp = this.list.get(key);
			System.out.println(key + "-" + lp);
		}
	}
}
