package w1d2.wordCount;

import java.util.ArrayList;
import java.util.List;

public class GroupByPair<K extends Comparable<K>, V extends Comparable<V>> 
implements Comparable<GroupByPair<K,V>>{
	private K key; // key
	private List<V> values; // value

	public GroupByPair(K k) {
		this.key = k;
		this.values=new ArrayList<>();
	}
	//Added for W1D3 - Avg word lenght problem
	public GroupByPair(K k, List<V> values) {
		this.key = k;
		this.values=values;
	}
	public K getKey() {
		return key;
	}

	public List<V> getValues() {
		return values;
	}

	public String toString() {

		String result = "(" + this.key + ",[";
		for (V v : values) {
			result += v;
			result += ",";
		}
		result += "])";
		return result;
	}
	public void addValue(V value) {
		this.values.add(value);
	}
	public void addValues(List<V> values) {
		this.values.addAll(values);
	}

	public String toStringSum() {
		String result = "(" + this.key + ",";
		int sum=0;
		for (V v : values) {
			sum += (int)v;
		}
		result += sum;
		result +=")";
		return result;
	}

	@Override
	public int compareTo(GroupByPair<K, V> that) {
		int cmp = this.key.compareTo(that.getKey());
		return cmp;
	}

}
