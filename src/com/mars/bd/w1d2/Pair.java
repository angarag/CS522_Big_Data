package com.mars.bd.w1d2;

public class Pair<K extends Comparable<K>, V extends Comparable<V>> implements Comparable<Pair<K, V>> {
	private K key; // key
	private V value; // value

	public Pair(K k, V v) {
		this.key = k;
		this.value = v;
	}

	public String toString() {
		return "(" + this.key + "," + this.value + ")";
	}

	public K getKey() {
		return key;
	}

	public void setKey(K key) {
		this.key = key;
	}

	public V getValue() {
		return value;
	}

	public void setValue(V value) {
		this.value = value;
	}

	public int compareTo(Pair<K, V> that) {
		int cmp = this.key.compareTo(that.key);
		return cmp;
	}

	@Override
	public boolean equals(Object o) {
		if (o == this) {
			return true;
		}
		if (!(o instanceof Pair)) {
			return false;
		}
		Pair c = (Pair) o;
		return this.key.compareTo((K) c.getKey()) == 0;
	}
}