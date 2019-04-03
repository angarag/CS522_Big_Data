package w1d2.wordCount;

import java.util.ArrayList;
import java.util.List;

public class WordCount {
	int m;
	int r;
	List<Mapper> mappers;
	List<Reducer> reducers;
	int[] nums;

	public WordCount(int m, int r) {
		this.m = m;
		this.r = r;
		this.mappers = new ArrayList<Mapper>(this.m);
		this.reducers = new ArrayList<Reducer>(this.r);
		for (int i = 0; i < m; i++) {
			this.mappers.add(new Mapper());
		}
		for (int i = 0; i < r; i++) {
			this.reducers.add(new Reducer());
		}
		System.out.println("Number of Input-Splits: " + m);
		System.out.println("Number of Reducers: " + r);
	}

	public static void main(String[] args) {
		WordCount nameNode = new WordCount(3, 4);
		List<String> strs = new ArrayList<>();
		/*
		 * strs.add("Given a text-file input123,\r\n" +
		 * "1. The program will extract each \"word\" and form a key-value pair \r\n" +
		 * "where the key is the \"word\" and value is integer one.  Note that your program \r\n"
		 * + "should treat Cat and cat as the same word.\r\n" +
		 * "2. Each pair is inserted into a List. 3. Sort the List using \"Collections\". \r\n"
		 * + "This may involve writing a comparator for the pair class.\r\n" +
		 * "4. Output the List\r\n" +
		 * "5. Note that tokens such as input123, abc.txt,  \r\n" +
		 * "a23bc and abc_ptr  are not words. However, key-value is two words.");
		 */
		strs.add("\"cat bat\" mat-pat mum.edu sat.\r\n" + "fat 'rat eat cat' mum_cs mat");
		strs.add("bat-hat mat pat \"oat\r\n" + "hat rat mum_cs eat oat-pat");
		strs.add("zat lat-cat pat jat.\r\n" + "hat rat. kat sat wat");
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
			int ri;
			Pair temp = nameNode.mappers.get(i).emit();
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
			String emit = nameNode.reducers.get(i).emit();
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
