package w1d2.wordCount;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** JDK 7+. */
public final class Util {
	public static final String FILENAME = "/home/angarag/Downloads/testDataForW1D1.txt";
//	"C:\\Users\\986689\\Downloads\\testDataForW1D1.txt";

	public static void main(String[] args) {
		List<String> list = extractWordsFromFile(FILENAME);
		System.out.println(list);
	}

	public static List<String> extractWordsFromFile(String FILENAME) {
		List<String> list = new ArrayList<String>();
		try (BufferedReader br = new BufferedReader(new FileReader(FILENAME))) {
			String sCurrentLine;
			while ((sCurrentLine = br.readLine()) != null) {
				String input = sCurrentLine.toLowerCase();
				Pattern p = Pattern.compile("[\\w']+");
				Matcher m = p.matcher(input);
				while (m.find()) {
					String word = input.substring(m.start(), m.end());
					// Special character check
					Pattern pp = Pattern.compile("[^a-z]", Pattern.CASE_INSENSITIVE);
					Matcher mm = pp.matcher(word);
					boolean b = mm.find();
					if (!b) {
						list.add(word);
					}
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return list;
	}

	public static List<String> extractWordsFromString(String sCurrentLine) {
		List<String> list = new ArrayList<String>();
		String input = sCurrentLine.toLowerCase();
		Pattern p = Pattern.compile("[\\w']+");
		Matcher m = p.matcher(input);
		while (m.find()) {
			String word = input.substring(m.start(), m.end());
			// Special character check
			Pattern pp = Pattern.compile("[^a-z]", Pattern.CASE_INSENSITIVE);
			Matcher mm = pp.matcher(word);
			boolean b = mm.find();
			if (!b) {
				list.add(word);
			}
		}
		return list;

	}
}