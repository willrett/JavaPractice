import java.util.HashMap;

public class WordRanking {

	/**
	 * getRank(String word) Returns the rank of the given word.
	 * 
	 * @param word
	 *            : the given word.
	 * @return the rank of the word given.
	 */
	public long getRank(String word) {
		// Store the distinct letters of a word, and the time it appears
		HashMap<Character, Integer> charCounts = new HashMap<Character, Integer>();
		char[] charArray = word.toCharArray();

		// compute the charCounts
		for (char a : charArray) {
			if (charCounts.containsKey(a)) {
				charCounts.put(a, charCounts.get(a) + 1);
			} else {
				charCounts.put(a, 1);
			}
		}

		if (charCounts == null || charCounts.isEmpty()) {
			return 0;
		}

		long rank = 1;
		for (char a : charArray) {
			for (char b : charCounts.keySet()) {
				if (b < a) {
					long sumOfFactorial = factorialSum(charCounts);
					long productOfFactorial = productOfFactorialWithoutDuplicate(charCounts, b);
					rank += sumOfFactorial/productOfFactorial;
				}
			}
			int count = charCounts.get(a);
			count --;
			if (count <= 0) {
				charCounts.remove(a);
			} else {
				charCounts.put(a, count);
			}
		}

		return rank;
	}

	/* computer n! */
	private long factorial(int n) {
		long fact = 1;
		for (int i = 2; i <= n; i++) {
			fact *= i;
		}
		return fact;
	}

	/*
	 * Gets the total amount of combinations of the remaining characters.
	 */
	private long factorialSum(HashMap<Character, Integer> charCounts) {
		int total = 0;
		for (Integer a : charCounts.values()) {
			total += a;
		}
		total -= 1;

		return factorial(total);
	}

	/*
	 * Gets the sum of the factorial of each value in the HashMap 
	 * by decrementing the count of the character that should have been there. 
	 */
	private long productOfFactorialWithoutDuplicate(
			HashMap<Character, Integer> charCounts, Character toBeReplaced) {
		int count = charCounts.get(toBeReplaced);
		charCounts.put(toBeReplaced, count-1);

		long total = 1;
		for (Integer a : charCounts.values()) {
			total *= factorial(a);
		}
		charCounts.put(toBeReplaced, count);
		return total;
	}

	/* Usage: 
	 * javac WordRanking.java
	 * java -jar WordRanking testing_word
	 */
	public static void main(String[] args) {
		WordRanking wordRank = new WordRanking();
		if (args.length < 1) {
			System.err.println("Please specify a word!");
			return;
		}
		String word = args[0];

		long startTime = System.currentTimeMillis();
		long rank = wordRank.getRank(word);
		long stopTime = System.currentTimeMillis();

		System.out.println(word + ": " + rank);
		System.out.println("Time used: " + (stopTime - startTime) + " ms");
	}
}
