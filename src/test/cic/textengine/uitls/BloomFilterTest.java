package test.cic.textengine.uitls;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import com.cic.textengine.utils.BloomFilter;
import com.cic.textengine.utils.BloomFilterFactory;
import junit.framework.TestCase;

/**
 * This code may be used, modified, and redistributed provided that the author
 * tag below remains intact.
 * 
 * @author Ian Clarke <ian@uprizer.com>
 */

public class BloomFilterTest extends TestCase {
	private void testBloomFilter(String filterType) {
		DecimalFormat df = new DecimalFormat("0.00000");
		Random r = new Random(124445l);
		int bfSize = 1000000;
		System.out.println("Testing " + bfSize + " bit " + filterType);
		int addCount = 100000;
		BloomFilter<Integer> bf = new BloomFilterFactory<Integer>().create(
				filterType, bfSize, addCount);
		HashSet<Integer> added = new HashSet<Integer>();
		for (int x = 0; x < addCount; x++) {
			int num = r.nextInt();
			added.add(num);
		}
		bf.addAll(added);
		assertTrue("Assert that there are no false negatives", bf
				.containsAll(added));

		int falsePositives = 0;
		for (int x = 0; x < addCount; x++) {
			int num = r.nextInt();

			// Ensure that this random number hasn't been added already
			if (added.contains(num)) {
				continue;
			}

			// If necessary, record a false positive
			if (bf.contains(num)) {
				falsePositives++;
			}
		}
		double expectedFP = bf.expectedFalsePositiveProbability();
		double actualFP = (double) falsePositives / (double) addCount;
		System.out.println("Got " + falsePositives + " false positives out of "
				+ addCount + " added items, rate = " + df.format(actualFP)
				+ ", expected = " + df.format(expectedFP));
		double ratio = expectedFP / actualFP;
		assertTrue(
				"Assert that the actual false positive rate doesn't deviate by more than 10% from what was predicted",
				ratio > 0.9 && ratio < 1.1);
	}

	private void testRemove(String filterType) {
		DecimalFormat df = new DecimalFormat("0.00000");
		Random r = new Random(124445l);
		int bfSize = 1000000;
		System.out.println("Testing " + bfSize + " bit " + filterType);
		int addCount = 100000;
		BloomFilter<Integer> bf = new BloomFilterFactory<Integer>().create(
				filterType, bfSize, addCount);
		HashSet<Integer> added = new HashSet<Integer>();
		for (int x = 0; x < addCount; x++) {
			int num = r.nextInt();
			added.add(num);
		}
		bf.addAll(added);
		assertTrue("Assert that there are no false negatives", bf
				.containsAll(added));

		/* Remove half items */
		int removeCount = addCount / 2;
		Random random = new Random();
		HashSet<Integer> removed = new HashSet<Integer>();
		Object[] adds = added.toArray();
		for (int i = 0; i < removeCount; i++) {
			int removeIdx = -1;
			while (removeIdx < 0 || removed.contains(adds[removeIdx]))
				removeIdx = random.nextInt(addCount);
			Integer remove = (Integer) adds[removeIdx];
			added.remove(remove);
			removed.add(remove);
		}
		
		assertTrue("Assert that before actuall removal, all those items are contained.", bf
				.containsAll(removed));
		
		/* actual removal */
		bf.removeAll(removed);
		
		/* Check how many items removed successful */
		int falseRemained = 0;
		int trueRemoved = 0;
		for (Integer remove : removed){
			if (bf.contains(remove))
				falseRemained ++;
			else
				trueRemoved ++;
				
		}
		double falseRaminedRate = falseRemained / removed.size();
		System.out.println(String.format("%d of %d (%s) actual removed items are still considered remained.", falseRemained, removed.size(), df.format(falseRaminedRate)));
		System.out.println(String.format("%d of %d (%s) actual removed items are considered removed.", trueRemoved, removed.size(), df.format(trueRemoved / removed.size())));
		
		/* Check how many items remained successfully */
		int falseRemoved = 0;
		for (Integer remain : added){
			if (!bf.contains(remain))
				falseRemoved ++;
		}
		double falseRemovedRate = falseRemoved / added.size();
		System.out.println(String.format("%d of %d (%s) actual remained items are considered removed.", falseRemoved, added.size(), df.format(falseRemovedRate)));
		

		int falsePositives = 0;
		for (int x = 0; x < addCount; x++) {
			int num = r.nextInt();

			// Ensure that this random number hasn't been added already
			if (added.contains(num)) {
				continue;
			}

			// If necessary, record a false positive
			if (bf.contains(num)) {
				falsePositives++;
			}
		}
		double expectedFP = bf.expectedFalsePositiveProbability();
		double actualFP = (double) falsePositives / (double) addCount;
		System.out.println("Got " + falsePositives + " false positives out of "
				+ addCount + " added items, rate = " + df.format(actualFP)
				+ ", expected = " + df.format(expectedFP));
		double ratio = expectedFP / actualFP;
		assertTrue(
				"Assert that the actual false positive rate doesn't deviate by more than 10% from what was predicted",
				ratio > 0.9 && ratio < 1.1);
	}

	public void testSimpleBloomFilter() {
		testBloomFilter("Simple");
	}

	public void testCountingFilter() {
		testBloomFilter("Counting");
	}
	
	public void testCountingFilterRemove() {
		testRemove("Counting");
	}

	public void testBloomFilterRemove() {
		testRemove("Simple");
	}
	
	public void testCounting16Filter() {
		testBloomFilter("Counting16");
	}
	
	public void testCounting16FilterRemove() {
		testRemove("Counting16");
	}
}