package com.cic.textengine.utils;

import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

public class Counting16Filter<E> implements BloomFilter<E>{
    /*
     * counters are packed into arrays of "units."  Currently a unit is a long,
     * which consists of 64 bits, per item need 4 bits.
     */
	private final static int PER_COUNTER_BITS = 4;
	private final static int PER_UNIT_BITS = 64;

	private final static int COUNTER_INDEX_TO_ARRAY = 4; //= log(ITEM_PER_UNIT)/log(2)
	private final static int ITEM_PER_UNIT = 16; //= PER_UNIT_BITS / PER_COUNTER_BITS
	private final static int COUNTER_INDEX_TO_BITS_MASK = ITEM_PER_UNIT -1;
	private final static int COUNTER_INDEX_TO_BITS_INDEX = 2; //= log(COUNTER_INDEX_TO_ARRAY)/log(2)

	private final static long COUNTER_GET_MASK = 0xf; //= (1 << PER_COUNTER_BITS) -1
	private final static int COUNTER_MAX_VALUE = 0xf; //= (1 << PER_COUNTER_BITS) -1

	private static final long serialVersionUID = 3527833617516722215L;
	protected int k;
	long[] counters;
	
	int bitArraySize, expectedElements;

	/**
	 * Construct a CountingFilter. You must specify the number of bits in the
	 * Bloom Filter, and also you should specify the number of items you
	 * expect to add. The latter is used to choose some optimal internal values to
	 * minimize the false-positive rate (which can be estimated with
	 * expectedFalsePositiveRate()).
	 * 
	 * @param bitArraySize
	 *            The number of bits in the bit array (often called 'm' in the
	 *            context of bloom filters).
	 * @param expectedElements
	 *            The typical number of items you expect to be added to the
	 *            SimpleBloomFilter (often called 'n').
	 */
	public Counting16Filter(int bitArraySize, int expectedElements) {
		this.bitArraySize = bitArraySize;
		this.expectedElements = expectedElements;
		this.k = (int) Math.ceil((bitArraySize / expectedElements)
				* Math.log(2.0));
		int arraySize = (bitArraySize + ITEM_PER_UNIT -1) / ITEM_PER_UNIT;
		counters = new long[arraySize];
	}
	
	private int get(int counterIndex) {
		int unitIndex = counterIndex >> COUNTER_INDEX_TO_ARRAY;
		int bitsIndex = (counterIndex & COUNTER_INDEX_TO_BITS_MASK) << COUNTER_INDEX_TO_BITS_INDEX;
		long bitsValue = (counters[unitIndex] >> bitsIndex) & COUNTER_GET_MASK;
		return (int) bitsValue;
	}
	
	private void set(int counterIndex, int counterValue) {
		int unitIndex = counterIndex >> COUNTER_INDEX_TO_ARRAY;
		int bitsIndex = (counterIndex & COUNTER_INDEX_TO_BITS_MASK) << COUNTER_INDEX_TO_BITS_INDEX;
		long valueBits = (counterValue & COUNTER_GET_MASK) << bitsIndex;
		long clearBits = ~(COUNTER_GET_MASK << bitsIndex);
		counters[unitIndex] = (counters[unitIndex] & clearBits) | valueBits;
	}

	/**
	 * Calculates the approximate probability of the contains() method returning
	 * true for an object that had not previously been inserted into the bloom
	 * filter. This is known as the "false positive probability".
	 * 
	 * @return The estimated false positive rate
	 */
	public double expectedFalsePositiveProbability() {
		return Math.pow((1 - Math.exp(-k * (double) expectedElements
				/ (double) bitArraySize)), k);
	}
	
	static	public double expectedFalsePositiveProbability(int bitArraySize, int expectedElements) {
		int k = (int) Math.ceil((bitArraySize / expectedElements)
				* Math.log(2.0));
		return Math.pow((1 - Math.exp(-k * (double) expectedElements
				/ (double) bitArraySize)), k);
	}

	/*
	 * @return This method will always return false
	 * 
	 * @see java.util.Set#add(java.lang.Object)
	 */
	public boolean add(E o) {
		Random r = new Random(o.hashCode());
		for (int x = 0; x < k; x++) {
			int idx = r.nextInt(bitArraySize);
			
			int count = get(idx);
			if (count < COUNTER_MAX_VALUE) {
				count++;
				set(idx, count);
			}
		}
		return false;
	}
	
	public boolean add(long objHashCode) {
		Random r = new Random(objHashCode);
		for (int x = 0; x < k; x++) {
			int idx = r.nextInt(bitArraySize);
			
			int count = get(idx);
			if (count < COUNTER_MAX_VALUE) {
				count++;
				set(idx, count);
			}
		}
		return false;
	}

	public boolean contains(long objHashCode) {
		Random r = new Random(objHashCode);
		for (int x = 0; x < k; x++) {
			int idx = r.nextInt(bitArraySize);
			if (get(idx) < 1)
				return false;
		}
		return true;
	}

	/**
	 * @return This method will always return false
	 */
	public boolean addAll(Collection<? extends E> c) {
		for (E o : c) {
			add(o);
		}
		return false;
	}

	/**
	 * Clear the Bloom Filter
	 */
	public void clear() {
		Arrays.fill(counters, 0);
	}

	/**
	 * @return False indicates that o was definitely not added to this Bloom Filter, 
	 *         true indicates that it probably was.  The probability can be estimated
	 *         using the expectedFalsePositiveProbability() method.
	 */
	public boolean contains(Object o) {
		Random r = new Random(o.hashCode());
		for (int x = 0; x < k; x++) {
			int idx = r.nextInt(bitArraySize);
			if (get(idx) < 1)
				return false;
		}
		return true;
	}

	public boolean containsAll(Collection<?> c) {
		for (Object o : c) {
			if (!contains(o))
				return false;
		}
		return true;
	}

	/**
	 * Not implemented
	 */
	public boolean isEmpty() {
		throw new UnsupportedOperationException();
	}

	/**
	 * Not implemented
	 */
	public Iterator<E> iterator() {
		throw new UnsupportedOperationException();
	}

	/**
	 * Not implemented
	 */
	public boolean remove(Object o) {
		if (!contains(o))
			return false;
		Random r = new Random(o.hashCode());
		for (int x = 0; x < k; x++) {
			int idx = r.nextInt(bitArraySize);
			
			int count = get(idx);
			if (count > 0) {
				count--;
				set(idx, count);
			}
		}
		return true;
	}
	
	public boolean remove(long objHashCode) {
		if (!contains(objHashCode))
			return false;
		Random r = new Random(objHashCode);
		for (int x = 0; x < k; x++) {
			int idx = r.nextInt(bitArraySize);
			
			int count = get(idx);
			if (count > 0) {
				count--;
				set(idx, count);
			}
		}
		return true;
	}

	/**
	 * Not implemented
	 */
	public boolean removeAll(Collection<?> c) {
		for (Object o : c){
			remove(o);
		}
		return false;
	}

	/**
	 * Not implemented
	 */
	public boolean retainAll(Collection<?> c) {
		throw new UnsupportedOperationException();
	}

	/**
	 * Not implemented
	 */
	public int size() {
		throw new UnsupportedOperationException();
	}

	/**
	 * Not implemented
	 */
	public Object[] toArray() {
		throw new UnsupportedOperationException();
	}

	/**
	 * Not implemented
	 */
	public <T> T[] toArray(T[] a) {
		throw new UnsupportedOperationException();
	}
}
