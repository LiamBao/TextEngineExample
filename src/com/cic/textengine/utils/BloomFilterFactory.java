package com.cic.textengine.utils;

public class BloomFilterFactory<E> {
	public BloomFilter<E> create(String type, int bitArraySize, int expectedElements){
		if (type.equalsIgnoreCase("Simple"))
			return new SimpleBloomFilter<E>(bitArraySize, expectedElements);
		if (type.equalsIgnoreCase("Counting"))
			return new CountingFilter<E>(bitArraySize, expectedElements);
		if (type.equalsIgnoreCase("Counting16"))
			return new Counting16Filter<E>(bitArraySize, expectedElements);
		return null;
	}
}
