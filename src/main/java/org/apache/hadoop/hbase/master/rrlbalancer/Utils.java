package org.apache.hadoop.hbase.master.rrlbalancer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;

public class Utils {

	public static interface ClusterDataKeyGenerator<V, K> {
		K generateKey(V v);
	}

	public static <K, V> Map<K, List<V>> cluster(Collection<V> input,
			ClusterDataKeyGenerator<V, K> keyGenerator) {
		Map<K, List<V>> result = new HashMap<K, List<V>>();
		for (V v : input) {
			K k = keyGenerator.generateKey(v);
			if (!result.containsKey(k))
				result.put(k, new ArrayList<V>());
			result.get(k).add(v);
		}

		return result;
	}

	public static <K, V> List<V> getMapEntriesForKeys(ListMultimap<K, V> map,
			Collection<K> keys) {
		List<V> result = new ArrayList<V>();
		for (K key : keys)
			if (map.containsKey(key))
				result.addAll(map.get(key));
		return result;
	}

	public static <K, V> Map<K, V> getMapEntriesForKeys(Map<K, V> map,
			Collection<K> keys) {
		Map<K, V> result = new HashMap<K, V>();
		for (K key : keys)
			if (map.containsKey(key))
				result.put(key, map.get(key));
		return result;
	}

	/**
	 * Returns the common elements of a and b. Note: Result is not a mutli bag
	 * operation, the input collections are converted into set equivalents.
	 * 
	 * @param a
	 * @param b
	 * @return
	 */
	public static <T> List<T> intersect(Collection<T> a, Collection<T> b) {
		Set<T> hashedEntriesA = new HashSet<T>();
		hashedEntriesA.addAll(a);

		Set<T> result = new HashSet<T>();
		for (T in : b)
			if (hashedEntriesA.contains(in))
				result.add(in);
		return new ArrayList<T>(result);
	}

	/**
	 * Returns the result of (a - b). Note: Result is not a mutli bag operation,
	 * the input collections are converted into set equivalents.
	 * 
	 * @param a
	 * @param b
	 * @return
	 */
	public static <T> List<T> minus(Collection<T> a, Collection<T> b) {
		Set<T> hashedEntriesB = new HashSet<T>();
		hashedEntriesB.addAll(b);

		Set<T> result = new HashSet<T>();
		for (T serverName : a)
			if (!hashedEntriesB.contains(serverName))
				result.add(serverName);
		return new ArrayList<T>(result);
	}

	public static <K, V> ListMultimap<V, K> reverseMap(Map<K, V> input) {
		ArrayListMultimap<V, K> multiMap = ArrayListMultimap.create();
		for (Map.Entry<K, V> entry : input.entrySet()) {
			multiMap.put(entry.getValue(), entry.getKey());
		}

		return multiMap;
	}

	public static <K, V> List<List<V>> getValuesAsList(Map<K, List<V>> input) {
		return new ArrayList<List<V>>(input.values());
	}

}
