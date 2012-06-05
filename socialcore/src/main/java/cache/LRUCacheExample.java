package cache;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Least-Recently-Used(LRU) Cache
 * @author terto
 *
 * @param <K>
 * @param <V>
 */
public class LRUCacheExample<K, V> extends LinkedHashMap<K, V> {
	private static final long serialVersionUID = 5598359195489302534L;
	private final int maxCapacity;
	
	public LRUCacheExample(int initialCapacity){
		super(initialCapacity);
		this.maxCapacity = initialCapacity;
	}
	
	@Override
	protected boolean removeEldestEntry(java.util.Map.Entry<K, V> eldest) {
		return this.size() > maxCapacity;
	}
	
	public static void main(String[] args) {
		LRUCacheExample<String, String> cache = new LRUCacheExample<String, String>(3);
		
		cache.put("1", "1");
		cache.put("2", "2");
		cache.put("3", "3");
		cache.put("4", "4");
		cache.put("5", "5");
		
		for (Map.Entry<String, String> entry :  cache.entrySet()) {
			System.out.println(entry.getKey());
		}
		
	}

}
