package simpledb.storage;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class LRUCache<K, V> {
    class Node {
        K key;
        V value;
        Node prev;
        Node next;

        public Node() {
        }

        public Node(K key, V value) {
            this.key = key;
            this.value = value;
        }
    }

    private Map<K, Node> lruCache;
    private int capacity;
    private int size;
    private Node head;
    private Node tail;

    public LRUCache() {
    }

    public LRUCache(int capacity) {
        this.capacity = capacity;
        head = new Node();
        tail = new Node();
        head.next = tail;
        tail.prev = head;
        lruCache = new ConcurrentHashMap<>();
    }

    public V get(K key) {
        if (!lruCache.containsKey(key)) {
            return null;
        }
        Node node = lruCache.get(key);
        node.prev.next = node.next;
        node.next.prev = node.prev;
        node.prev = head;
        node.next = head.next;
        head.next.prev = node;
        head.next = node;
        return node.value;
    }

    public void put(K key, V value) {
        Node node = lruCache.get(key);
        if (node == null) {
            node = new Node(key, value);
            size++;
            lruCache.put(key, node);
        } else {
            node.value = value;
            node.prev.next = node.next;
            node.next.prev = node.prev;
        }
        node.prev = head;
        node.next = head.next;
        head.next.prev = node;
        head.next = node;
        if (size > capacity) {
            remove(tail.prev);
        }
    }

    public void remove(K key) {
        Node node = lruCache.get(key);
        if (node == null) {
            return;
        }
        remove(node);
    }

    private void remove(Node node) {
        lruCache.remove(node.key);
        node.prev.next = node.next;
        node.next.prev = node.prev;
        size--;
    }

    public K getLastPageId() {
        if (size == 0) {
            return null;
        }
        return tail.prev.key;
    }

    public int size() {
        return size;
    }

    public boolean isFull() {
        return size > capacity;
    }

    public List<V> values() {
        return lruCache.values().stream().map(node -> node.value).toList();
    }
}
