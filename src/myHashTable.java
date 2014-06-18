package com.zhang.other;
/*
 * The first step is to compute a hash function that transforms the search
 * key into an array index. Ideally, different keys would map to different indices.
 * 
 * Second step is a collision resolution process that deals with this situation.
 * 
 */
public class myHashTable{
	
	public class entry{
		private int key;
		private int data;
		private entry next;
		
		public entry(int key, int data){
			this.key = key;
			this.data = data;
		}
	}
	
	private int size = 10;
	private entry[] dataSets = new entry[size];
	
	public myHashTable(){}
	
	public int get(int key){
		int tempKey = hashFunc(key);
		entry tempE = dataSets[tempKey];
		
		while(tempE!=null){
			if(tempE.key == key)
				return tempE.data;
			tempE = tempE.next;
		}
		return 0;
	}
	
	public void put(int key, int data){
		//hash collision
		entry e = new entry(key, data);
		int tempKey = hashFunc(key);
		//System.out.println("tempKey: " + tempKey);
		entry tempE = dataSets[tempKey];
		if(tempE!=null){
			while(tempE.next!= null ){
				tempE = tempE.next;
			}
			tempE.next = e;
		}else{
			dataSets[tempKey] = e;
		}
	}
	
	private int hashFunc(int key){
		return key%3;
	}
	
	public static void main(String args[]){
		myHashTable ht = new myHashTable();
		ht.put(1, 7);
		ht.put(2, 8);
		ht.put(3, 9);
		ht.put(4, 10);
		
		System.out.println(ht.get(1));
		System.out.println(ht.get(2));
		System.out.println(ht.get(3));
		System.out.println(ht.get(4));
	}
}
