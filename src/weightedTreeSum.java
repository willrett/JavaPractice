package com.zhang.other;

import java.util.LinkedList ;

import com.zhang.leetcode.*;
public class weightedTreeSum {
	
	public static int weightedTreeSum(TreeNode root){
		if(root == null)
			return 0;
		
		LinkedList<TreeNode> q = new LinkedList<TreeNode> ();
		int current = 1;
		int next = 0;
		int weight = 1;
		int sum = 0;
		q.add(root);
		
		while(!q.isEmpty()){
			TreeNode temp = q.removeFirst();
			current--;
			sum += temp.val*weight;
			
			if(temp.left != null){
				q.add(temp.left);
				next++;
			}
			
			if(temp.right != null){
				q.add(temp.right);
				next++;
			}
			
			if(current == 0){
				current = next;
				next = 0;
				weight++;
			}
		}
		return sum;
		
	}
	
	public static void main(String args[]){
		TreeNode root = new TreeNode(1);
		root.left =  new TreeNode(2);
		root.right =  new TreeNode(3);
		root.left.left =  new TreeNode(4);
		root.left.right =  new TreeNode(5);
		System.out.println(weightedTreeSum(root));
	}
}
