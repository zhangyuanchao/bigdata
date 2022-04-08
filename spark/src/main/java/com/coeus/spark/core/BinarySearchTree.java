package com.coeus.spark.core;

import java.util.PriorityQueue;

public class BinarySearchTree {

    private class Node {

        int data; // 数据域
        Node right; // 右子树
        Node left; // 左子树
    }

    private Node root; // 树根节点

    private void insert(int key) {
        Node p = new Node(); //待插入的节点
        p.data=key;

        if(root == null) {
            root = p;
        } else {
            Node parent = new Node();
            Node current = root;
            while(true) {
                parent = current;
                if(key > current.data) {
                    current = current.right; // 右子树
                    if(current == null) {
                        parent.right = p;
                        return;
                    }
                } else { // 暂且假设插入的节点值都不同
                    current = current.left; // 左子树
                    if(current == null) {
                        parent.left = p;
                        return;
                    }
                }
            }
        }
    }

    public void preOrder(Node root)
    { // 前序遍历,"中左右"
        if (root != null)
        {
            System.out.print(root.data + " ");
            preOrder(root.left);
            preOrder(root.right);
        }
    }

    public void inOrder(Node root)
    { // 中序遍历,"左中右"
        if (root != null)
        {
            inOrder(root.left);
            System.out.print(root.data + " ");
            inOrder(root.right);
        }
    }

    public void postOrder(Node root)
    { // 后序遍历,"左右中"
        if (root != null)
        {
            postOrder(root.left);
            postOrder(root.right);
            System.out.print(root.data + " ");
        }
    }

    public static void main(String[] args) {
        PriorityQueue<Integer> pq = new PriorityQueue<Integer>();
        pq.add(6);
        pq.add(3);
        pq.add(9);
        pq.add(1);
        pq.add(5);
        pq.add(2);
        pq.add(8);
        pq.add(2);
        pq.add(7);
        while (!pq.isEmpty()) {
            System.out.println(pq.poll());
        }
    }
}
