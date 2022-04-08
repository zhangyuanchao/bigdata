package com.coeus.spark.core;

public class PalindromeLinkedList {

    public static void main(String[] args) {
        int raw[] = {1, 2, 4, 6, 9, 3, 1, 3, 9, 6, 4, 2, 1}; // 模拟单链表
        int a[] = new int[raw.length/2 + 1];
        int len = raw.length;
        boolean ispalindromed = true;
        // 按顺序遍历链表
        for (int i = 0; i < len; i++) {
            if (i < len/2 + 1) {
                a[i] = raw[i];
            } else {
                a[len-1 - i] = raw[i] - a[len-1 - i];
                if (a[len-1 - i] != 0) {
                    ispalindromed = false;
                    break;
                }
            }
        }
        if (ispalindromed) {
            System.out.println("原链表是回文链表");
        } else {
            System.out.println("原链表不是回文链表");
        }
    }
}
