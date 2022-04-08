package com.coeus.spark.core;

public class Palindrome {

    public static void main(String[] args) {
        // int raw[] = {1, 2, 4, 6, 9, 3, 3, 9, 6, 5, 2, 1}; // 模拟单链表
        int raw[] = {1}; // 模拟单链表
        int len = raw.length;
        boolean ispalindromed = true;
        // 按顺序遍历链表
        for (int i = 0; i < len; i++) {
            if (i >= len/2 + 1) {
                raw[len-1 - i] = raw[i] - raw[len-1 - i];
                if (raw[len-1 - i] != 0) {
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
