package com.coeus.spark.core;

public class MatrixArea {

    public static void main(String[] args) {
        int matrix[][] = {{1, 0, 1, 0, 0}, {1, 0, 1, 1, 1}, {1, 1, 1, 1, 1}, {1, 0, 0, 1, 0}};
        int maxLen = 0;
        int startIndex = 0;
        int endIndex = 0;
        for (int i = 0; i < matrix.length; i++) {
            for (int j = 0; j < matrix[i].length; j++) {
                if (matrix[i][j] == 1) {
                    endIndex++;
                } else {
                    startIndex++;
                    endIndex++;
                }

            }

        }
    }
}
