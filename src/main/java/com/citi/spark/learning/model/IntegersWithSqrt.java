package com.citi.spark.learning.model;

import java.io.Serializable;

public class IntegersWithSqrt implements Serializable {
    private int originalNumber;
    private double sqrt;

    public IntegersWithSqrt(int originalNumber) {
        this.originalNumber = originalNumber;
        this.sqrt = Math.sqrt(originalNumber);
    }

    @Override
    public String toString() {
        return "IntegersWithSqrt{" +
                "originalNumber=" + originalNumber +
                ", sqrt=" + sqrt +
                '}';
    }
}
