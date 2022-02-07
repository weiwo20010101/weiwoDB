package com.weiwodb.backend.common;

/**
 * @Author weiwo
 * @Created on 2022/1/25
 */
public class SubArray {
    public byte[] raw;
    public int start;
    public int end;

    public SubArray(byte[] raw, int start, int end) {
        this.raw = raw;
        this.start = start;
        this.end = end;
    }
}
