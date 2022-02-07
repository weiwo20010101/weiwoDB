package com.weiwodb.transport;

/**
 * @Author weiwo
 * @Created on 2022/1/30
 */
public class Package {
    byte[] data;
    Exception e;

    public Package(byte[] data, Exception e) {
        this.data = data;
        this.e = e;
    }

    public byte[] getData() {
        return data;
    }



    public Exception getE() {
        return e;
    }

}
