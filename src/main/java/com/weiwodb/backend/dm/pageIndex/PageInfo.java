package com.weiwodb.backend.dm.pageIndex;

/**
 * @Author weiwo
 * @Created on 2022/1/26
 */
public class PageInfo {
    public int pageNumber;
    public int freeSpace;

    public PageInfo(int pageNumber, int freeSpace) {
        this.pageNumber = pageNumber;
        this.freeSpace = freeSpace;
    }
}
