package com.weiwodb.backend.dm.pageIndex;

import com.weiwodb.backend.dm.page.Page;
import com.weiwodb.backend.dm.pageCache.PageCache;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @Author weiwo
 * @Created on 2022/1/26
 */
public class PageIndex {
    private static final int BLOCK_NUMBER=40;
    private static final int BLOCK_SIZE= PageCache.PAGE_SIZE/BLOCK_NUMBER;

   private List<List<PageInfo>> pageIndex;
    public PageIndex(){
        pageIndex= Collections.synchronizedList(new ArrayList<List<PageInfo>>());
    }
    public void add(int pageNumber,int freeSpace){
        int index=freeSpace/BLOCK_SIZE;
        //index=2 表示freeSpace的大小是[2*size,3*size)
        //index=0 表示freeSpace的大小是[0,size)
        //index=40表示freeSpace的大小是[40*size,41*size)不存在的
        //index=39表示freeSpace的大小是[39*size,40*size)
        //[0,39] 最大可能index值为39 最小值为0
       pageIndex.get(index).add(new PageInfo(pageNumber,freeSpace));
    }
    /*
    从页面索引中获取页面信息
   */
    public PageInfo find(int space){//需要至少space大小的空间
         int n=space/BLOCK_SIZE;
         int remove=-1;
         PageInfo res=null;
         while(n<=BLOCK_NUMBER-1&&res==null){
             List<PageInfo> list=pageIndex.get(n);
            for(int i=0;i<list.size()&&res==null;i++) {
                PageInfo info = list.get(i);
                if (info.freeSpace >= space) {
                    res = info;
                    remove = i;
                    break;
                }
            }
            list.remove(remove);//注意，获取之后会将其remove掉，用过之后可以再次添加回来
         }
         return res;
    }

}


//声哥代码
//        import java.util.ArrayList;
//        import java.util.List;
//        import java.util.concurrent.locks.Lock;
//        import java.util.concurrent.locks.ReentrantLock;
//
//        import top.guoziyang.mydb.backend.dm.pageCache.PageCache;
////ok
//public class PageIndex {
//    // 将一页划成40个区间
//    private static final int INTERVALS_NO = 40;
//    private static final int THRESHOLD = PageCache.PAGE_SIZE / INTERVALS_NO;
//
//    private Lock lock;
//    private List<PageInfo>[] lists;
//
//    @SuppressWarnings("unchecked")
//    public PageIndex() {
//        lock = new ReentrantLock();
//        lists = new List[INTERVALS_NO+1];//[0,40]
//        for (int i = 0; i < INTERVALS_NO+1; i ++) {
//            lists[i] = new ArrayList<>();
//        }
//    }
//
//    public void add(int pgno, int freeSpace) {
//        lock.lock();
//        try {
//            //增加空闲空间，计算机得到空闲空间对应的区块树
//            int number = freeSpace / THRESHOLD;
//            //如果freespace=2.5 number=2
//            lists[number].add(new PageInfo(pgno, freeSpace));
//        } finally {
//            lock.unlock();
//        }
//    }
//
//    public PageInfo select(int spaceSize) {
//        lock.lock();
//        try {
//            //1  比如我现在需要2.5的空间那么number=3，也就是在下标为3出寻找需要的空间
//            int number = spaceSize / THRESHOLD;
//            if(number < INTERVALS_NO) number ++;
//            //当需要一页的空间是，可以进入循环
//            while(number <= INTERVALS_NO) {
//                if(lists[number].size() == 0) {//没有空间了
//                    number ++;
//                    continue;//
//                }
//                //循环结束，表示找到了需要的空间，那么List中remove第一条数据
//                return lists[number].remove(0);
//            }
//            return null;
//        } finally {
//            lock.unlock();
//        }
//    }
//
//}

