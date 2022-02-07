package com.weiwodb.backend.vm;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.weiwodb.common.utils.Error;

/**
 * 维护了一个依赖等待图，以进行死锁检测
 */
public class LockTable {

    private Map<Long, List<Long>> x2u;  // 某个XID已经获得的资源的UID列表
    private Map<Long, Long> u2x;        // UID被某个XID持有
    private Map<Long, List<Long>> wait; // 正在等待UID的XID列表
    private Map<Long, Lock> waitLock;   // 正在等待资源的XID的锁
    private Map<Long, Long> waitU;      // XID正在等待的UID
    private Lock lock;

    public LockTable() {
        x2u = new HashMap<>();
        u2x = new HashMap<>();
        wait = new HashMap<>();
        waitLock = new HashMap<>();
        waitU = new HashMap<>();
        lock = new ReentrantLock();
    }

   public Lock add(long xid,long uid) throws  Exception{
        lock.lock();
        try {
            if(isInList(x2u,xid,uid)) return  null;
            if(!u2x.containsKey(uid)){//uid被xid拥有
                u2x.put(uid,xid);//被拥有
                putIntoList(x2u,xid,uid);//放入xid对应的list中
                return null;
            }
            //说明uid被其它xid占用
            waitU.put(xid,uid);
            putIntoList(wait,uid,xid);
            if(hasDeadLock()){
                waitU.remove(xid);
                removeFromList(wait,uid,xid);
                throw Error.DeadlockException;
            }
            Lock l=new ReentrantLock();
            l.lock();
            waitLock.put(xid,l);
            return  l;
        }finally {
            lock.unlock();
        }
   }
   public void remove(long xid){
        lock.lock();
        try {
         List<Long> list=x2u.get(xid);
         if(list!=null){
             while (!list.isEmpty()){
                 long uid= list.remove(0);
                 selectNewXID(uid);
             }
         }
        x2u.remove(xid);
         waitLock.remove(xid);
         waitU.remove(xid);
        }finally {
            lock.unlock();
        }
   }

    private void selectNewXID(long uid) {
        u2x.remove(uid);
        List<Long> list=wait.get(uid);
        assert  list.size() > 0;
        while (list.size()>0){
            long xid=list.remove(0);
            if(!waitLock.containsKey(xid)){
                continue;
            }else {
                u2x.put(uid,xid);
                Lock lock=waitLock.remove(xid);
                waitU.remove(xid);
                //xid获取资源之后，需要去除 1. waitU  2 waitLock 对应的key
                lock.unlock();//释放锁
                break;
            }
        }
        if(list.size()==0) wait.remove(uid);
    }
    int v=1;
    private Map<Long,Integer> map=new HashMap<>();
    private boolean hasDeadLock() {
        map.clear();
        v=1;
       for(long key:map.keySet()){
           Integer val=map.get(key);
           if(val!=null&&val>0){
               continue;
           }
           v++;
           if(dfs(key)){
               return  true;
           }
       }
       return  false;
    }
    public boolean dfs(long key){
        Integer temp=map.get(key);
        if(temp!=null&&temp==v){
            return  true;
        }
        if(temp!=null&&temp<v){
            return false;
        }
        //如果map中不存在
        map.put(key,v);
        Long waited     = waitU.get(key);
        if(waited==null) return false;
        Long owned = u2x.get(waited);
        if(owned==null) return false;
//        assert  owned!=null;
        return dfs(owned);
    }

    private void removeFromList(Map<Long, List<Long>> listMap, long uid0, long uid1) {
        List<Long> l = listMap.get(uid0);
        if(l == null) return;
        Iterator<Long> i = l.iterator();
        while(i.hasNext()) {
            long e = i.next();
            if(e == uid1) {
                i.remove();
                break;
            }
        }
        if(l.size() == 0) {
            listMap.remove(uid0);
        }
    }

    private void putIntoList(Map<Long, List<Long>> listMap, long uid0, long uid1) {
        if(!listMap.containsKey(uid0)) {
            listMap.put(uid0, new ArrayList<>());
        }
        listMap.get(uid0).add(0, uid1);
    }

    private boolean isInList(Map<Long, List<Long>> listMap, long uid0, long uid1) {
        List<Long> l = listMap.get(uid0);
        if(l == null) return false;
        Iterator<Long> i = l.iterator();
        while(i.hasNext()) {
            long e = i.next();
            if(e == uid1) {
                return true;
            }
        }
        return false;
    }

}
