package com.weiwodb.backend.common;

import com.weiwodb.common.utils.Error;

import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @Author weiwo
 * @Created on 2022/1/25
 */
//T 资源类型
public abstract class AbstractCache<T> {
    private HashMap<Long,T> cache;
    private HashMap<Long,Integer>references;
    private HashMap<Long,Boolean>getting;

    private int maxResource;
    private int count;
    private Lock lock;

    public AbstractCache(int maxResource){
        this.maxResource=maxResource;
        cache=new HashMap<>();
        references=new HashMap<>();
        getting=new HashMap<>();
        lock=new ReentrantLock();//获取锁
    }

    /**
     * 获取缓存
     * @param key
     */
    protected T get(long key) throws Exception{
        while (true){
            lock.lock();
            if(getting.containsKey(key)){
                lock.unlock();
                try {
                    Thread.sleep(300);
                }catch (Exception e){
                    e.printStackTrace();//显示一次
                    continue;
                }
                continue;
            }
            if(cache.containsKey(key)){
                T res=cache.get(key);
                references.put(key,references.get(key)+1);
                lock.unlock();
                return  res;
            }
            if(maxResource>0&&maxResource==count){
                lock.unlock();
                throw Error.CacheFullException;
            }
             count++;
             getting.put(key,true);
             lock.unlock();;
             break;
        }
        T res=null;
        try {
            res=getForCache(key);
        }catch (Exception e){
            lock.lock();
            //遇到count,map的修改操作，+-操作时都需要进行加锁
            count--;
            getting.remove(key);
            lock.unlock();
            throw  e;
        }
        lock.lock();
        //操作缓存
        cache.put(key,res);
        references.put(key,1);
        getting.remove(key);
        lock.unlock();
        return  res;
    }
 protected void release(long key){
        lock.lock();//对释放缓存的行为加锁
   try {
       int ref= references.get(key)-1;
       if(ref==0){
           T willFlushed=cache.get(key);
           releaseForCache(willFlushed);
           cache.remove(key);
           references.remove(key);
           count--;
       }else {
           references.put(key,ref);
       }
   }finally {
       lock.unlock();
   }
 }

 protected  void close(){
        lock.lock();
        try {
            Set<Long> keys = cache.keySet();
            for(long key:keys){
                //释放key 写回
                //引用计数中删除
                //缓存中删除
                release(key);//该操作更大的意义是写回,count-1
                references.remove(key);//强制删除，不管之前是否因为引用计数为0而删除
                cache.remove(key);//强制删除，不管之前是否因为引用计数为0而删除
            }
        }finally {
            lock.unlock();
        }
 }
    /**
     * 当资源不在缓存中时的获取行为
     */
    protected  abstract  T getForCache(long key) throws  Exception;

    /**
     * 当资源被驱逐时的写回行为
     */
    protected  abstract  void  releaseForCache(T obj);
}
