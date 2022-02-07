package com.weiwodb.backend.vm;

import com.weiwodb.backend.tm.TransactionManager;

/**
 * 该类的方法参数都是 事务管理器tm，事务类t，和记录e
 */
public class Visibility {
    //解决版本跳跃问题

    /**
     * T1 begin
     * T2 begin
     * R1(X) // T1读取x0
     * R2(X) // T2读取x0
     * U1(X) // T1将X更新到x1
     * T1 commit
     * U2(X) // T2将X更新到x2
     * T2 commit
     */
    public static boolean isVersionSkip(TransactionManager tm, Transaction t, Entry e) {
       long xmax=e.getXMAX();//记录最后一次被修改的事务id
        if(t.level==0) {
            return false;//0表示提交度，允许版本跳跃
        } else {//非0表示可重复读，不允许版本跳跃
            // 当事务T2想要修改记录x时，发现x已经被一个不可见的已经提交的事务T1修改了（不可见并且提交），那么就发生了版本跳跃问题
            //判断条件为：t1已经提交并且t1不可见
            return tm.isCommitted(xmax)&&(xmax>t.xid||t.isInSnapShot(xmax));

        }
    }

    public static boolean isVisible(TransactionManager tm, Transaction t, Entry e) {
        if(t.level == 0) {//不同隔离级别
            return readCommitted(tm, t, e);
        } else {
            return repeatableRead(tm, t, e);
        }
    }
     /*
     判断某个记录对某个事务是否可见，级别为提交读。
     如果自己创建，并且没有删除，可见
     如果由一个已经提交了的事务创建，没有删除，或者(删除它的事务还没有提交，可见。

      */
    private static boolean readCommitted(TransactionManager tm, Transaction t, Entry e) {
       long xid=t.xid;
       long XMIN=e.getXMIN();
       long XMAX=e.getXMAX();
       if(XMIN==xid&&XMAX==0) return  true;

       if(tm.isCommitted(XMIN)){
           if(XMAX==0) return true;
           if(XMAX!=xid){
               if(!tm.isCommitted(XMAX)){
                   return  true;
               }
           }
       }

       return false;
    }

    /**
     * 为了解决不可重复读的问题，我们规定事务只能读取它开始时，就已经结束的事务。
     * 增加了这条规定之后，事务需要忽略：
     * 1在本事务开始时，还没有开始的事务。只需要比较事务id，即可。
     * 2在本事务开始时，还是active状态的数据。需要在本事务开始时，记录下当前活跃的所有事务，如果记录的的某个版本的，其创建事务id
     * 即XMIN，在这些事务中，也应当对本事务不可见。
     * 应该需要提供一个结构，来抽象一个事务，来保存快照数据。
     *
     */
    //判断如下
    /*
    1.
     */
    private static boolean repeatableRead(TransactionManager tm, Transaction t, Entry e) {
        long xid = t.xid;
        long xmin = e.getXMIN();
        long xmax = e.getXMAX();
        if(xmin == xid && xmax == 0) return true;
        //对于xmin的要求
        if(tm.isCommitted(xmin) && xmin < xid && !t.isInSnapShot(xmin)) {
            //对于xmax的要求
            //如果没有该记录没有被某一个事务删除 return true;
            if(xmax == 0) return true;
            if(xmax != xid) {
                /*
                如果不是本事务中删除
                1. 删除操作对应的事务还没有提交
                2. 删除操作对于的事务是在该事务之后才创建爱你
                3. 删除操作对应的事务在事务xid判断时正在活跃

                这三种情况都读取不到删除操作，因此我们认为还是可以读到数据的.
                 */
                if(!tm.isCommitted(xmax) || xmax > xid || t.isInSnapShot(xmax)) {
                    return true;
                }
            }
        }
        return false;
    }

}
