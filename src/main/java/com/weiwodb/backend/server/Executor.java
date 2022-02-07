package com.weiwodb.backend.server;

import com.weiwodb.backend.parser.Parser;
import com.weiwodb.backend.tbm.BeginRes;
import com.weiwodb.backend.tbm.TableManager;
import com.weiwodb.common.utils.statement.*;
import com.weiwodb.common.utils.Error;
//首先Parser解析sql
//然后根据返回值类型 调用tbm中的方法进行操作 最终返回结果
public class Executor {
    private long xid;
    TableManager tbm;

    public Executor(TableManager tbm) {
        this.tbm = tbm;
        this.xid = 0;
    }

    public void close() {
        if(xid != 0) {
            System.out.println("Abnormal Abort: " + xid);
            tbm.abort(xid);
        }
    }

    public byte[] execute(byte[] sql) throws Exception {//execute方法抛出异常 获取异常，放到Package中发送出去
        System.out.println("Execute: " + new String(sql));
        Object stat = Parser.Parse(sql);//解析sql语句
        if(Begin.class.isInstance(stat)) {
            //?
            if(xid != 0) {//比如多次begin 就会在这里被检查到
                throw Error.NestedTransactionException;
            }
            //xid + "begin".getBytes()
            BeginRes r = tbm.begin((Begin)stat);
            xid = r.xid;
            return r.result;
        } else if(Commit.class.isInstance(stat)) {
            if(xid == 0) {
                //如果没有begin，直接就commit就会直接提交
                throw Error.NoTransactionException;
            }
            byte[] res = tbm.commit(xid);
            xid = 0;//提交了之后xid属性记为0
            return res;
        } else if(Abort.class.isInstance(stat)) {
            if(xid == 0) {//如果没有begin，而直接输入Abort命令就会抛出无事务异常
                throw Error.NoTransactionException;
            }
            byte[] res = tbm.abort(xid);
            xid=0;
            return res;
        } else {
            return execute2(stat);
        }
    }

    private byte[] execute2(Object stat) throws Exception {
        boolean tmpTransaction = false;
        Exception e=null;
        if(xid == 0) {//如果是直接输入命令，即xid==0,那么就会开启一个默认隔离级别的事务
            tmpTransaction = true;
            BeginRes r = tbm.begin(new Begin());
            xid = r.xid;
        }
        try {
            byte[] res = null;
            if(Show.class.isInstance(stat)) {
                res = tbm.show(xid);
            } else if(Create.class.isInstance(stat)) {
                res = tbm.create(xid, (Create)stat);
            } else if(Select.class.isInstance(stat)) {
                res = tbm.read(xid, (Select)stat);
            } else if(Insert.class.isInstance(stat)) {
                res = tbm.insert(xid, (Insert)stat);
            } else if(Delete.class.isInstance(stat)) {
                res = tbm.delete(xid, (Delete)stat);
            } else if(Update.class.isInstance(stat)) {
                res = tbm.update(xid, (Update)stat);
            }
            return res;
        } catch(Exception e1) {
            e = e1;
            throw e;
        } finally {
            if(tmpTransaction) {//如果是临时事务，即每一个命令都是一个事务，那么
                //在命令执行结束之后就需要根据是否出现异常来进行决定提交或者回滚
                if(e != null) {
                    tbm.abort(xid);
                } else {
                    tbm.commit(xid);
                }
                xid = 0;
            }
        }
    }
}
