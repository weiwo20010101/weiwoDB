package com.weiwodb.backend.tbm;

import java.util.Arrays;
import java.util.List;

import com.google.common.primitives.Bytes;

import com.weiwodb.backend.im.BPlusTree;
import com.weiwodb.common.utils.Error;
import com.weiwodb.backend.tm.TransactionManagerImpl;
import com.weiwodb.common.utils.Panic;
import com.weiwodb.common.utils.ParseStringRes;
import com.weiwodb.common.utils.Parser;
import com.weiwodb.common.utils.statement.SingleExpression;

/**
 * field 表示字段信息
 * 二进制格式为：
 * [FieldName][TypeName][IndexUid]
 * 如果field无索引，IndexUid为0,如果有索引，那么IndexUid指向索引二叉树的根
 * 支持三种字段类型 int long string
 * 基于vm，数据保存在entry中
 */
public class Field {
    long uid;//字段uid
    private Table tb;
    String fieldName;
    String fieldType;
    private long index;
    private BPlusTree bt;//通过bootUid(index),dm获取,包含dataItem+lock


    public Field(long uid, Table tb) {
        this.uid = uid;
        this.tb = tb;
    }

    public Field(Table tb, String fieldName, String fieldType, long index) {
        this.tb = tb;
        this.fieldName = fieldName;
        this.fieldType = fieldType;
        this.index = index;
    }


    //uid->FieldUID   //基于entry
    //结构为  xmin,xmax,[data :fileName,fieldType,index->
    public static Field loadField(Table tb, long uid) {
        byte[] raw = null;
        try {
            //raw = entry.data() 以拷贝的形式返回   超级事务的隔离级别为 提交读
            raw = ((TableManagerImpl)tb.tbm).vm.read(TransactionManagerImpl.SUPER_XID, uid);
        } catch (Exception e) {
            Panic.panic(e);
        }
        assert raw != null;
        //raw:fileName,fileType,index

        return new Field(uid, tb).parseSelf(raw);
    }


    private Field parseSelf(byte[] raw) {
        int position = 0;

        ParseStringRes res = Parser.parseString(raw);
        fieldName = res.str;//获取字段名称
        position += res.next;
        res = Parser.parseString(Arrays.copyOfRange(raw, position, raw.length));
        fieldType = res.str;//获取字段类型
        position += res.next;
        //获取索引
        this.index=Parser.BytesToLong(Arrays.copyOfRange(raw, position, position+8));
        if(index != 0) {
            //如果有索引
            try {
                //在字段这个类中加载索引 b+tree
                bt = BPlusTree.load(index, ((TableManagerImpl)tb.tbm).dm);
            } catch(Exception e) {
                Panic.panic(e);
            }
        }
        return this;
    }

    public static Field createField(Table tb, long xid, String fieldName, String fieldType, boolean indexed) throws Exception {
        typeCheck(fieldType);
        //创建一列，需要指定哪个表，字段名称，字段类型，是否索引,如果是则创建一个索引
        Field f = new Field(tb, fieldName, fieldType, 0);
        if(indexed) {
            //Im与dm直接相关，不需要引用vm
            //此处的index对应的位置存储这个根节点uid,read之后
            long index = BPlusTree.create(((TableManagerImpl)tb.tbm).dm);//创建索引，传入dm，也就是创建一个b+树
            BPlusTree bt = BPlusTree.load(index, ((TableManagerImpl)tb.tbm).dm);
            f.index = index;
            f.bt = bt;
        }
        f.persistSelf(xid);//传入xid，操作的位置是由dm根据页面索引找的
        return f;
    }
    //
    private void persistSelf(long xid) throws Exception {
        byte[] nameRaw = Parser.StringToBytes(fieldName);
        byte[] typeRaw = Parser.StringToBytes(fieldType);
        byte[] indexRaw = Parser.longToBytes(index);
        //vm的插入                                          //xid,raw
        this.uid = ((TableManagerImpl)tb.tbm).vm.insert(xid, Bytes.concat(nameRaw, typeRaw, indexRaw));
    }
    //
    private static void typeCheck(String fieldType) throws Exception {
        if(!"int32".equals(fieldType) && !"int64".equals(fieldType) && !"string".equals(fieldType)) {
            throw Error.InvalidFieldException;
        }
    }

    public boolean isIndexed() {
        return index != 0;
    }

    public void insert(Object key, long uid) throws Exception {
        long uKey = value2Uid(key);//key -> long
        //根据fieldType分成不同的key计算方式
        //int32 int64 String
        bt.insert(uKey, uid);
    }

    public List<Long> search(long left, long right) throws Exception {
        return bt.searchRange(left, right);
        //B+树搜索操作
    }

    public Object string2Value(String str) {
        //可以作为简单表达式中比较的值使用
        //根据字符串类型转为
        switch(fieldType) {
            case "int32":
                return Integer.parseInt(str);
            case "int64":
                return Long.parseLong(str);
            case "string":
                return str;
        }
        return null;
    }

    //
    public long value2Uid(Object key) {
        long uid = 0;
        switch(fieldType) {
            case "string":
                uid = Parser.StringToUid((String)key);
                break;
            case "int32":
                int uint = (int)key;
                return (long)uint;
            case "int64":
                uid = (long)key;
                break;
        }
        return uid;
    }

    //
    public byte[] value2Raw(Object v) {
        byte[] raw = null;
        switch(fieldType) {
            case "int32":
                raw = Parser.intToBytes((int)v);
                break;
            case "int64":
                raw = Parser.longToBytes((long)v);
                break;
            case "string":
                raw = Parser.StringToBytes((String)v);
                break;
        }
        return raw;
    }

    class ParseValueRes {
        Object v;
        int shift;
    }

    //
    public ParseValueRes parserValue(byte[] raw) {
        ParseValueRes res = new ParseValueRes();
        switch(fieldType) {
            case "int32":
                res.v = Parser.BytesToInt(Arrays.copyOf(raw, 4));
                res.shift = 4;
                break;
            case "int64":
                res.v = Parser.BytesToLong(Arrays.copyOf(raw, 8));
                res.shift = 8;
                break;
            case "string":
                ParseStringRes r = Parser.parseString(raw);
                res.v = r.str;
                res.shift = r.next;
                break;
        }
        return res;
    }
    //
    public String printValue(Object v) {
        String str = null;
        switch(fieldType) {
            case "int32":
                str = String.valueOf((int)v);
                break;
            case "int64":
                str = String.valueOf((long)v);
                break;
            case "string":
                str = (String)v;
                break;
        }
        return str;
    }

    //
    @Override
    public String toString() {
        return new StringBuilder("(")
                .append(fieldName)
                .append(", ")
                .append(fieldType)
                .append(index!=0?", Index":", NoIndex")
                .append(")")
                .toString();
    }

    public FieldCalRes calExp(SingleExpression exp) throws Exception {
        //表达式的是三个属性都是String类型 string2Value根据字段类型转化
        //计算where的范围 闭区间,最小值为0，最大值为Long.MAX_VALUE
        Object v = null;
        FieldCalRes res = new FieldCalRes();
        switch(exp.compareOp) {
            case "<":
                res.left = 0;
                v = string2Value(exp.value);
                res.right = value2Uid(v);//value2Uid将v（三种类型）都转成long类型
                if(res.right > 0) {
                    res.right --;
                }
                break;
            case "=":
                v = string2Value(exp.value);
                res.left = value2Uid(v);
                res.right = res.left;
                break;
            case ">":
                res.right = Long.MAX_VALUE;
                v = string2Value(exp.value);
                res.left = value2Uid(v) + 1;
                break;
        }
        return res;
    }
}
