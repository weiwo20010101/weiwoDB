package com.weiwodb.backend.parser;


import com.weiwodb.common.utils.Error;


public class Tokenizer {
    private byte[] stat;//需要解析的语句
    private int pos;//position
    private String currentToken;//当前token
    private boolean flushToken;//是否是flushToken

    private Exception err;

    //
    public Tokenizer(byte[] stat) {
        this.stat = stat;
        this.pos = 0;
        this.currentToken = "";
        this.flushToken = true;
    }

    public String peek() throws Exception {
        if(err != null) {
            throw err;
        }
        //
        if(flushToken) {
            String token = null;
            try {
                token = next();
            } catch(Exception e) {
                err = e;
                throw e;
            }
            currentToken = token;
            flushToken = false;
        }
        //如果没有pop刷新一下，就会一直返回currentToken
        //pop刷新之后 调用next方法获取一个字符串 并且将其设置为当前token
        return currentToken;
    }
   //
    public void pop() {
        flushToken = true;
    }

    public byte[] errStat() { //错误表述就是在position对应的地方添加 字符 "<< ."
        byte[] res = new byte[stat.length+3];
        System.arraycopy(stat, 0, res, 0, pos);
        System.arraycopy("<< ".getBytes(), 0, res, pos, 3);
        System.arraycopy(stat, pos, res, pos+3, stat.length-pos);
        return res;
    }
     //pop对于position有约束  if(post>stat.length) pos=stat.length;
    private void popByte() {
        pos ++;
        if(pos > stat.length) {
            pos = stat.length;
        }
    }
    //一个字节一个字节的获取 与position属性有环
    // if(pos==stat.length)
    private Byte peekByte() {
        if(pos == stat.length) {
            return null;
        }
        return stat[pos];
    }
     //获取下一个String
    private String next() throws Exception {
        if(err != null) {
            throw err;
        }
        return nextMetaState();
    }

    private String nextMetaState() throws Exception {
        //去掉空格 直到b非空格 非lt 非换行符
        while(true) {
            Byte b = peekByte();
            if (b == null) {
                return "";
            }
            if (!isBlank(b)) {
                break;
            }
            popByte();
        }

        byte b = peekByte();//获取空格之后的第一个字节
        //ok
        if(isSymbol(b)) {
            //如果是 > < * {} = , ()
            popByte();
            return new String(new byte[]{b});
            //如果是引号
        } else if(b == '"' || b == '\'') { //如果b是单引号或者双引号
            return nextQuoteState();//获取引用的内容
        } else if(isAlphaBeta(b) || isDigit(b)) {//数字或者字母
            return nextTokenState();
        } else {
            err = Error.InvalidCommandException;
            throw err;
        }
    }
//下一个String  那么就获取下一个String
    private String nextTokenState() throws Exception {
        StringBuilder sb = new StringBuilder();
        while(true) {
            Byte b = peekByte();//非字母
            //比如 int32  如果b==null 或者 不是 字母，数字 字符 _ 之间的任意一个，那就返回
            if(b == null || ! (isAlphaBeta(b) || isDigit(b) || b == '_')  ) {
                if(b != null && isBlank(b)) {
                    popByte();
                }
                return sb.toString();
            }
            sb.append(new String(new byte[]{b}));
            popByte();
        }
    }
    //数字
    static boolean isDigit(byte b) {
        return (b >= '0' && b <= '9');
    }
    //字母
    static boolean isAlphaBeta(byte b) {
        return ((b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z'));
    }
    //??
    private String nextQuoteState() throws Exception {
        //获取一个字节 " '
        byte quote = peekByte();
        popByte();
        //创建StringBuilder
        StringBuilder sb = new StringBuilder();
        while(true) {
            //获取下一个字节
            Byte b = peekByte();//大写B 包装类 Byte
            if(b == null) {//如果下一个字节为空 防止没有匹配的 " ' 抛出异常
                err = Error.InvalidCommandException;
                throw err;
            }
            //如果下一个字节==quote 也弹出break  返回null
            if(b == quote) { //如果遇到 匹配的 " 'position++ 终止循环
                popByte();
                break;
            }
            //如果不同
            sb.append(new String(new byte[]{b}));
//            sb.append(b)
            popByte();
        }
        return sb.toString();
    }
  //特殊字母
    static boolean isSymbol(byte b) {
        return (b == '>' || b == '<' || b == '=' || b == '*' ||
		b == ',' || b == '(' || b == ')');
    }
 //空格 \n \t
    static boolean isBlank(byte b) {
        return (b == '\n' || b == ' ' || b == '\t');
    }
}
