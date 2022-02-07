package com.weiwodb.common.utils;

import java.security.SecureRandom;

/**
 * @Author weiwo
 * @Created on 2022/1/26
 */
public class RandomUtil {
    public static byte[] randomBytes(int length){
        SecureRandom random = new SecureRandom();
        byte[] res=new byte[length];
        random.nextBytes(res);
        return res;
    }
}
