package com.jz.api.com.jz.gateway.common.signtype.signutil;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * MD5加密算法
 *
 * @author Admin
 * @version $Id: Md5Utils.java 2014年9月3日 下午4:01:08 $
 */
public class MD5 {
    private static Logger log = LoggerFactory.getLogger(MD5.class);
    private static final String SIGN_TYPE = "MD5";
    private static final String CHARSET_NAME = "UTF-8";
    private static final String salt = "3zsAa6W9gfSMMhPSlQTdWFUSHY3LS8Vb";

    /**
     * MD5加密
     *
     * @param data
     * @return
     * @throws NoSuchAlgorithmException
     */
    public static String encrypt(byte[] data) {
        try {
            MessageDigest md5 = MessageDigest.getInstance(SIGN_TYPE);
            md5.update(data);
            return byte2hex(md5.digest());
        } catch (NoSuchAlgorithmException e) {
            log.debug("md5 加密异常", e);
        }
        return "";
    }

    /**
     * MD5加密
     *
     * @param str
     * @return
     * @throws NoSuchAlgorithmException
     */
    public static String encrypt(String str) {
        try {
            MessageDigest md5 = MessageDigest.getInstance(SIGN_TYPE);
            md5.update((str + salt).getBytes(CHARSET_NAME));
            return byte2hex(md5.digest());
        } catch (Exception e) {
            log.debug("md5 加密异常", e);
        }
        return null;
    }

    /**
     * MD5加盐加密
     *
     * @param str
     * @param salt
     * @return
     * @throws NoSuchAlgorithmException
     */
    public static String encrypt(String str, String salt) {
        try {
            MessageDigest md5 = MessageDigest.getInstance(SIGN_TYPE);
            md5.update((str + salt).getBytes(CHARSET_NAME));
            return byte2hex(md5.digest());
        } catch (Exception e) {
            if (log.isDebugEnabled()) {
                log.debug("md5 加密异常", e);
            }
        }
        return "";
    }

    /**
     * MD5加盐加密 16位
     *
     * @param str
     * @param salt
     * @return
     * @throws NoSuchAlgorithmException
     */
    public static String encrypt16(String str, String salt) {
        try {
            MessageDigest md5 = MessageDigest.getInstance(SIGN_TYPE);
            md5.update((str + salt).getBytes(CHARSET_NAME));
            return byte2hex(md5.digest()).substring(8, 24);
        } catch (Exception e) {
            if (log.isDebugEnabled()) {
                log.debug("md5 加密异常", e);
            }
        }
        return "";
    }

    public static String encrypt(String str, String salt, String charset) {
        try {
            MessageDigest md5 = MessageDigest.getInstance(SIGN_TYPE);
            md5.update((str + salt).getBytes(charset));
            return byte2hex(md5.digest());
        } catch (Exception e) {
            if (log.isDebugEnabled()) {
                log.debug("md5 加密异常", e);
            }
        }
        return "";
    }

    public static String byte2hex(byte[] bytes) {
        StringBuilder sign = new StringBuilder();
        for (int i = 0; i < bytes.length; i++) {
            String hex = Integer.toHexString(bytes[i] & 0xFF);
            if (hex.length() == 1) {
                sign.append("0");
            }
            sign.append(hex.toUpperCase());
        }

        return sign.toString();
    }

    public static byte[] hex2byte(String str) {
        if (str == null) {
            return null;
        }
        str = str.trim();
        int len = str.length();
        if (len <= 0 || len % 2 == 1) {
            return null;
        }
        byte[] b = new byte[len / 2];
        try {
            for (int i = 0; i < str.length(); i += 2) {
                b[(i / 2)] = (byte) Integer.decode("0x" + str.substring(i, i + 2)).intValue();
            }
            return b;
        } catch (Exception e) {
        }
        return null;
    }

    /**
     * 给TOP请求做MD5签名。
     *
     * @param sortedParams 所有字符型的TOP请求参数
     * @param secret       签名密钥
     * @return 签名
     * @throws IOException
     */
    public static String signRequestNew(Map<String, String> sortedParams, String secret) throws IOException {
        // 第一步：把字典按Key的字母顺序排序
        List<String> keys = new ArrayList<String>(sortedParams.keySet());
        Collections.sort(keys);

        // 第二步：把所有参数名和参数值串在一起
        StringBuilder query = new StringBuilder();
        for (int i = 0; i < keys.size(); i++) {
            String key = keys.get(i);
            String value = sortedParams.get(key);
            if (!StringUtils.isEmpty(key) && !StringUtils.isEmpty(value) && !"sign".equals(key)) {
                query.append(key).append("=").append(value);
            }
        }
        log.info("获取当APP请求参数，签名前值为：" + query.toString());
        return MD5.encrypt(query.toString(), secret);
    }

    /**
     * 把数组所有元素字典排序，并按照“参数=参数值”的模式用“&”字符拼接成字符串
     * @param params 需要排序并参与字符拼接的参数组
     * @return 拼接后字符串　
     */
    public static String stringNormalSort(Map<String, Object> params) {
        List<String> keys = new ArrayList<String>(params.keySet());
        Collections.sort(keys);
        String prestr = "";
        for (int i = 0; i < keys.size(); i++) {
            String key = keys.get(i);
            String value = params.get(key).toString();
            try {
                value = URLEncoder.encode(value, "UTF-8");
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }
            if (i == keys.size() - 1) {//拼接时，不包括最后一个&字符
                prestr = prestr + key + "=" + value;
            } else {
                prestr = prestr + key + "=" + value + "&";
            }
        }
        return prestr;
    }

    public static void main(String[] args) {
        System.out.println(encrypt("200128008012abc@123", ""));
    }
}
