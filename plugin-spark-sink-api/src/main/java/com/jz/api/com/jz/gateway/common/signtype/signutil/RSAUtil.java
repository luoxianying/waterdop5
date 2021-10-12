package com.jz.api.com.jz.gateway.common.signtype.signutil;

//import sun.misc.BASE64Decoder;
//import sun.misc.BASE64Encoder;
import org.apache.commons.codec.binary.Base64;

import javax.crypto.Cipher;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.security.*;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * @author ZC
 * @PACKAGE_NAME: com.jz.gateway.common.signtype.signutil
 * @PROJECT_NAME: jz_dm_gateway
 * @NAME: RSAUtil
 * @DATE: 2021-4-15/16:05
 * @DAY_NAME_SHORT: 周四
 * @Description: RSA工具类
 **/
public class RSAUtil {

    public static final String SIGN_TYPE_RSA = "RSA";

    /**
     * RSA最大加密明文大小
     */
    private static final int MAX_ENCRYPT_BLOCK = 117;
    /**
     * RSA最大解密密文大小
     */
    private static final int MAX_DECRYPT_BLOCK = 128;

    /**
     * RSA 位数 如果采用2048 上面最大加密和最大解密则须填写:  245 256
     */
    private static final int INITIALIZE_LENGTH = 1024;

    /**
     * RSA密钥长度必须是64的倍数，在512~65536之间。默认是1024
     */
    public static final int KEY_SIZE = 2048;
    /**
     * 获取公钥的key
     */
    private static final String PUBLIC_KEY = "RSAPublicKey";

    /** */
    /**
     * 获取私钥的key
     */
    private static final String PRIVATE_KEY = "RSAPrivateKey";

    //生成秘钥对
    public static Map<String, Object> getKeyPair() throws Exception {
        KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance(SIGN_TYPE_RSA);
        keyPairGenerator.initialize(INITIALIZE_LENGTH);
        KeyPair keyPair = keyPairGenerator.generateKeyPair();
        String publicKey = getPublicKey(keyPair);
        String privateKey = getPrivateKey(keyPair);
        Map<String, Object> keyMap = new HashMap<String, Object>(2);
        keyMap.put(PUBLIC_KEY, publicKey);
        keyMap.put(PRIVATE_KEY, privateKey);
        return keyMap;
    }

    //获取公钥(Base64编码)
    public static String getPublicKey(KeyPair keyPair) {
        PublicKey publicKey = keyPair.getPublic();
        byte[] bytes = publicKey.getEncoded();
        return byte2Base64(bytes);
    }

    //获取私钥(Base64编码)
    public static String getPrivateKey(KeyPair keyPair) {
        PrivateKey privateKey = keyPair.getPrivate();
        byte[] bytes = privateKey.getEncoded();
        return byte2Base64(bytes);
    }

    //字节数组转Base64编码
    public static String byte2Base64(byte[] bytes) {
        //BASE64Encoder encoder = new BASE64Encoder();

        return new String(Base64.encodeBase64(bytes));
    }





    //将Base64编码后的公钥转换成PublicKey对象
    public static PublicKey string2PublicKey(String pubStr) throws Exception {
        byte[] keyBytes = base642Byte(pubStr);
        X509EncodedKeySpec keySpec = new X509EncodedKeySpec(keyBytes);
        KeyFactory keyFactory = KeyFactory.getInstance(SIGN_TYPE_RSA);
        PublicKey publicKey = keyFactory.generatePublic(keySpec);
        return publicKey;
    }

    //将Base64编码后的私钥转换成PrivateKey对象
    public static PrivateKey string2PrivateKey(String priStr) throws Exception {
        byte[] keyBytes = base642Byte(priStr);
        PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(keyBytes);
        KeyFactory keyFactory = KeyFactory.getInstance(SIGN_TYPE_RSA);
        PrivateKey privateKey = keyFactory.generatePrivate(keySpec);
        return privateKey;
    }

    //Base64编码转字节数组
    public static byte[] base642Byte(String base64Key) throws IOException {
        //BASE64Decoder decoder = new BASE64Decoder();
        return Base64.decodeBase64(base64Key);
    }


    //公钥加密
    public static String publicEncrypt(byte[] content, PublicKey publicKey) throws Exception {
        String encode = "";
        Cipher cipher = Cipher.getInstance(SIGN_TYPE_RSA);
        cipher.init(Cipher.ENCRYPT_MODE, publicKey);
        int inputLength = content.length;
        System.out.println("加密字节数：" + inputLength);
        // 标识
        int offSet = 0;
        byte[] resultBytes = {};
        byte[] cache = {};
        while (inputLength - offSet > 0) {
            if (inputLength - offSet > MAX_ENCRYPT_BLOCK) {
                cache = cipher.doFinal(content, offSet, MAX_ENCRYPT_BLOCK);
                offSet += MAX_ENCRYPT_BLOCK;
            } else {
                cache = cipher.doFinal(content, offSet, inputLength - offSet);
                offSet = inputLength;
            }
            resultBytes = Arrays.copyOf(resultBytes, resultBytes.length + cache.length);
            System.arraycopy(cache, 0, resultBytes, resultBytes.length - cache.length, cache.length);
        }
        //BASE64Encoder encoder = new BASE64Encoder();
        //encode = encoder.encode(resultBytes).trim().replaceAll("\\r\\n","");

        return encode;
    }

    //私钥加密
    public static String privateEncrypt(byte[] content, PrivateKey privateKey) throws Exception {
        String encode = "";
        Cipher cipher = Cipher.getInstance(SIGN_TYPE_RSA);
        cipher.init(Cipher.ENCRYPT_MODE, privateKey);
        int inputLength = content.length;
        System.out.println("加密字节数：" + inputLength);
        // 标识
        int offSet = 0;
        byte[] resultBytes = {};
        byte[] cache = {};
        while (inputLength - offSet > 0) {
            if (inputLength - offSet > MAX_ENCRYPT_BLOCK) {
                cache = cipher.doFinal(content, offSet, MAX_ENCRYPT_BLOCK);
                offSet += MAX_ENCRYPT_BLOCK;
            } else {
                cache = cipher.doFinal(content, offSet, inputLength - offSet);
                offSet = inputLength;
            }
            resultBytes = Arrays.copyOf(resultBytes, resultBytes.length + cache.length);
            System.arraycopy(cache, 0, resultBytes, resultBytes.length - cache.length, cache.length);
        }
        //BASE64Encoder encoder = new BASE64Encoder();
        //encode = encoder.encode(resultBytes).trim().replaceAll("\\r\\n","");
        encode = new String(Base64.encodeBase64(resultBytes)).trim().replaceAll("\\r\\n","");
        return encode;
    }

    //私钥解密
    public static String privateDecrypt(byte[] content, PrivateKey privateKey) throws Exception {
        Cipher cipher = Cipher.getInstance(SIGN_TYPE_RSA);
        cipher.init(Cipher.DECRYPT_MODE, privateKey);
        int inputLength = content.length;
        System.out.println("解密字节数：" + inputLength);
        // 返回UTF-8编码的解密信息
        int inputLen = content.length;
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        int offSet = 0;
        byte[] cache;
        int i = 0;
        // 对数据分段解密
        while (inputLen - offSet > 0)
        {
            if (inputLen - offSet > MAX_DECRYPT_BLOCK)
            {
                cache = cipher.doFinal(content, offSet, MAX_DECRYPT_BLOCK);
            } else
            {
                cache = cipher.doFinal(content, offSet, inputLen - offSet);
            }
            out.write(cache, 0, cache.length);
            i++;
            offSet = i * MAX_DECRYPT_BLOCK;
        }
        byte[] decryptedData = out.toByteArray();
        out.close();
        return new String(decryptedData, "UTF-8");
    }
    public static void main(String[] args) throws Exception {
        //生成秘钥对
       /* Map<String, Object> keyPair = RSAUtil.getKeyPair();
        String publicKeyCode = (String) keyPair.get("RSAPublicKey");
        String privateKeyCode = (String) keyPair.get("RSAPrivateKey");
        System.out.println("RSAPublicKey=" + publicKeyCode);
        System.out.println("RSAPrivateKey=" + privateKeyCode);*/
     /*   Map<String, Object> stringObjectMap = getKeyPair();
        for (Map.Entry<String, Object> entry : stringObjectMap.entrySet()) {
            System.out.println(entry.getKey() + "=========" + entry.getValue());
        }*/
        String publicKeyStr = "MIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQCfZXqtG6wxTsrPq8XzS38ygzs3fnaEJ5M8ZkqU" +
                "XEl3dZgLSLj9U32NJW4wKqGHYHJ7gQKXF8wcuvQPdgmoJlo7pjPqavMukDyafUkAQhjqAgA6MjSi" +
                "aqQBa4unbxzye742JjxWvIP+57gcDHlBRrDNvpGDdu47AocsWYPG06W0mwIDAQAB";
        String privateKeyStr = "MIICdwIBADANBgkqhkiG9w0BAQEFAASCAmEwggJdAgEAAoGBAJ9leq0brDFOys+rxfNLfzKDOzd+\n" +
                "doQnkzxmSpRcSXd1mAtIuP1TfY0lbjAqoYdgcnuBApcXzBy69A92CagmWjumM+pq8y6QPJp9SQBC\n" +
                "GOoCADoyNKJqpAFri6dvHPJ7vjYmPFa8g/7nuBwMeUFGsM2+kYN27jsChyxZg8bTpbSbAgMBAAEC\n" +
                "gYAxM9wIb5BZsB6+uCFklptu9j9jQ/BFnwm+DT1cSpiK/YuvgAVKfWk2FqXKzH4MEeOE6C/qf/gL\n" +
                "rlIEK8WTWDNl1ZMTBqfgbk0IbIaCqSj8WF+DNSe3dBVrOACoD8AodtE7UOokHqxQeUzUA/S05S+d\n" +
                "TCGcv712n/mLflzzA8JjIQJBAMuTkqX9T3zsovZIa8Ict0vGVzKMH95e9M54FpR51v0UjQcWIuzW\n" +
                "9beRka0+2nZV58K1KZ6wa7fOX3mpirSldNkCQQDIcWmXHVB1D/OPN0H4vRV2eYTKUhJ7gRDMADQk\n" +
                "aPHZZi9vNVL0zHSAvjnHQ5pyDCf3rFqjMPK7RMP3gr/rzvyTAkBrWzKN6Kz/g0dZO83f+wbKphkb\n" +
                "5ft0aH0PWRMHT82Jf0nz/7+BSMch/FlnlGre1uS2sJT3Q7A6qVF+NmOYalzZAkEAm63iWdkNxW33\n" +
                "OHOtjJinU9Y7+bvW2Q+8UQWTeff8Z2KDUvdyj+lAT0HvtEFgclXYsPevifIZhLN7FQD7ORYRewJB\n" +
                "AMkKXX5Ku+LhddvNpF3iSeApRJ7zoIRHS0hn9likrfDEw8G359ZgJ/88BNOHtIsGDQJcjogO0Tfk\n" +
                "F7GwGrCrKiM=";
        String content01c ="{\n" +
                "             \"request_fileds\":{\n" +
                "                \"engine_name\":\"Impala 服务\"\n" +
                "            },\n" +
                "            \"page_num\":\"-1\"\n" +
                "        }";
        content01c=content01c.trim().replaceAll("\\r\\n","");
        String content = "{\"request_fileds\":{\"engine_name\":\"Impala 服务\"},\"page_num\":\"-1\"}";        //将Base64编码后的公钥转换成PublicKey对象
        PublicKey publicKey = RSAUtil.string2PublicKey(publicKeyStr);

        String test = "MIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQCfZXqtG6wxTsrPq8XzS38ygzs3fnaEJ5M8ZkqU\n" +
                "XEl3dZgLSLj9U32NJW4wKqGHYHJ7gQKXF8wcuvQPdgmoJlo7pjPqavMukDyafUkAQhjqAgA6MjSi\n" +
                "aqQBa4unbxzye742JjxWvIP 57gcDHlBRrDNvpGDdu47AocsWYPG06W0mwIDAQAB";

        //用公钥加密
        String byte2Base64 = publicEncrypt(content.getBytes(),publicKey);
        //RSAUtil.publicEncrypt(content.getBytes(), publicKey);
        //加密后的内容Base64编码
        //String byte2Base64 = RSAUtil.byte2Base64(publicEncrypt);
        System.out.println("公钥加密并Base64编码的结果：" + byte2Base64);

        //===================服务端解密================
        //将Base64编码后的私钥转换成PrivateKey对象
        PrivateKey privateKey = RSAUtil.string2PrivateKey(privateKeyStr);
        //加密后的内容Base64解码
        String str = "dkw4fBy+nv+yBDidhrn3/WnInp7VTuThmC9AOHRlf8aMSY1kpUhylJrVf1JoGr4KAr8UtR0dZmRML5ngLiUUESNT5VJCfWjP2sTUUpTmQVMGNMF/VYTh9SXa0M5q90tJvcYPnkZamL1pj/LSb/7cMHnO59ELTtf1cI0ro/1mtcM=";

        byte[] base642Byte = RSAUtil.base642Byte(str);

        //用私钥解密
        String privateDecrypt = RSAUtil.privateDecrypt(base642Byte, privateKey);
        //解密后的明文
        System.out.println("解密后的明文: " + privateDecrypt);



    }
}
