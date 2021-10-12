package com.jz.api.com.jz.gateway.common.signtype.signutil;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * @author ZC
 * @PACKAGE_NAME: com.jz.gateway.common.signtype.signutil
 * @PROJECT_NAME: jz_dm_gateway
 * @NAME: RSAUtil
 * @DATE: 2021-4-15/16:05
 * @DAY_NAME_SHORT: 周四
 * @Description: RSA工具类
 **/
public class Sha256 {
    /**
     * SHA256签名算法
     */
    public static String getSignCommon(Map<String, Object> map, List<String> removeKeyList) {
        TreeMap<String, Object> params = new TreeMap<String, Object>();
        params.putAll(map);
        if (removeKeyList == null) {
            removeKeyList = new ArrayList<>();
        }
        StringBuilder sb = new StringBuilder();
        String resultStr = "";
        for (String key : params.keySet()) {
            if (!removeKeyList.contains(key)) {
                String format = null;
                format = String.format("%s=%s&", key, params.get(key));
                sb.append(format);
            }
        }
        StringBuilder delete = sb.delete(sb.length() - 1, sb.length());
        String string = delete.toString();
        resultStr = getSHA256(string);
        return resultStr;
    }

    public static String getSHA256(String str) {
        MessageDigest messageDigest;
        String encodestr = "";
        try {
            messageDigest = MessageDigest.getInstance("SHA-256");
            messageDigest.update(str.getBytes("UTF-8"));
            byte[] digest = messageDigest.digest();
            encodestr = MD5.byte2hex(digest);
        } catch (NoSuchAlgorithmException | UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return encodestr;
    }
}
