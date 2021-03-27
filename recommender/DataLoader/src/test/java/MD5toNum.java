import java.security.MessageDigest;

public class MD5toNum {
    public static void main(String[] args) {

    }
    public static String getMD5(byte[] source) {
        String s = null;
        char hexDigits[] = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9'};
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            md.update(source);
            byte tmp[] = md.digest();
            char str[] = new char[16];
            int k = 0;
            for (int i = 0; i < 16; i++) {
                byte byte0 = tmp[i];
                //只取高位
                str[k++] = hexDigits[(byte0 >>> 4 & 0xf) % 10];
                // str[k++] = hexDigits[byte0 & 0xf];
            }
            s = new String(str);  // 换后的结果转换为字符串
        } catch (Exception e) {
            e.printStackTrace();
        }
        return s;
    }
}
