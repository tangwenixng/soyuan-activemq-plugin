/**
 * @author tangwx@soyuan.com.cn
 * @date 2021/7/1 下午2:48
 */
public class CommonTest {

    public static void main(String[] args) {
        try {
            Object a = new Object();

            String b = (String) a;

            System.out.println(b);
        } catch (Exception e) {
            System.out.println("error");

        }
    }
}
