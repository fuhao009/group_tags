import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class test {
    public static void main(String[] args) {
        System.out.println("start");
        try {
            Process pr = Runtime.getRuntime().exec("python D:\\test_tags\\src\\main\\java\\test.py");
            System.out.println();
            BufferedReader in = new BufferedReader(new
                    InputStreamReader(pr.getInputStream()));
            String line;
            while ((line = in.readLine()) != null) {
                System.out.println(line);
            }
            in.close();
            System.out.println("end");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}