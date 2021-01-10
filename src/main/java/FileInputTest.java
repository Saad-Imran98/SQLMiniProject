import java.io.File;
import java.io.FileNotFoundException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Scanner;

public class FileInputTest {

    public static void main(String[] args) {
        File file = new File(MyDbApp.DELAYEDFLIGHTSFILE);

        try{
            Scanner scanner = new Scanner(file);
            while(scanner.hasNext()) {
                String line = scanner.nextLine();
                System.out.println(line);
                System.out.println(line.split(",")[0]);
            }
        } catch(FileNotFoundException e){
            e.printStackTrace();
        }
    }
}
