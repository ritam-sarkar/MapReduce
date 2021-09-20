package test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.Random;

public class CreateRandomNumber {

   public static void main(String [] args) throws FileNotFoundException {
      PrintWriter out = new PrintWriter(new File("random.txt"));
      Random rand = new Random();
      int number, count=0;
      while(count!=99999999)
      {
         number=rand.nextInt(100)+1;
         out.print(number+" ");
         count++;
      }
      out.close();
   }
}
