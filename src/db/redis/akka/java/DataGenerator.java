package db.redis.akka.java;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.math.BigInteger;
import java.security.SecureRandom;

public class DataGenerator {
	public static void main(String[] args) {

			DataGenerator gen = new DataGenerator();
			gen.generateCustomerData();
			gen.generateLogs();
			System.out.println("Done");
	}
	
	public void generateCustomerData() {
		try {
			File file = new File("customer_100000.txt");
			
			FileWriter fw = new FileWriter(file.getAbsolutePath());
			
			BufferedWriter bw = new BufferedWriter(fw);
			
			int temp, age;
			SecureRandom random = new SecureRandom();
			String name;
			String gender;
			
			for(int i = 1; i <= 100000; i++) {
				name = new BigInteger(100, random).toString(32);
				temp = (int)(Math.random() * 2);
				age = (int)(Math.random() * 40 + 20);
				
				if(temp == 0)
					gender = "M";
				else
					gender = "F";
					
				System.out.print(i + "," + name + "," + gender + "," + age + "\n");
				bw.write(i + "," + name + "," + gender + "," + age + "\n");
			}
			System.out.println(file.getAbsolutePath());
			bw.close();
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void generateLogs() {
		try {
			File file = new File("logs_1000000.txt");
			
			FileWriter fw = new FileWriter(file.getAbsolutePath());
			
			BufferedWriter bw = new BufferedWriter(fw);
			
			int cust_id;
			int total;
			for(int i = 1; i <= 1000000; i++) {
				cust_id = (int)(Math.random() * 99999 + 1);
				total = (int)(Math.random() * 200 + 10);
				
				System.out.print(cust_id + "," + (10000000 + i) + "," + total + "\n");
				bw.write(cust_id + "," + (10000000 + i) + "," + total + "\n");
			}
			System.out.println(file.getAbsolutePath());
			bw.close();
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
