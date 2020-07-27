package hdfs;

import java.io.IOException;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

// 키보드에서 데이터를 입력받아 하둡 파일로 저장하기
public class WriteHadoopFIle {
	public static void main(String[] args) throws IOException {
		Configuration conf = new Configuration();
		conf.addResource(new Path ("/usr/local/hadoop-2.9.2/etc/hadoop/core-site.xml"));
		conf.addResource(new Path ("/usr/local/hadoop-2.9.2/etc/hadoop/hdfs-site.xml"));
		conf.addResource(new Path ("/usr/local/hadoop-2.9.2/etc/hadoop/mapred-site.xml"));
		FileSystem hdfs = FileSystem.get(conf);
		Path path = new Path("consoledata");
		System.out.println("하둡 파일에 저장할 내용을 입력하세요.");
		Scanner sc = new Scanner(System.in);
		FSDataOutputStream out = hdfs.create(path); // hdfs.create(path) : 하둡 서버에 path의 파일을 생성하기
		while(true) {
			String console = sc.nextLine();
			if(console.equals("exit")) break;
			out.writeUTF(console + "\n");
		}
		out.flush(); out.close();
	}
}
