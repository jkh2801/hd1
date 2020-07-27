package hdfs;

import java.io.FileInputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

// 파일을 읽어서 하둡 파일로 저장하기
public class WriteHadoopFIle2 {
	public static void main(String[] args) throws IOException {
		Configuration conf = new Configuration();
		conf.addResource(new Path ("/usr/local/hadoop-2.9.2/etc/hadoop/core-site.xml"));
		conf.addResource(new Path ("/usr/local/hadoop-2.9.2/etc/hadoop/hdfs-site.xml"));
		conf.addResource(new Path ("/usr/local/hadoop-2.9.2/etc/hadoop/mapred-site.xml"));
		String file = "src/main/java/hdfs/DisplayHadoopFile.java";
		FileSystem hdfs = FileSystem.get(conf);
		FileInputStream fis  = new FileInputStream(file); // 내 컴퓨터의 파일 읽기
		int len = 0;
		byte [] buf = new byte [8096];
		Path path = new Path(file); // 하둡 서버에 저장할 파일 지정. path도 같이 설정된다.
		FSDataOutputStream out = hdfs.create(path); // out: 하둡서버에 출력될 파일 지정
		while((len = fis.read(buf)) != -1) {
			out.writeUTF(new String(buf, 0, len));
		}
		fis.close(); out.flush(); out.close();
	}
}
