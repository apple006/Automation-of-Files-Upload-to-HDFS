package utd.bigdata;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
//import java.nio.file.FileSystem;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;

//import org.apache.hadoop.util.*;
//import org.apache.commons.io.IOUtils;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class HadoopUpload {
	public static void transferFiles(String localSrc, String filename, String dst) throws Exception{
		FileSystem hdfs =FileSystem.get(new Configuration());
		Path localFilePath = new Path(localSrc);
		Path hdfsFilePath=new Path(dst+filename);
		//Path hdfsFilePath=new Path("/assignment/"+filename);
		hdfs.copyFromLocalFile(localFilePath, hdfsFilePath);
		decompressFiles(filename,dst);
	}	
	 public static void copy(InputStream input, OutputStream output, int bufferSize) throws IOException {
		    byte[] buf = new byte[bufferSize];
		    int n = input.read(buf);
		    while (n >= 0) {
		      output.write(buf, 0, n);
		      n = input.read(buf);
		    }
		    output.flush();
	}
	 
	public static void decompressFiles(String filename, String dt)throws Exception{
		String uri = dt + filename;
		//String uri = "/assignment/"+filename;
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
		Path inputPath = new Path(uri);
		CompressionCodecFactory factory = new CompressionCodecFactory(conf);
		CompressionCodec codec = factory.getCodec(inputPath);
		if (codec == null) {
			System.err.println("No codec found for " + uri);
			System.exit(1);
		}
		String outputUri =
		CompressionCodecFactory.removeSuffix(uri, codec.getDefaultExtension());
		InputStream in = null;
		OutputStream out = null;
		try {
			in = codec.createInputStream(fs.open(inputPath));
			out = fs.create(new Path(outputUri));
			IOUtils.copyBytes(in, out, conf);
		} finally {
			IOUtils.closeStream(in);
			IOUtils.closeStream(out);
		}
		fs.delete(inputPath, true);
	}
	
	/*public static void deleteCompressedFiles(){
		File folder = new File("/assignment/");
		File fList[] = folder.listFiles();
		// Searchs .bz2
		for (int i = 0; i < fList.length; i++) {
		    File pes = fList[i];
		    if (pes.getName().endsWith(".bz2")) {
		        pes.delete();
		    }
		}
		}*/
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		List<String> urlList = new ArrayList<String>();
		urlList.add("http://www.utdallas.edu/~axn112530/cs6350/lab2/input/20417.txt.bz2");
		urlList.add("http://www.utdallas.edu/~axn112530/cs6350/lab2/input/5000-8.txt.bz2");
		urlList.add("http://www.utdallas.edu/~axn112530/cs6350/lab2/input/132.txt.bz2");
		urlList.add("http://www.utdallas.edu/~axn112530/cs6350/lab2/input/1661-8.txt.bz2");
		urlList.add("http://www.utdallas.edu/~axn112530/cs6350/lab2/input/972.txt.bz2");
		urlList.add("http://www.utdallas.edu/~axn112530/cs6350/lab2/input/19699.txt.bz2");
		String src = args[1];
		String dst = args[2];
		try{	
		for(String file : urlList){
			URL url = new URL(file);
			String filename = file.substring(file.indexOf("t/")+2);
			//System.out.println(filename);
			HttpURLConnection connection = (HttpURLConnection) url.openConnection();
			connection.setRequestMethod("GET");
			connection.addRequestProperty("User-Agent", 
					"Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.0)");
			InputStream in = connection.getInputStream();
			//String localSrc= "/home/012/g/gx/gxs161530/hw1/"+filename;
			String localSrc= src+filename;
			//String localSrc= "D:/"+filename;
			FileOutputStream out = new FileOutputStream(localSrc);
			//FileOutputStream out = new FileOutputStream("D:/"+filename);
			copy(in, out, 1024);
			out.close();
			transferFiles(localSrc,filename,dst);
			}
	//	deleteCompressedFiles();
		}catch(Exception ex){
			ex.printStackTrace();
		}
	}
}
