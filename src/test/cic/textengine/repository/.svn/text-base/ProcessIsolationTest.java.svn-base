package test.cic.textengine.repository;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.channels.FileChannel;

import org.junit.Test;

public class ProcessIsolationTest {

	@Test
	public void testMoveSourceToDest()
	{
		int numOfFiles = 1000;
		
		String sourcepath = "/home/joe.sun/develop/workspace/Test/xml/Source";
		String destpath = "/home/joe.sun/develop/workspace/Test/xml/Dest";
		
		File sourceDir = new File(sourcepath);
//		File destDir = new File(destpath);
		
		File[] files = sourceDir.listFiles(new FilenameFilter() {
			public boolean accept(File dir, String name) {
				if (name.toLowerCase().endsWith(".xml")) {
					return true;
				} else {
					return false;
				}
			}
		});
		
		for(int i=0; i<numOfFiles; i++)
		{
			File src = null;
			File dest = null;
			src = files[i];
			dest = new File(destpath+File.separator+src.getName());
			try {
				moveSourceToDest(src, dest);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	private void moveSourceToDest(File src, File dest) throws IOException {
		// Create channel on the source
		FileChannel srcChannel = new FileInputStream(src).getChannel();

		// Create channel on the destination
		FileChannel dstChannel = new FileOutputStream(dest).getChannel();

		// Copy file contents from source to destination
		dstChannel.transferFrom(srcChannel, 0, srcChannel.size());

		// Close the channels
		srcChannel.close();
		dstChannel.close();
		
		// Remove the source file
//		src.delete();
		
		System.out.println(String.format("File %s moved to %s", src, dest));
	}
}
