package in.dream_lab.nifi.tf.processors;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.edgent.function.Supplier;

public class TempSource implements Supplier<Iterable<Pair<String,String>>>{

	private String sourceDir;

	public TempSource(String jsonDir) {
		this.sourceDir = jsonDir;
	}

	@Override
	public Iterable<Pair<String, String>> get() {
		List<Pair<String,String>> list = new ArrayList<Pair<String,String>>();
		File dir = new File(sourceDir);
		if(dir.isDirectory())
		{
			File[] files = dir.listFiles();
			for(File file:files) {
					byte[] bytes;
					try {
						bytes = Files.readAllBytes(file.toPath());
						String content =  new String(bytes,"UTF-8");
						Pair<String, String> e = Pair.of(file.getName(), content);
						list.add(e);
					} catch (IOException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
			}
		}
		return list;
	}

}
