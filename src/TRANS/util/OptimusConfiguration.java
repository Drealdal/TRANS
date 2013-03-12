package TRANS.util;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.List;

import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.input.SAXBuilder;

import TRANS.Exceptions.WrongArgumentException;


public class OptimusConfiguration {
	java.util.TreeMap<String, String> conf = new java.util.TreeMap<String, String>();

	public OptimusConfiguration() {
		
	}

	public OptimusConfiguration(String confDir) throws WrongArgumentException,
			JDOMException, IOException {
		
		System.out.println(confDir);
		File dir = new File(confDir);

		if (dir == null || !dir.isDirectory()) {
			throw new WrongArgumentException("Configuration Directory",
					"Not directroy");
		}

		File[] fname = dir.listFiles(new FilenameFilter() {
			// ������ʵ�ֽӿ�FilenameFileter��Ψһ����
			public boolean accept(File dir, String name) {
				return name.indexOf(".xml") != -1;
			}
		});
		for (File file : fname) {
			SAXBuilder builder = new SAXBuilder();
			Document doc = builder.build(file.getAbsolutePath());
			Element root = doc.getRootElement();
			List<Element> elements = root.getChildren();
			for (Element e : elements) {
				Element key = e.getChild("name");
				Element value = e.getChild("value");
				this.conf.put(key.getText(), value.getText());
			}
		}

	}

	public int getInt(String key, int dvalue) {
		String value = conf.get(key);
		try {
			return Integer.parseInt(value);
		} catch (Exception e) {
			return dvalue;
		}
	}

	public String getString(String key, String defalutValue) {
		String value = conf.get(key);

		return value == null ? defalutValue : value;
	}


}
