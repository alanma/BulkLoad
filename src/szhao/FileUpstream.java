package szhao;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.regex.Pattern;

public class FileUpstream implements Upstream {


	/**
	 * @param args
	 */
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		FileUpstream fu = new FileUpstream("/home/training/download.csv");
		MessageRecord mr = new MessageRecord();
		while (fu.getNextRecord(mr)) {
			System.out.println(mr.toString());
		}
	}

	public FileUpstream(String filename) throws IOException {
		this.filename = filename;
		if (!initialize()) {
			throw new IOException("Failed to initialize!");
		}
		df = new SimpleDateFormat(creationDateFormatPattern);
	}
	
	private boolean initialize() throws IOException {
		if (filename == null || filename.isEmpty()) {
			throw new IOException("Filename is incorrect!");
		}
		input = new BufferedReader(new FileReader(filename));
		return (input != null)? true: false;
	}
	
	public String dump() throws IOException {
		StringBuilder sbu = new StringBuilder();
		String str = null;
		while ((str = input.readLine()) != null) {
			sbu.append(str);
			sbu.append("\n");
		}
		
		return sbu.toString();
	}
	
	private String filename;
	private BufferedReader input;
	private SimpleDateFormat df;
	private String creationDateFormatPattern = "yyyy-MM-dd HH:mm:ss";

	@Override
	public boolean getNextRecord(MessageRecord record) {
		// TODO Auto-generated method stub
		try {
			String str = input.readLine();
			if (str == null) {
				return false;
			}
			if (parseRecord(record, str)) {
				return true;
			} else {
				return false;
			}
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
		
	}
	
	private boolean parseRecord(MessageRecord rc, String line) {
		rc.clear();
		String[] cols = line.split(Pattern.quote("\",\""), -1);
		if (cols.length != 14) 
			return false;
		try {
			rc.setEventType(cols[0].replace("\"", ""));
			rc.setEventID(Long.parseLong(cols[1]));
			rc.setPayloadVersion(Integer.parseInt(cols[2]));
			rc.setCreationDate(df.parse(cols[3]).getTime());
			rc.setExpirationSecond(Integer.parseInt(cols[4]));
			rc.setRefID(Long.parseLong(cols[5]));
			rc.setPayLoad0(cols[6]);
			rc.setPayLoad1(cols[7]);
			rc.setPayLoad2(cols[8]);
			rc.setPayLoad3(cols[9]);
			rc.setProducer(cols[10]);
			rc.setProperties(cols[11]);
			rc.setUtf8Status(cols[12]);
			rc.setParititionKey(cols[13]);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
		
		return true;
	}
}
