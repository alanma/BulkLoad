package szhao;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class MessageRecord implements WritableComparable<MessageRecord> {
	private String eventType;
	private long eventID;
	private int payloadVersion;
	private long creationDate;
	private int expirationSecond;
	private long refID;
	private String payLoad0;
	private String payLoad1;
	private String payLoad2;
	private String payLoad3;
	private String producer;
	private String properties;
	private String utf8Status;
	private String parititionKey;
	
	
	public String getEventType() {
		return eventType;
	}
	public void setEventType(String eventType) {
		this.eventType = eventType;
	}
	public long getEventID() {
		return eventID;
	}
	public void setEventID(long eventID) {
		this.eventID = eventID;
	}
	public int getPayloadVersion() {
		return payloadVersion;
	}
	public void setPayloadVersion(int payloadVersion) {
		this.payloadVersion = payloadVersion;
	}
	public long getCreationDate() {
		return creationDate;
	}
	public void setCreationDate(long creationDate) {
		this.creationDate = creationDate;
	}
	public int getExpirationSecond() {
		return expirationSecond;
	}
	public void setExpirationSecond(int expirationSecond) {
		this.expirationSecond = expirationSecond;
	}
	public long getRefID() {
		return refID;
	}
	public void setRefID(long refID) {
		this.refID = refID;
	}
	public String getPayLoad0() {
		return payLoad0;
	}
	public void setPayLoad0(String payLoad0) {
		this.payLoad0 = payLoad0;
	}
	public String getPayLoad1() {
		return payLoad1;
	}
	public void setPayLoad1(String payLoad1) {
		this.payLoad1 = payLoad1;
	}
	public String getPayLoad2() {
		return payLoad2;
	}
	public void setPayLoad2(String payLoad2) {
		this.payLoad2 = payLoad2;
	}
	public String getPayLoad3() {
		return payLoad3;
	}
	public void setPayLoad3(String payLoad3) {
		this.payLoad3 = payLoad3;
	}
	public String getProducer() {
		return producer;
	}
	public void setProducer(String producer) {
		this.producer = producer;
	}
	public String getProperties() {
		return properties;
	}
	public void setProperties(String properties) {
		this.properties = properties;
	}
	public String getUtf8Status() {
		return utf8Status;
	}
	public void setUtf8Status(String utf8Status) {
		this.utf8Status = utf8Status;
	}
	public String getParititionKey() {
		return parititionKey;
	}
	public void setParititionKey(String paritition_key) {
		this.parititionKey = paritition_key;
	}
	
	public void clear() {
		eventID = 0;
		payloadVersion = 0;
		creationDate = 0;
		expirationSecond = 0;
		refID = 0;
		eventType = null;
		payLoad0 = null;
		payLoad1 = null;
		payLoad2 = null;
		payLoad3 = null;
		producer = null;
		properties = null;
		utf8Status = null;
		parititionKey = null;
	}
	
	public String toString() {
		StringBuilder sbu = new StringBuilder();
		Date dt = new Date();
		dt.setTime(creationDate);
		String outputPattern = "yyyy-MM-dd HH:mm:ss";
		SimpleDateFormat df = new SimpleDateFormat(outputPattern);
		
		sbu.append("eventID: ["+eventID+"]\n");
		sbu.append("payloadVersion: ["+payloadVersion+"]\n");
		sbu.append("creationDate: ["+df.format(dt)+"]\n");
		sbu.append("expirationSecond: ["+expirationSecond+"]\n");
		sbu.append("refID: ["+refID+"]\n");
		sbu.append("eventType: ["+eventType+"]\n");
		sbu.append("payLoad0: ["+payLoad0+"]\n");
		return sbu.toString();
	}
	@Override
	public void readFields(DataInput i) throws IOException {
		// TODO Auto-generated method stub
		eventType = WritableUtils.readString(i);
		eventID = WritableUtils.readVLong(i);
		payloadVersion = WritableUtils.readVInt(i);
		creationDate = WritableUtils.readVLong(i);
		expirationSecond = WritableUtils.readVInt(i);
		refID = WritableUtils.readVLong(i);
		payLoad0 = WritableUtils.readString(i);
		payLoad1 = WritableUtils.readString(i);
		payLoad2 = WritableUtils.readString(i);
		payLoad3 = WritableUtils.readString(i);
		producer = WritableUtils.readString(i);
		properties = WritableUtils.readString(i);
		utf8Status = WritableUtils.readString(i);
		parititionKey = WritableUtils.readString(i);
		
		
	}
	@Override
	public void write(DataOutput o) throws IOException {
		// TODO Auto-generated method stub
		WritableUtils.writeString(o, eventType);
		WritableUtils.writeVLong(o, eventID);
		WritableUtils.writeVInt(o, payloadVersion);
		WritableUtils.writeVLong(o, creationDate);
		WritableUtils.writeVInt(o, expirationSecond);
		WritableUtils.writeVLong(o, refID);
		WritableUtils.writeString(o, payLoad0);
		WritableUtils.writeString(o, payLoad1);
		WritableUtils.writeString(o, payLoad2);
		WritableUtils.writeString(o, payLoad3);
		WritableUtils.writeString(o, producer);
		WritableUtils.writeString(o, properties);
		WritableUtils.writeString(o, utf8Status);
		WritableUtils.writeString(o, parititionKey);
	}

	@Override
	public int compareTo(MessageRecord o) {
		// TODO Auto-generated method stub
		return 0;
	}
}
