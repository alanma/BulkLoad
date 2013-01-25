package szhao.mr;

import java.io.IOException;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import szhao.MessageRecord;

public class BulkLoaderMapper extends 
		Mapper<Text, MessageRecord, ImmutableBytesWritable, KeyValue> {
	final static byte[] COLUMN_FAM = "fam".getBytes();
	
	
	ImmutableBytesWritable hKey = new ImmutableBytesWritable();
	KeyValue kv;
	@Override
	protected void map(Text key, MessageRecord value,
			Context context)
			throws IOException, InterruptedException {
		hKey.set((value.getEventType() + value.getRefID()).getBytes());
		kv = new KeyValue(hKey.get(), COLUMN_FAM, HColumnEnum.creationDate.getColumnName(),Bytes.toBytes(value.getCreationDate()));
		context.write(hKey, kv);
		kv = new KeyValue(hKey.get(), COLUMN_FAM, HColumnEnum.eventID.getColumnName(),Bytes.toBytes(value.getEventID()));
		context.write(hKey, kv);
		kv = new KeyValue(hKey.get(), COLUMN_FAM, HColumnEnum.eventType.getColumnName(),Bytes.toBytes(value.getEventType()));
		context.write(hKey, kv);
		kv = new KeyValue(hKey.get(), COLUMN_FAM, HColumnEnum.expirationSecond.getColumnName(),Bytes.toBytes(value.getExpirationSecond()));
		context.write(hKey, kv);
		kv = new KeyValue(hKey.get(), COLUMN_FAM, HColumnEnum.parititionKey.getColumnName(),Bytes.toBytes(value.getParititionKey()));
		context.write(hKey, kv);
		kv = new KeyValue(hKey.get(), COLUMN_FAM, HColumnEnum.payLoad.getColumnName(),Bytes.toBytes(value.getPayLoad0() + value.getPayLoad1() + value.getPayLoad2() + value.getPayLoad3()));
		context.write(hKey, kv);
		kv = new KeyValue(hKey.get(), COLUMN_FAM, HColumnEnum.payloadVersion.getColumnName(),Bytes.toBytes(value.getPayloadVersion()));
		context.write(hKey, kv);
		kv = new KeyValue(hKey.get(), COLUMN_FAM, HColumnEnum.producer.getColumnName(),Bytes.toBytes(value.getProducer()));
		context.write(hKey, kv);
		kv = new KeyValue(hKey.get(), COLUMN_FAM, HColumnEnum.properties.getColumnName(),Bytes.toBytes(value.getProperties()));
		context.write(hKey, kv);
		kv = new KeyValue(hKey.get(), COLUMN_FAM, HColumnEnum.refID.getColumnName(),Bytes.toBytes(value.getRefID()));
		context.write(hKey, kv);
		kv = new KeyValue(hKey.get(), COLUMN_FAM, HColumnEnum.utf8Status.getColumnName(),Bytes.toBytes(value.getUtf8Status()));
		context.write(hKey, kv);
	}
	
	
}
