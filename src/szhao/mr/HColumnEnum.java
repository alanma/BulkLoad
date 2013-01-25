package szhao.mr;

public enum HColumnEnum {
	eventType ("eventType".getBytes()),
	eventID("eventID".getBytes()),
	payloadVersion ("payloadVersion".getBytes()),
	creationDate ("creationDate".getBytes()),
	expirationSecond ("expirationSecond".getBytes()),
	refID ("refID".getBytes()),
	payLoad ("payLoad".getBytes()),
	producer ("producer".getBytes()),
	properties ("properties".getBytes()),
	utf8Status ("utf8Status".getBytes()),
	parititionKey ("parititionKey".getBytes());
	
	private final byte[] columnName;
	
	HColumnEnum(byte[] column) {
		this.columnName = column;
	}
	
	public byte[] getColumnName() {
		return this.columnName;
	}
}
