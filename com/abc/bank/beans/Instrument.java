/*********************************************************************************************************
 * File Name      : Instrument.java
 * Project        : ABC Bank Position Calculation System
 * Description    : Bean class to hold values parsed from Daily positions file
 * 
 * Modification Log
 * Date                 Author                               Description
 * -------------------------------------------------------------------------
 * Sep 23, 2018          Saurabh Singh                         Created
 * -------------------------------------------------------------------------
 *******************************************************************************************************/

package com.abc.bank.beans;

public class Instrument {

	
	protected String instrumentName;
	protected long accountNumber;
	protected String accountType;	
	protected long quantity;
	protected long deltaQuantity;
	
	
	public String getInstrumentName() {
		return instrumentName;
	}
	public void setInstrumentName(String instrumentName) {
		this.instrumentName = instrumentName;
	}
	public long getAccountNumber() {
		return accountNumber;
	}
	public void setAccountNumber(long accountNumber) {
		this.accountNumber = accountNumber;
	}
	public String getAccountType() {
		return accountType;
	}
	public void setAccountType(String accountType) {
		this.accountType = accountType;
	}
	public long getQuantity() {
		return quantity;
	}
	public void setQuantity(long quantity) {
		this.quantity = quantity;
	}
	
	public long getDeltaQuantity() {
		return deltaQuantity;
	}
	public void setDeltaQuantity(long deltaQuantity) {
		this.deltaQuantity = deltaQuantity;
	}
	
	
	@Override
	public String toString() {
		return  instrumentName + "," + accountNumber + "," + accountType + "," + quantity + ","  + deltaQuantity ;
	}
	
	
}
