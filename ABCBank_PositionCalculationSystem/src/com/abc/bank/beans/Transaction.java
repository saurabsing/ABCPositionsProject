/*********************************************************************************************************
 * File Name      : Transaction.java
 * Project        : ABC Bank Position Calculation System
 * Description    : Bean class to hold values parsed from transactions positions file
 * 
 * Modification Log
 * Date                 Author                               Description
 * -------------------------------------------------------------------------
 * Sep 23, 2018          Saurabh Singh                         Created
 * -------------------------------------------------------------------------
 *******************************************************************************************************/

package com.abc.bank.beans;

public class Transaction {
	
	/** transaction id is not required to be stored	 */
	protected String instrumentName;
	protected String transactionType;	
	protected long transactionQuantity;
	
	
	public String getInstrumentName() {
		return instrumentName;
	}
	public void setInstrumentName(String instrumentName) {
		this.instrumentName = instrumentName;
	}
	public String getTransactionType() {
		return transactionType;
	}
	public void setTransactionType(String transactionType) {
		this.transactionType = transactionType;
	}
	public long getTransactionQuantity() {
		return transactionQuantity;
	}
	public void setTransactionQuantity(long transactionQuantity) {
		this.transactionQuantity = transactionQuantity;
	}
	
	@Override
	public String toString() {
		return "Transaction [instrumentName=" + instrumentName
				+ ", transactionType=" + transactionType
				+ ", transactionQuantity=" + transactionQuantity + "]";
	}
		
}
