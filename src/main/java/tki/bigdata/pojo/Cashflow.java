package tki.bigdata.pojo;

import java.util.Date;

import lombok.Data;

@Data
public class Cashflow {
	private String date;
	private float amount;
	private int contractId;
    private Contract contract;
    
    
	public int getContractId() {
		return contractId;
	}
	public void setContractId(int contractId) {
		this.contractId = contractId;
	}
	public String getDate() {
		return date;
	}
	public void setDate(String date) {
		this.date = date;
	}
	public float getAmount() {
		return amount;
	}
	public void setAmount(float amount) {
		this.amount = amount;
	}
	public Contract getContract() {
		return contract;
	}
	public void setContract(Contract contract) {
		this.contract = contract;
	}
}
