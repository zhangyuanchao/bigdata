package com.mnemosyne.hadoop.mr.db;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;

public class HistoryBalance implements Writable, DBWritable {

    private Long id;

    // 账户名称
    private String accountName;

    // 账号
    private String accountNo;

    // 账户类型, 1:WXPAY, 2:ALIPAY, 3:CCB
    private int accountType;

    // 法人公司名称
    private String corporateCompanyName;

    // 法人公司id
    private Integer corporateCompanyId;

    // 使用公司名称列表, 以','分隔
    private String useCompanyName;

    // 使用公司id列表, 以','分隔
    private String useCompanyId;

    // 合作银行名称
    private String cooperateBankName;

    // 合作银行id
    private Integer cooperateBankId;

    // 开户行
    private String accountSubBankName;

    // 账户余额
    private BigDecimal balance;

    // 可用余额
    private BigDecimal availableBalance;

    // 冻结余额
    private BigDecimal frozenBalance;

    // 币种
    private Integer currency;

    // 日期
    private String balanceDate;

    // 更新时间
    private Date updateTime;

    // 创建时间
    private Date createTime;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getAccountName() {
        return accountName;
    }

    public void setAccountName(String accountName) {
        this.accountName = accountName;
    }

    public String getAccountNo() {
        return accountNo;
    }

    public void setAccountNo(String accountNo) {
        this.accountNo = accountNo;
    }

    public int getAccountType() {
        return accountType;
    }

    public void setAccountType(int accountType) {
        this.accountType = accountType;
    }

    public String getCorporateCompanyName() {
        return corporateCompanyName;
    }

    public void setCorporateCompanyName(String corporateCompanyName) {
        this.corporateCompanyName = corporateCompanyName;
    }

    public Integer getCorporateCompanyId() {
        return corporateCompanyId;
    }

    public void setCorporateCompanyId(Integer corporateCompanyId) {
        this.corporateCompanyId = corporateCompanyId;
    }

    public String getUseCompanyName() {
        return useCompanyName;
    }

    public void setUseCompanyName(String useCompanyName) {
        this.useCompanyName = useCompanyName;
    }

    public String getUseCompanyId() {
        return useCompanyId;
    }

    public void setUseCompanyId(String useCompanyId) {
        this.useCompanyId = useCompanyId;
    }

    public String getCooperateBankName() {
        return cooperateBankName;
    }

    public void setCooperateBankName(String cooperateBankName) {
        this.cooperateBankName = cooperateBankName;
    }

    public Integer getCooperateBankId() {
        return cooperateBankId;
    }

    public void setCooperateBankId(Integer cooperateBankId) {
        this.cooperateBankId = cooperateBankId;
    }

    public String getAccountSubBankName() {
        return accountSubBankName;
    }

    public void setAccountSubBankName(String accountSubBankName) {
        this.accountSubBankName = accountSubBankName;
    }

    public BigDecimal getBalance() {
        return balance;
    }

    public void setBalance(BigDecimal balance) {
        this.balance = balance;
    }

    public BigDecimal getAvailableBalance() {
        return availableBalance;
    }

    public void setAvailableBalance(BigDecimal availableBalance) {
        this.availableBalance = availableBalance;
    }

    public BigDecimal getFrozenBalance() {
        return frozenBalance;
    }

    public void setFrozenBalance(BigDecimal frozenBalance) {
        this.frozenBalance = frozenBalance;
    }

    public Integer getCurrency() {
        return currency;
    }

    public void setCurrency(Integer currency) {
        this.currency = currency;
    }

    public String getBalanceDate() {
        return balanceDate;
    }

    public void setBalanceDate(String balanceDate) {
        this.balanceDate = balanceDate;
    }

    public Date getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(Date updateTime) {
        this.updateTime = updateTime;
    }

    public Date getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(this.id);
        out.writeUTF(this.accountName);
        out.writeUTF(this.accountNo);
        out.writeInt(this.accountType);
        out.writeUTF(this.corporateCompanyName);
        out.writeInt(this.corporateCompanyId);
        out.writeUTF(this.useCompanyName);
        out.writeUTF(this.useCompanyId);
        out.writeUTF(this.cooperateBankName);
        out.writeInt(this.cooperateBankId);
        out.writeUTF(this.accountSubBankName);
        out.writeInt(this.currency);
        out.writeDouble(this.balance.doubleValue());
        out.writeDouble(this.availableBalance.doubleValue());
        out.writeDouble(this.frozenBalance.doubleValue());
        out.writeUTF(this.balanceDate);
        out.writeLong(this.updateTime.getTime());
        out.writeLong(this.createTime.getTime());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.id = in.readLong();
        this.accountName = in.readUTF();
        this.accountNo = in.readUTF();
        this.accountType = in.readInt();
        this.corporateCompanyName = in.readUTF();
        this.corporateCompanyId = in.readInt();
        this.useCompanyName = in.readUTF();
        this.useCompanyId = in.readUTF();
        this.cooperateBankName = in.readUTF();
        this.cooperateBankId = in.readInt();
        this.accountSubBankName = in.readUTF();
        this.currency = in.readInt();
        this.balance = new BigDecimal(in.readDouble());
        this.availableBalance = new BigDecimal(in.readDouble());
        this.frozenBalance = new BigDecimal(in.readDouble());
        this.balanceDate = in.readUTF();
        this.updateTime = new Date(in.readLong());
        this.createTime = new Date(in.readLong());
    }

    @Override
    public void write(PreparedStatement statement) throws SQLException {

    }

    @Override
    public void readFields(ResultSet resultSet) throws SQLException {
        this.id = resultSet.getLong(1);
        this.accountName = resultSet.getString(2);
        this.accountNo = resultSet.getString(3);
        this.accountType = resultSet.getInt(4);
        this.corporateCompanyName = resultSet.getString(5);
        this.corporateCompanyId = resultSet.getInt(6);
        this.useCompanyName = resultSet.getString(7);
        this.useCompanyId = resultSet.getString(8);
        this.cooperateBankName = resultSet.getString(9);
        this.cooperateBankId = resultSet.getInt(10);
        this.accountSubBankName = resultSet.getString(11);
        this.currency = resultSet.getInt(12);
        this.balance = resultSet.getBigDecimal(13);
        this.availableBalance = resultSet.getBigDecimal(14);
        this.frozenBalance = resultSet.getBigDecimal(15);
        this.balanceDate = resultSet.getString(16);
        this.updateTime = resultSet.getDate(17);
        this.createTime = resultSet.getDate(18);
    }

    @Override
    public String toString() {
        return id + "," + accountName + "," + accountNo + "," + accountType +
                ",'" + corporateCompanyName + "," + corporateCompanyId + ",'" + useCompanyName +
                ",'" + useCompanyId + ",'" + cooperateBankName + "," + cooperateBankId +
                "," + accountSubBankName + "," + balance + "," + availableBalance + "," + frozenBalance +
                "," + currency + ",'" + balanceDate + "," + updateTime + "," + createTime;
    }
}
