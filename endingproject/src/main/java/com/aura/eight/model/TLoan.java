package com.aura.eight.model;

import java.io.Serializable;

public class TLoan implements Serializable {
    private String uid;
    private String loan_time;
    private String loan_amount;
    private String plannum;

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    public String getLoan_time() {
        return loan_time;
    }

    public void setLoan_time(String loan_time) {
        this.loan_time = loan_time;
    }

    public String getLoan_amount() {
        return loan_amount;
    }

    public void setLoan_amount(String loan_amount) {
        this.loan_amount = loan_amount;
    }

    public String getPlannum() {
        return plannum;
    }

    public void setPlannum(String plannum) {
        this.plannum = plannum;
    }
}
