package com.hlbigdata.api;

/**
 * Created by sponge on 15-10-12.
 */
public class PhoneInfo {
    private String province;
    private String city;
    private String telephone;
    private String operator;

    public String getProvince() {
        return province;
    }

    public void setProvince(String province) {
        this.province = province;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getTelephone() {
        return telephone;
    }

    public void setTelephone(String telephone) {
        this.telephone = telephone;
    }

    public String getOperator() {
        return operator;
    }

    public void setOperator(String operator) {
        this.operator = operator;
    }

    @Override
    public String toString() {
        return "PhoneInfo{" +
                "province='" + province + '\'' +
                ", city='" + city + '\'' +
                ", telephone='" + telephone + '\'' +
                ", operator='" + operator + '\'' +
                '}';
    }
}
