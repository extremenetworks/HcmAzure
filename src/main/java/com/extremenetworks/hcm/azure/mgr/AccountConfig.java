package com.extremenetworks.hcm.azure.mgr;

public class AccountConfig {

    // Extreme Networks tenant / account IDs
    private String tenantId;
    private String accountId;

    // Customer's Azure account
    private String appId;
    private String key;
    private String subscription;
    private String azureTenantId;

    public AccountConfig() {
    }

    @Override
    public String toString() {
        return "App id: " + appId + ", tenant id: " + tenantId + " and subscription: " + subscription;
    }

    public String getTenantId() {
        return tenantId;
    }

    public void setTenantId(String tenantId) {
        this.tenantId = tenantId;
    }

    public String getAccountId() {
        return accountId;
    }

    public void setAccountId(String accountId) {
        this.accountId = accountId;
    }

    public String getAppId() {
        return appId;
    }

    public void setAppId(String appId) {
        this.appId = appId;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getSubscription() {
        return subscription;
    }

    public void setSubscription(String subscription) {
        this.subscription = subscription;
    }

    public String getAzureTenantId() {
        return azureTenantId;
    }

    public void setAzureTenantId(String azureTenantId) {
        this.azureTenantId = azureTenantId;
    }

}