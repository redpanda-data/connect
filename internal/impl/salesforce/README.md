# Salesforce

## Prerequisites

- **Install Salesforce CLI**: `brew install salesforce-cli`
- **Create account**: Sign up at [developer.salesforce.com/signup](https://developer.salesforce.com/signup). Salesforce generates a username automatically — **it is NOT your email**. Verify your email and set a password via the link received.
- **Log in**: `sf org login web` — opens a browser, log in and authorize.

## Connected App Setup

A **Connected App** must be created in your Salesforce org and configured to use the `client_credentials` OAuth grant. The app runs on behalf of a designated Run-As user and that user's permissions apply to all API calls.

**Deploy the Connected App**:
```bash
./app/setup.sh > .env
```

The script deploys the Connected App via the Metadata API, writes `SALESFORCE_ORG_URL` and `SALESFORCE_CLIENT_ID` to `.env`, and prints a deep-link to the app's Setup page on stderr:

```
Connected App: https://<org>.salesforce.com/lightning/setup/ConnectedApplication/page?address=%2F<id>
```

**Retrieve the Consumer Secret:**
1. On the app page click **Manage Consumer Details** (email verification required).
2. Copy the Consumer Secret.
3. Append to `.env`:
   ```
   SALESFORCE_CLIENT_SECRET="<secret>"
   ```

### 3. Verify

```bash
set -a; source .env; set +a
./app/setup.sh > /dev/null
```

When `SALESFORCE_CLIENT_SECRET` is set, `setup.sh` probes the `client_credentials` OAuth flow end-to-end. `OAuth: OK` on stderr means the integration tests (`internal/impl/salesforce/input_salesforce*_integration_test.go`) can run against this org. Any other response is printed verbatim and the script exits non-zero.
