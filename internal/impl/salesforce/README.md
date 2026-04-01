## Salesforce connector setup

A **Connected App** needs to be created in Salesforce Org to enable Redpanda connectivity in your Salesforce instance. A connected app must run on a **dedicated API user's** behalf for the `client_credentials` grant type to not require the user's username and password.

Then, the **Redpanda Connect** will have the dedicated API user's permissions for API interaction

You have to perform the following steps in your Salesforce instance:

- **Create dedicated API user**
    1. Go to Setup
    2. Go to Administration -> Users -> Users
    3. Click on New User
    4. Fill in the mandatory information (for email use another email than yours, the dedicated API user must have a **dedicated email address**)
    5. Set **User License** to **Salesforce Integration**
    6. Set **Profile** to **Minimum Access - API Only Integrations**
    7. Create user
    8. Set the API user password via the email received
- **Create Salesforce External Client App**
    1. Go to Setup
    2. Go to Platform Tools -> Apps -> External Client Apps -> External Client App Manager
    3. Click on **New External Client App**
    5. Fill in the mandatory information
    6. Check **Enable OAuth Settings**
    7. Check **Enable for Device Flow**
    8. Set **Callback URL** to **http://localhost**
    9. Add the following **OAuth Scopes**
        ```
        Manage user data via APIs (api)
        Full access (full)
        Access Connect REST API resources (chatter_api)
        Perform requests at any time (refresh_token, offline_access)
        Access Analytics REST API resources (wave_api)
        Access content resources (content)
        Access the Salesforce API Platform (sfap_api)
        Access Interaction API resources (interaction_api)
        Access all Data Cloud API resources (cdp_api)
        ```
    10. Check **Require Proof Key for Code Exchange (PKCE) Extension for Supported Authorization Flows**
    11. Check **Require Secret for Web Server Flow**
    12. Check **Require Secret for Refresh Token Flow**
    13. Check **Enable Client Credentials Flow**
    14. Check **Enable Authorization Code and Credentials Flow**
    15. Click **Save**
    16. Click **Continue**
- **Allow the connected app to run on behalf of dedicated API user**
    1. Go to Setup
    2. Go to Platform Tools -> Apps -> External Client App > External Client App  Manager
    3. At your created Connected App click on dropdown button and click on **Edit Policies**
    4. Set the **IP Relaxation** to **Relax IP restrictions for activated devices**
    5. Check Enable Client Credentials Flow
    6. In the **Client Credentials Flow** set the **Run As** to previously created dedicated user
    7. Click **Save**
- **Gather client_id and client_secret from the connected app**
    1. Go to Setup
    2. Go to Platform Tools -> Apps -> External Client App > External Client App  Manager
    3. At your created Client App click on dropdown button and click on **Edit Settings**
    4. Click on Consumer Key and Secret
    5. Do the required authorization
    6. The secrets will be available