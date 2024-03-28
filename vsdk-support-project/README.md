# Vault Java SDK Support Project

Ensure Support Team maintains compliance in Test Vaults.

## How to import

Import as a Maven project. This will automatically pull in the required Vault Java SDK dependencies. 

For Intellij this is done by:
- File -> Open -> Navigate to project folder -> Select the 'pom.xml' file -> Open as Project

For Eclipse this is done by:
- File -> Import -> Maven -> Existing Maven Projects -> Navigate to project folder -> Select the 'pom.xml' file

## Setup

Ensure all required components are configured in the Vault.  This includes connection, doc field on the base document, SDK queue, and job definition.

#### Deploying the vSDK VPK Package

1.  Log in to your vault and navigate to Admin > Deployment > Inbound Packages and click Import.
2.  Locate and select the vpk file on your computer. Vault opens and displays the details for the package.  
3.  From the Actions menu, select Review & Deploy. Vault displays a list of all components in the package.
4.  Click Next.   
5.  On the confirmation page, review and click Finish. You will receive an email when Vault completes the deployment.

  
