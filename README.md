![](https://img.shields.io/badge/Code-Python-informational?style=flat&logo=python&color=ffe333&logoColor=ffffff)
![](https://img.shields.io/badge/Database-PostgreSQL-informational?style=flat&logo=postgresql&color=4169E1&logoColor=ffffff)
![](https://img.shields.io/badge/Cloud-Azure-informational?style=flat)

# Azure Function for LogicMonitor Collector HTTPS Publisher
This Azure function is an HTTP Trigger that receives JSON data from the Collector HTTPS Data Publisher in LogicMonitor and stores it in a Azure PostgreSQL Database.

## Prerequisites

- LogicMonitor Collector Publisher within Collector Configuration. For more information see the [LogicMonitor Collector HTTPS Publisher Documentation](https://www.logicmonitor.com/support/logicmonitor-data-publisher-with-https-client)
- Azure Account: Active Azure subscription
- Function App: Deploy Function App within Azure to deploy HTTP Trigger Function
- Visual Studio Code / Visual Stuido to deploy Function to Function App [Develop Azure Functions by using Visual Studio Code](https://learn.microsoft.com/en-us/azure/azure-functions/functions-develop-vs-code?tabs=node-v4%2Cpython-v2%2Cisolated-process%2Cquick-create&pivots=programming-language-python)

## function_app.py
The python code for this Function accounts for the HTTPS Publisher’s default headers. In this version, we explicitly check for the gzip encoding (sent via Content-Encoding) and decompress the payload before parsing the JSON. 
- This code assumes that the publisher is sending the data with:
    - ```Content-Type: application/json```
    - ```Content-Encoding: gzip```
    - ```Accept-Encoding: gzip,deflate```

## Create Database in Azure
This example is using Azure PostgreSQL Flexible Server in Azure.  For more information see [Azure Database for PosgreSQL flexible server](https://learn.microsoft.com/en-us/azure/postgresql/flexible-server/service-overview)

Once set up with Azure PostgreSQL Flexible Server, create databse table
- Example:
```
CREATE TABLE json_data (
    id SERIAL PRIMARY KEY,
    data JSONB NOT NULL
);
```
## Adjust POSTGRES_CONN_STR
- Within ``` local.settings.json ``` add your Postgres connection string as an environment variable
- Example:
```
{
  "IsEncrypted": false,
  "Values": {
    "AzureWebJobsStorage": "<your_storage_connection_string>",
    "FUNCTIONS_WORKER_RUNTIME": "python",
    "POSTGRES_CONN_STR": "host=your_postgres_host dbname=your_db user=your_user password=your_password"
  }
}
```
## Deploying to Azure via VS Code
- Sign In to Azure in VS Code:  Open the Azure sidebar in VS Code and sign in with your Azure account.
- Create a Function App in Azure: Right-click on ```Azure Functions``` in the sidebar and select ```Create Function App in Azure...```.
- Follow the prompts (subscription, unique function app name, region, runtime settings) to create your Function App.
- Deploy Your Project:  Once the Function App is created, right-click your project folder (or use the Command Palette) and select ```Azure Functions: Deploy to Function App...```.
- Choose the target Function App you just created.
- Update Application Settings:  In the Azure portal, navigate to your Function App’s Configuration settings and add the same ```POSTGRES_CONN_STR``` environment variable (with its connection string) that you used locally.

## LogicMonitor Collector Configuration
- Within the ```Collector-Publisher config``` in the LogicMonitor Collector configuration add the ```default (Function key)``` to the ```publisher.http.url=```
- Save and Restart the collector
- To add more Agent Configuration parameters please see the [LogicMonitor Collector HTTPS Publisher Documentation](https://www.logicmonitor.com/support/logicmonitor-data-publisher-with-https-client)

## Making Data Queryable by Power BI
Since the data is stored in your Azure PostgreSQL – Flexible Server, you can query it directly from Power BI by following these steps
- Install the PostgreSQL Connector:  In Power BI Desktop, ensure that you have the PostgreSQL connector installed.
- Connect to PostgreSQL:
    - Open Power BI Desktop and select Get Data > PostgreSQL Database.
    - Enter your PostgreSQL server’s hostname, database name, and credentials.
    - (Make sure your PostgreSQL firewall and network settings allow connections from your Power BI client.)
- Import or DirectQuery:  Choose whether to import the data or use DirectQuery depending on your reporting needs.
- Build Reports:  Once connected, Power BI will list the tables (including table name, example: ```json_data```).
- For more information please see [Import Data from Azure Database for PostgreSQL flexible server in Power BI](https://learn.microsoft.com/en-us/azure/postgresql/flexible-server/connect-with-power-bi-desktop)
