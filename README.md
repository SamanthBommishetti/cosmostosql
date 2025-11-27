# Cosmos DB → Azure SQL One-Time Migration Function

This is a **one-time data migration Azure Function** that moves all documents from an Azure Cosmos DB container (`Products`) to Azure SQL Database, while correctly flattening the `tags` array into a separate child table.

Perfect for interviews, take-home assignments, or real production migrations.

## Features

- Reads every document from Cosmos DB (handles millions of records safely)  
- Flattens nested `tags` array → `ProductTags` table (one row per tag)  
- Uses proper pagination + batch inserts (no memory issues, no throttling)  
- Produces a clean migration report (total, failures, duration)  
- 100% local testing supported  
- Secrets stored safely in `local.settings.json` (never hard-coded)  
- Deployable to Azure in one click

## Folder Structure
Cosmossql/
├── MigrateProducts/
│   ├── init.py          ← Main migration logic (working version)
│   └── function.json        ← HTTP trigger
├── local.settings.json      ← Your secrets (gitignored!)
├── requirements.txt         ← Python dependencies
├── host.json
└── README.md                ← This file

## Prerequisites (One-time setup)

1. **Install ODBC Driver 18 for SQL Server**  
   → https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server  
   (Choose the .msi for Windows)

2. **Install Azure Functions Core Tools**  
   ```bash
   npm install -g azure-functions-core-tools@4 --unsafe-perm true
   python -m venv .venv
.\.venv\Scripts\activate
pip install -r requirements.txt

func start
