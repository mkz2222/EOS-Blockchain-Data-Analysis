This data pipeline ETL project provides on-chain data analytics for the EOS blockchain (now called Vaulta). The repository consists of two main components:
1.	A data scraper that continuously extracts the latest block information from the blockchain.
2.	A data analysis module, hosted using Linux Systemd, which automatically analyzes the collected data daily and transform the result into a new dataset.

The data analysis part will analyze four areas of the blockchain:
1.	The top 10 accounts that creates the most sub-accounts on blockchain.
2.	All the accounts that are for sale.
3.	The most searched keywords from EOSTree.io
4.	All the current block producers (Blockchain validators).
The analysis using R provides an overview of the blockchain.

The visualization is as follow:
![acct_per_creator1](https://github.com/user-attachments/assets/3226be5f-384b-4558-a1ff-7af89dcc549f)

![acct_per_creator2](https://github.com/user-attachments/assets/c56a924b-2232-40bb-a6ff-2797799a3b6e)

![creator_rank](https://github.com/user-attachments/assets/d3a277b8-d377-406b-816e-7e805622f2b5)

![overview_2](https://github.com/user-attachments/assets/c7440849-0b6d-4c11-96e4-cf64d9e9c3dc)

![voter_pie_2](https://github.com/user-attachments/assets/5978c947-a1d1-48a3-9255-2fdd076e98ab)

The Python libraries used are:
•	Pandas
•	Numpy
•	Mysql.connector
•	Eosapi 
•	Requests

