## Vulnerabilites Found -

1. SQL Injection Vulnerability found in search_users function, and it has been fixed by replacing it with a parameterised query where username is the parameter passed to the query.

2. A vulnerability was found in the process_transaction function where a minimum balance check was not being performed (balance could have gone below zero) as well as an SQL injection vulnerability. The query was modified to check for minimum balance and it was parameterised.

3. There was no atomicity for the update balance query so I introduced a rollback for the update sql query in case it fails.

## Performance Solution -

Context - We must use sqlite3 in this task and I'm choosing between asyncio and multithreading as background task queues are mostly for heavy production based environments so its not required for a small server like ours

Solution - I chose Python multithreading and not asyncio as sqlite3 is blocking by nature and by using threads we can have multiple threads running in the background. When the background thread faces a delay the OS automatically switches back to the main Flask thread. This will keep our API responsive allowing it to handle more requests while the background thread handles the slow banking logic.
