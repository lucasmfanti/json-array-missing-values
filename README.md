# json-array-missing-values
This is a repo containing a DAG with a very specific use. 
# The database
Let's suppose you have a database with a history table, containing the history of a certain event. That table has an id and also an event_id, so that an event can have multiple event_history entries. With that table, you can view the whole history of your event. Let's suppose there is also a column named values, containing an array of different JSON files, with different ids of these values associated with that event. The problem is that you need to find out exactly when the values went missing from your event history.
# The structure
So, this DAG is aimed to solve that problem. You can't easily work with that type of file in SQL and do it natively on BigQuery, but that is not an issue to Python. So we first extract the relevant info into a raw dataframe. We manipulate that dataframe to obtain the information we need. And finally we export the dataframe to a csv on a Google Storage folder where we can create a table from the files in that folder. 
The dataframe is structured like a transition table, containing a from_eh_id and a to_eh_id, so that we know that the value went missing during the transition of those two entries on the event history table.
# The granularity
We start from a event history granularity, and at the end we are at a value granularity.
# BigQuery() and Storage()
Those two functions are custom made functions to perform a query on BigQuery and to upload from your Airflow instance, respectively.