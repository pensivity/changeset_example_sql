# changeset_example_sql
An example of a changeset process used to take records from one SQL table and push the differences into another SQL table.

This process taught me a lot about how to protect operations on a table (using TRY...CATCH with THROW, and begin/commit/rollback transactions), as well as about batching (using semicolons and GO). Most of my learning in SQL has been self-taught so this was a good exercise.

Note: This process should have been created as a stored procedure but for reasons I won't get in to the database this was created for does not allow stored procs.
