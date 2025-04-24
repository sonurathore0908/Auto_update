**🎯 Purpose of the Project**
This project is a Redshift Auto-Updater with Email Notification.
Its main job is to automate the update of rows in a Redshift database table based on your custom logic defined in a JSON file.
After making updates, it sends you an email summary showing:

-> How many rows matched your condition (SELECT count),
-> How many rows were updated,
-> The actual SQL queries used.

✅ What It Automates
✅ Finds records in your Redshift table that match certain conditions.
✅ Updates specific columns with new values for those records.
✅ Logs the SELECT and UPDATE results.
✅ Sends an email report to you or your team.

🧠 Use Cases
-> Mark "fake device installs" as "not fraud" after review.
->Update any batch of records programmatically.
->Send a daily/weekly summary of what data was changed.
->Replace manual SQL updates and manual email writing.

⚙️ Example
Imagine you work in fraud analytics, and every day you have to:
-> Go into Redshift and check which installs are labeled as "Fake Device".
-> Update them to "Not Fraud" after verification.
-> Email your team about what was changed.

**With this tool:**

-> You define that logic in config.json once.
**** Just run update.py and it:****
Executes the queries,
Logs how many rows changed,
Emails your team instantly.
