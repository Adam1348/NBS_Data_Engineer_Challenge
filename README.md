# Data Engineer Challenge

Build a pipeline process to move wikipedia page view statistics data from
http://dumps.wikimedia.org/other/pagecounts-raw to Google BigQuery. Find the 10 most popular wikipedia pages,
by language, during the first hour of 2012.

## Introduction
My first attempt to solve this problem was using MySql 8.0 running on my laptop which I am most familiar with. With the
growing amount of data I put into it, turns out one mysql running on localhost is too slow for this project.
To accelerate the processing speed, I might have to get rid of my laptop and turn to the cloud platforms for a solution.
Google Cloud Platform is a very popular one and I have a free trial account which is about to expire. Easy decision.

Despite the 'local or cloud' problem, what kind of technology to use is the other problem I need to settle.
I want to take advantage of this opportunity and get my hands on some interesting technology other than traditional
RDBMS. Hadoop is the first one I can think of. Apache Hive with HDFS has a SQL-like interface and is the one I really
want to learn and try. HDFS is good at processing TBs or PBs amount of data. But the largest file of Jan 1st 2012 is
about 84MB, one years' data will be around 800GB. In my opinion, Configuring HDFS may be overkill for these problem.
BigQuery can do low-latency OLAP and has a potential to scale horizontally. It uses SQL and is easy to set up.
So I decided to use it as the backend engine.

This is a really interesting project. I had a lot of fun.

## Getting Started

These instructions give you an idea on how to execute the process and tests on your local machine.

### Prerequisites
You will need Python3 and the latest version of the following packages. I used pip to install them.
```markdown
pip install requests
pip install --upgrade google-cloud-bigquery
pip install --upgrade google-cloud-storage
```

### Installing
Just download the project to your local machine.

### Running
Before you run the scripts, set the environment variable **GOOGLE_APPLICATION_CREDENTIALS** to the file path of the JSON
file I attached in the email:
```markdown
export GOOGLE_APPLICATION_CREDENTIALS="[PATH]"
```
For example
```markdown
export GOOGLE_APPLICATION_CREDENTIALS="/home/user/Downloads/service-account-file.json"
```

This variable only applies to your current shell session, so if you open a new session, set the variable again.

#### Download and clean the data
First, download the files from dumps.wikimedia.org. You can select any one hour or multiple hours by fill the start_time
and end_time slot in the format of `YYYY/mm/dd-hh`, For example, the first hour of 2012 is `2012/1/1-0`. If you just
want one hour's data, make `[start_time]` == `[end_time]`. The output will be saved under the folder
`\wiki_page_view_count`. If there isn't one, the program will create it.

After downloading, the program will convert the raw data to csv row by row, remove the `.` and its following part from
language, drop any row containing `Special:`, `User:` and `File:` and add a column containing the data time of these
records.
```markdown
cd my_challenge
python download_file.py [start_time] [end_time]
```
#### Upload to BigQuery
If this part is executed for the first time. A data set called `wiki` will be created. Provide the command with any
table name you want. The table will be created inside `wiki` and the data will be upload to it.

Later I found out my way of processing data created duplicates, such as `aa.b Main.page` and `aa.mw Main.page` now have
the same language and page. So I added one more step into it, I summed their view_counts inside to_bq.py.
```markdown
python to_bq.py [table_name]
```
#### Export to local
Due to the limitations BigQuery has on exporting files, I decided to take a detour. Saved output csv to Google Cloud
Storage and then download from there. Same as above, you need to provide the table name you want query.
```markdown
python export_bq.py [table_name]
```
Check your the project folder to collect the output csv file. There are five columns, `Language`, `Page`, `Date_time`,
`Total_views` and `Rank`.

#### Running the tests
Enter project folder, For example:
```markdown
cd Data-Engineer-Challenge
python -m unittest test.test_download_file.py
```
### Result
`output/pandora_nbs_output.csv` is the output file. Top 10 pages viewed, per language, of the first hour of 2012.

`output/result-20190116.csv` is the top 100 biggest view_count change between 2012/1/1-0 and 2012/1/1-1, which is the
second hour against the first hour.

`output/Language_Most_Viewed.csv` is the top 1000 most viewed language.

## Reference
[Google BigQuery Documentation](https://cloud.google.com/bigquery/docs/)
