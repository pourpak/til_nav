# %%
import duckdb

db_source = '/run/media/christoffer/M.2 Samsung/reddit.duckdb'
submission_path = '/home/christoffer/Nedlastinger/reddit/submissions/RS_2024-08.zst'
comment_path = '/home/christoffer/Nedlastinger/reddit/comments/RC_2024-08.zst'


con = duckdb.connect(db_source)

# %%
con.execute("""
    CREATE TABLE submissions (
    author VARCHAR,
    author_fullname VARCHAR,
    id VARCHAR,
    title VARCHAR,
    created_utc TIMESTAMP,
    subreddit VARCHAR,
    subreddit_id VARCHAR,
    name VARCHAR,
    selftext VARCHAR,
    score INTEGER,
    is_self VARCHAR,
    media_only VARCHAR,
    num_comments INTEGER
)
    """)

con.execute(f"""
INSERT INTO submissions
SELECT author, author_fullname, id, title, to_timestamp(created_utc), subreddit, subreddit_id, name, selftext, score, 
        is_self, media_only, num_comments
FROM read_json(
{submission_path},
format = 'newline_delimited',
compression = 'zstd'
 )
""")

# %%
con.execute("""
    CREATE TABLE comments (
    author VARCHAR,
    author_fullname VARCHAR,
    id VARCHAR,
    created_utc TIMESTAMP,
    subreddit VARCHAR,
    subreddit_id VARCHAR,
    name VARCHAR,
    body VARCHAR,
    score INTEGER,
    parent_id VARCHAR,
)
    """)

# %%
con.execute(f"""
INSERT INTO comments
SELECT author, author_fullname, id, to_timestamp(created_utc), subreddit, subreddit_id, name, body, score, parent_id
FROM read_json(
'/home/christoffer/Nedlastinger/reddit/comments/RC_2024-08.zst',
format = 'newline_delimited',
compression = 'zstd'
 )
""")

