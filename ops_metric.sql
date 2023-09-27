CREATE TABLE users 
(
    user_id	DOUBLE,
    created_at	VARCHAR(512),
    company_id	DOUBLE,
    language	VARCHAR(512),
    activated_at	VARCHAR(512),
    state	VARCHAR(512)
);


LOAD DATA INFILE "C:/Users/farha/Downloads/users.csv"
INTO TABLE users
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

CREATE TABLE events_users 
(
    user_id	DOUBLE,
    occurred_at	VARCHAR(512),
    event_type	VARCHAR(512),
    event_name	VARCHAR(512),
    location	VARCHAR(512),
    device	VARCHAR(512),
    user_type	DOUBLE
);

LOAD DATA INFILE "C:/Users/farha/Downloads/events.csv"
INTO TABLE events_users
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;



CREATE TABLE email_events 
(
    user_id	DOUBLE,
    occurred_at	VARCHAR(512),
    action	VARCHAR(512),
    user_type	DOUBLE
);

LOAD DATA INFILE "C:/Users/farha/Downloads/email_events.csv"
INTO TABLE email_events
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

#weekly user engagement
select week(occurred_at) as Week, count(distinct user_id) as 'Weekly User Engagement'
from events_users
group by Week
order by Week;

#user growth
SELECT 
    DATE_FORMAT(created_at, '%Y-%m') AS Month,
    COUNT(DISTINCT user_id) AS UserCount
FROM
    users
where
	activated_at not in ("")
GROUP BY
    Month
ORDER BY
    Month;


SELECT 
    Month,
    UserCount,
    UserCount - LAG(UserCount, 1, 0) OVER (ORDER BY Month) AS UserGrowth
FROM
    (SELECT 
        DATE_FORMAT(created_at, '%Y-%m') AS Month,
        COUNT(DISTINCT user_id) AS UserCount
    FROM
        users
    WHERE
        activated_at <> ''
    GROUP BY
        Month
    ) AS user_counts
ORDER BY
    Month;
    
    
#weekly_retention
Select first_login_week as Week_Numbers,
SUM(CASE WHEN week_number = 0 THEN 1 ELSE 0 END) AS "0 Week",
SUM(CASE WHEN week_number = 1 THEN 1 ELSE 0 END) AS "1 Week",
SUM(CASE WHEN week_number = 2 THEN 1 ELSE 0 END) AS "2 Weeks",
SUM(CASE WHEN week_number = 3 THEN 1 ELSE 0 END) AS "3 Weeks",
SUM(CASE WHEN week_number = 4 THEN 1 ELSE 0 END) AS "4 Weeks",
SUM(CASE WHEN week_number = 5 THEN 1 ELSE 0 END) AS "5 Weeks",
SUM(CASE WHEN week_number = 6 THEN 1 ELSE 0 END) AS "6 Weeks",
SUM(CASE WHEN week_number = 7 THEN 1 ELSE 0 END) AS "7 Weeks",
SUM(CASE WHEN week_number = 8 THEN 1 ELSE 0 END) AS "8 Weeks",
SUM(CASE WHEN week_number = 9 THEN 1 ELSE 0 END) AS "9 Weeks",
SUM(CASE WHEN week_number = 10 THEN 1 ELSE 0 END) AS "10 Weeks",
SUM(CASE WHEN week_number = 11 THEN 1 ELSE 0 END) AS "11 Weeks",
SUM(CASE WHEN week_number = 12 THEN 1 ELSE 0 END) AS "12 Weeks",
SUM(CASE WHEN week_number = 13 THEN 1 ELSE 0 END) AS "13 Weeks",
SUM(CASE WHEN week_number = 14 THEN 1 ELSE 0 END) AS "14 Weeks",
SUM(CASE WHEN week_number = 15 THEN 1 ELSE 0 END) AS "15 Weeks",
SUM(CASE WHEN week_number = 16 THEN 1 ELSE 0 END) AS "16 Weeks",
SUM(CASE WHEN week_number = 17 THEN 1 ELSE 0 END) AS "17 Week",
SUM(CASE WHEN week_number = 18 THEN 1 ELSE 0 END) AS "18 Weeks"
from
(select a.user_id, a.login_week, b.first_login_week, a.login_week-first_login_week as week_number	
from
(SELECT user_id, (week(occurred_at)) AS login_week 
FROM events_users 
GROUP BY user_id, week(occurred_at)) a,
(SELECT user_id, min(week(occurred_at)) AS first_login_week 
FROM events_users 
GROUP BY user_id) b
where a.user_id = b.user_id) as With_Week_Number
group by Week_Numbers
order by Week_Numbers;


SELECT user_id, activated_at
FROM users
WHERE activated_at > '2014-05-01'
ORDER BY user_id;

SELECT DISTINCT u.user_id, e.occurred_at
FROM users u join events_users e on u.user_id = e.user_id
WHERE u.activated_at > '2014-05-01' and e.event_name = 'login'
GROUP BY week(e.occurred_at)
ORDER BY e.occurred_at;


#weekly engagement per device
SELECT week(occurred_at) as Weeks, device,
count(distinct user_id)as User_engagement
FROM events_users
GROUP BY device, week(occurred_at)
ORDER BY week(occurred_at);


#email engagement
SELECT week(occurred_at) as Week,
count( DISTINCT ( CASE WHEN action = "sent_weekly_digest" THEN user_id end )) as weekly_digest,
count( distinct ( CASE WHEN action = "sent_reengagement_email" THEN user_id end )) as reengagement_mail,
count( distinct ( CASE WHEN action = "email_open" THEN user_id end )) as opened_email,
count( distinct ( CASE WHEN action = "email_clickthrough" THEN user_id end )) as email_clickthrough
from email_events
group by Week
order by Week




























