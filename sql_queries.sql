Data lemor questions answer

create database vr
use vr;


CREATE TABLE daily_sales (
    product_id INT,
    sale_date DATE,
    sales INT
);
INSERT INTO daily_sales VALUES
(1, '2022-01-01', 100),
(1, '2022-01-02', 120),
(1, '2022-01-03', 110),
(1, '2022-01-04', 150),
(1, '2022-01-05', 130);

select * from (
select product_id,sale_date,round(avg(sales) over(partition by product_id order by sale_date rows between 2 preceding and current row),2) as rolling_avg_3days
from daily_sales
)t
where rolling_avg_3days > 120
order by product_id,sale_date

CREATE TABLE daily_sales1 (
    product_id INT,
    sale_date DATE,
    sales INT
);

INSERT INTO daily_sales1 VALUES
(1, '2022-01-01', 100),
(1, '2022-01-02', 120),
(1, '2022-01-03', 130),
(1, '2022-01-04', 90),

(2, '2022-01-01', 200),
(2, '2022-01-02', 180),
(2, '2022-01-03', 210),
(2, '2022-01-04', 220);

select distinct product_id from(
select product_id,sales,lag(sales,1) over(partition by product_id order by sale_date) as prev1,
lag(sales,2) over(partition by product_id order by sale_date) as prev2
from daily_sales1 
)t
where sales > prev1
and prev1 > prev2
order by product_id

CREATE TABLE user_logins (
    user_id INT,
    login_date DATE
);
INSERT INTO user_logins VALUES
(1, '2022-01-01'),
(1, '2022-01-02'),
(1, '2022-01-03'),
(1, '2022-01-05'),

(2, '2022-01-01'),
(2, '2022-01-03'),
(2, '2022-01-04'),
(2, '2022-01-05'),

(3, '2022-01-10'),
(3, '2022-01-11'),
(3, '2022-01-12');

select  distinct user_id from(
select user_id, 
lag(login_date) over() as 3_consutive_days
from user_logins
)t
group by user_id
having count(*) <=3
order by user_id asc;

select distinct user_id from(
select user_id,login_date,lag(login_date,1) over(partition by user_id order by login_date) as prev_1,
lag(login_date,2) over(partition by user_id order by login_date) as prev_2
from user_logins
)t
where datediff(login_date,prev_1) = 1
and datediff(login_date,prev_2) = 1
order by user_id

DROP TABLE IF EXISTS events;

CREATE TABLE events (
    app_id INT,
    event_type VARCHAR(20),
    timestamp DATETIME
);

INSERT INTO events VALUES
(123, 'impression', '2022-07-18 11:36:12'),
(123, 'impression', '2022-07-18 11:37:12'),
(123, 'click', '2022-07-18 11:37:42'),
(234, 'impression', '2022-07-18 14:15:12'),
(234, 'click', '2022-07-18 14:16:12');

select app_id,
round(100 * sum(case when event_type = 'click' then 1 else 0 end)/
sum(case when event_type = 'impression' then 1 else 0 end),2)
from events
where year(timestamp) = 2022
group by app_id

DROP TABLE IF EXISTS tweets;

CREATE TABLE tweets (
    tweet_id INT,
    user_id INT,
    msg VARCHAR(255),
    tweet_date DATETIME
);

INSERT INTO tweets VALUES
(214252, 111, 'Tesla private at $420. Funding secured.', '2021-12-30 00:00:00'),
(739252, 111, 'Despite the constant negative press covfefe', '2022-01-01 00:00:00'),
(846402, 111, 'Following @NickSinghTech changed my life!', '2022-02-14 00:00:00'),
(241425, 254, 'If the salary is so competitive why won’t you tell me?', '2022-03-01 00:00:00'),
(231574, 148, 'I no longer have a manager. I can’t be managed', '2022-03-23 00:00:00');

select user_num as tweet_count,count(*) as user_num from(
select user_id,count(*) as user_num from 
tweets
where year(tweet_date) = 2022
group by user_id
)t
group by user_num
order by tweet_count

DROP TABLE IF EXISTS posts;

CREATE TABLE posts (
    user_id INT,
    post_id INT,
    post_content TEXT,
    post_date DATETIME
);

INSERT INTO posts VALUES
(151652, 599415, 'Need a hug', '2021-07-10 12:00:00'),
(661093, 624356, 'Another busy day', '2021-07-29 13:00:00'),
(004239, 784254, 'Happy 4th of July!', '2021-07-04 11:00:00'),
(661093, 442560, 'Just going to cry', '2021-07-08 14:00:00'),
(151652, 111766, 'I''m so done with covid', '2021-07-12 19:00:00');

select * from posts;

select user_id,datediff(max(post_date),min(post_date))as days_between
from posts
where year(post_date) = 2021
group by user_id
having count(*) >=2

DROP TABLE IF EXISTS posts;

CREATE TABLE posts (
    user_id INT,
    post_id INT,
    post_content TEXT,
    post_date DATETIME
);

INSERT INTO posts VALUES
(1, 101, 'Post A', '2021-01-01 10:00:00'),
(1, 102, 'Post B', '2021-01-05 12:00:00'),
(1, 103, 'Post C', '2021-01-10 08:00:00'),
(2, 201, 'Post D', '2021-02-01 09:00:00'),
(2, 202, 'Post E', '2021-02-03 11:00:00'),
(3, 301, 'Only post', '2021-03-01 15:00:00');

select user_id,datediff(max(post_date),min(post_date)) as days_between,
round(avg(gap_days),2) as avg_gap_days from(
select user_id,post_date,datediff(post_date,lag(post_date) over(partition by user_id order by post_date))as gap_days
from posts
where year(post_date) = 2021
)t
group by user_id
having count(*) >= 2
order by user_id

DROP TABLE IF EXISTS trades;
DROP TABLE IF EXISTS users;

CREATE TABLE trades (
    order_id INT,
    user_id INT,
    quantity INT,
    status VARCHAR(20),
    date DATETIME,
    price DECIMAL(5,2)
);

CREATE TABLE users (
    user_id INT,
    city VARCHAR(50),
    email VARCHAR(100),
    signup_date DATETIME
);

INSERT INTO trades VALUES
(100101, 111, 10, 'Cancelled', '2022-08-17 12:00:00', 9.80),
(100102, 111, 10, 'Completed', '2022-08-17 12:00:00', 10.00),
(100259, 148, 35, 'Completed', '2022-08-25 12:00:00', 5.10),
(100264, 148, 40, 'Completed', '2022-08-26 12:00:00', 4.80),
(100305, 300, 15, 'Completed', '2022-09-05 12:00:00', 10.00),
(100400, 178, 32, 'Completed', '2022-09-17 12:00:00', 12.00),
(100565, 265, 2, 'Completed', '2022-09-27 12:00:00', 8.70);

INSERT INTO users VALUES
(111, 'San Francisco', 'rrok10@gmail.com', '2021-08-03 12:00:00'),
(148, 'Boston', 'sailor9820@gmail.com', '2021-08-20 12:00:00'),
(178, 'San Francisco', 'harrypotterfan182@gmail.com', '2022-01-05 12:00:00'),
(265, 'Denver', 'shadower_@hotmail.com', '2022-02-26 12:00:00'),
(300, 'San Francisco', 'houstoncowboy1122@hotmail.com', '2022-06-30 12:00:00');

select u.city,count(t.order_id) as total_orders 
from trades t join users u on t.user_id=u.user_id 
where t.status = 'Completed'
group by u.city
order by total_orders desc limit 3;

select city,total_orders from(
select u.city,count(*)as total_orders,dense_rank() over(order by count(*)desc) as rk
from trades t join users u on t.user_id=u.user_id
where t.status = 'completed'
group by u.city
)t
where rk <=3
order by total_orders desc

DROP TABLE IF EXISTS pages;
DROP TABLE IF EXISTS page_likes;

CREATE TABLE pages (
    page_id INT PRIMARY KEY,
    page_name VARCHAR(100)
);

CREATE TABLE page_likes (
    user_id INT,
    page_id INT,
    liked_date DATETIME
);

INSERT INTO pages VALUES
(20001, 'SQL Solutions'),
(20045, 'Brain Exercises'),
(20701, 'Tips for Data Analysts');

INSERT INTO page_likes VALUES
(111, 20001, '2022-04-08 00:00:00'),
(121, 20045, '2022-03-12 00:00:00'),
(156, 20001, '2022-07-25 00:00:00');

select p.page_id from pages p left join page_likes pl on p.page_id=pl.page_id
where pl.page_id  is null;

select p.page_id from pages p
where  not exists (
select 1 from page_likes pl
where pl.page_id=p.page_id
)
order by p.page_id

DROP TABLE IF EXISTS parts_assembly;

CREATE TABLE parts_assembly (
    part VARCHAR(50),
    finish_date DATETIME,
    assembly_step INT
);

INSERT INTO parts_assembly VALUES
('battery', '2022-01-22 00:00:00', 1),
('battery', '2022-02-22 00:00:00', 2),
('battery', '2022-03-22 00:00:00', 3),
('bumper', '2022-01-22 00:00:00', 1),
('bumper', '2022-02-22 00:00:00', 2),
('bumper', NULL, 3),
('bumper', NULL, 4);


select part,assembly_step from parts_assembly 
where finish_date is null

DROP TABLE IF EXISTS viewership;

CREATE TABLE viewership (
    user_id INT,
    device_type VARCHAR(20),
    view_time DATETIME
);

INSERT INTO viewership VALUES
(123, 'tablet', '2022-01-02 00:00:00'),
(125, 'laptop', '2022-01-07 00:00:00'),
(128, 'laptop', '2022-02-09 00:00:00'),
(129, 'phone',  '2022-02-09 00:00:00'),
(145, 'tablet', '2022-02-24 00:00:00');

select 
sum(case when device_type = 'laptop' then 1 else 0 end)as laptop_views,
sum(case when device_type in('phone','tablet') then 1 else 0 end)mobie_views
from viewership

DROP TABLE IF EXISTS posts;

CREATE TABLE posts (
    user_id INT,
    post_id INT,
    post_content TEXT,
    post_date DATETIME
);

INSERT INTO posts VALUES
(151652, 599415, 'Need a hug', '2021-07-10 12:00:00'),
(661093, 624356, 'Another day...', '2021-07-29 13:00:00'),
(4239, 784254, 'Happy 4th of July!', '2021-07-04 11:00:00'),
(661093, 442560, 'Just going to cry...', '2021-07-08 14:00:00'),
(151652, 111766, 'I need traveling ASAP!', '2021-07-12 19:00:00');

select user_id,datediff(max(post_date),min(post_date)) as days_between
from posts
where year(post_date) = 2021
group by user_id
having count(*) >= 2

DROP TABLE IF EXISTS messages;

CREATE TABLE messages (
    message_id INT,
    sender_id INT,
    receiver_id INT,
    content VARCHAR(255),
    sent_date DATETIME
);

INSERT INTO messages VALUES
(901, 3601, 4500, 'You up?', '2022-08-03 00:00:00'),
(902, 4500, 3601, 'Only if you’re buying', '2022-08-03 00:00:00'),
(743, 3601, 8752, 'Let’s take this offline', '2022-06-14 00:00:00'),
(922, 3601, 4500, 'Get on the call', '2022-08-10 00:00:00');

select sender_id,count(*) as message_count from
messages
where year(sent_date) = 2022 and month(sent_date) = 8
group by sender_id
order by message_count desc
limit 2;

select sender_id,message_count from(
select sender_id,count(*) as message_count,dense_rank() over(order by count(*) desc) as rk
from messages
where sent_date >= '2022-08-01' and sent_date < '2022-09-01'
group by sender_id
)t
where rk <= 2
order by message_count desc

DROP TABLE IF EXISTS job_listings;

CREATE TABLE job_listings (
    job_id INT,
    company_id INT,
    title VARCHAR(255),
    description TEXT
);

INSERT INTO job_listings VALUES
(248, 827, 'Business Analyst', 'Evaluates business data to improve decision-making.'),
(149, 845, 'Business Analyst', 'Evaluates business data to improve decision-making.'),
(945, 345, 'Data Analyst', 'Reviews data to identify key insights.'),
(164, 345, 'Data Analyst', 'Reviews data to identify key insights.'),
(172, 244, 'Data Engineer', 'Builds systems to collect and manage raw data.');

select count(distinct company_id) as duplicate_company from(
select company_id
from job_listings
group by company_id,title,description
having count(*) >= 2
)t;

DROP TABLE IF EXISTS reviews;

CREATE TABLE reviews (
    review_id INT,
    user_id INT,
    submit_date DATETIME,
    product_id INT,
    stars INT
);

INSERT INTO reviews VALUES
(6171, 123, '2022-06-08 00:00:00', 50001, 4),
(7802, 265, '2022-06-10 00:00:00', 69852, 4),
(5293, 362, '2022-06-18 00:00:00', 50001, 3),
(6352, 192, '2022-07-26 00:00:00', 69852, 3),
(4517, 981, '2022-07-05 00:00:00', 69852, 2);

select month(submit_date) as months,product_id,round(avg(stars),2)as avg_star_rating
from reviews
group by month(submit_date),product_id
order by month(submit_date),product_id

DROP TABLE IF EXISTS employee;

CREATE TABLE employee (
    employee_id INT,
    name VARCHAR(100),
    salary INT,
    department_id INT,
    manager_id INT
);

INSERT INTO employee VALUES
(1, 'Emma Thompson', 3800, 1, 6),
(2, 'Daniel Rodriguez', 2230, 1, 7),
(3, 'Olivia Smith', 7000, 1, 8),
(4, 'Noah Johnson', 6800, 2, 9),
(5, 'Sophia Martinez', 1750, 1, 11),
(6, 'Liam Brown', 13000, 3, NULL),
(7, 'Ava Garcia', 12500, 3, NULL),
(8, 'William Davis', 6800, 2, NULL);

select  e1.employee_id,e1.name from employee e1 join employee e2 
on e1.manager_id=e2.employee_id
where e1.salary > e2.salary

select e1.name as employee_name,e2.name as manager_name from employee e1 join employee e2
on e1.manager_id=e2.employee_id

select name as employee_name from employee 
where manager_id is null

select e1.name as emp1_name,e2.name,e1.manager_id from employee
e1 join employee e2 on e1.manager_id=e2.manager_id
and e1.employee_id < e2.employee_id
where e1.manager_id is not null

q.12

DROP TABLE IF EXISTS transactions;

CREATE TABLE transactions (
    transaction_id INT,
    account_id INT,
    amount DECIMAL(10,2),
    transaction_type VARCHAR(20)
);

INSERT INTO transactions VALUES
(123, 101, 10.00, 'Deposit'),
(124, 101, 20.00, 'Deposit'),
(125, 101, 5.00,  'Withdrawal'),
(126, 201, 20.00, 'Deposit'),
(128, 201, 10.00, 'Withdrawal');


select account_id,
sum(case when transaction_type = 'Deposit' then amount 
when transaction_type = 'Withdrawal' then -amount end)as final_balance
from transactions 
group by account_id

select account_id,
sum(case when transaction_type = 'Deposit' then amount else 0 end)
-
sum(case when transaction_type = 'Withdrawal' then amount else 0 end)
as final_balance
from transactions
group by account_id

DROP TABLE IF EXISTS events;

CREATE TABLE events (
    app_id INT,
    event_type VARCHAR(20),
    timestamp DATETIME
);

INSERT INTO events VALUES
(123, 'impression', '2022-07-18 11:36:12'),
(123, 'impression', '2022-07-18 11:37:12'),
(123, 'click',      '2022-07-18 11:37:42'),
(234, 'impression', '2022-07-18 14:15:12'),
(234, 'click',      '2022-07-18 14:16:12'),
(234, 'click',      '2021-07-18 14:16:12');  

q.13
select app_id,
round(100.0 *sum(case when event_type = 'click' then 1 else 0 end)
/
sum(case when event_type = 'impression' then 1 else 0 end)
,2)as CTR
from events
where year(timestamp) = 2022
group by app_id

DROP TABLE IF EXISTS website_events;

CREATE TABLE website_events (
    campaign_id INT,
    event_type VARCHAR(20),   -- 'visit' or 'purchase'
    event_time DATETIME
);
INSERT INTO website_events VALUES
(1, 'visit',    '2023-01-01 10:00:00'),
(1, 'visit',    '2023-01-01 11:00:00'),
(1, 'purchase', '2023-01-01 11:30:00'),
(1, 'purchase', '2022-12-31 11:30:00'),  -- should NOT count

(2, 'visit',    '2023-02-05 09:00:00'),
(2, 'visit',    '2023-02-05 10:00:00'),
(2, 'visit',    '2023-02-05 11:00:00'),
(2, 'purchase', '2023-02-05 12:00:00');

select campaign_id,
round(100.0 * sum(case when event_type = 'purchase' then 1 else 0 end)
/
sum(case when event_type = 'visit' then 1 else 0 end)
,2) as conversion_rate
from website_events 
where year(event_time) = 2023
group by campaign_id

DROP TABLE IF EXISTS emails;

CREATE TABLE emails (
    email_id INT,
    user_id INT,
    signup_date DATETIME
);
DROP TABLE IF EXISTS texts;

CREATE TABLE texts (
    text_id INT,
    email_id INT,
    signup_action VARCHAR(20),  -- 'Confirmed' or 'Not Confirmed'
    action_date DATETIME
);
INSERT INTO emails VALUES
(125, 7771, '2022-06-14'),
(433, 1052, '2022-07-09');

INSERT INTO texts VALUES
(6878, 125, 'Confirmed', '2022-06-14'),
(6997, 433, 'Not Confirmed', '2022-07-09'),
(7000, 433, 'Confirmed', '2022-07-10');


select e.user_id from emails e  join texts t on e.email_id=t.email_id
 where e.signup_date = 'Not Confirmed' and e.signup_date + 1
 
 
