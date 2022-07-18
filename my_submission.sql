-- I. How many page views were generated for a specific day?

SELECT
    COUNT(DISTINCT event_id) AS num_views
FROM `snowplow-cto-office.snowplow_hackathonPI.events_hackathon`
WHERE 1 = 1
  AND event_name = 'page_view'
  AND DATE(derived_tstamp) = '2021-09-15'; -- Or any other date

-- If we want to check this for all dates in the dataset:
SELECT
    DATE(derived_tstamp) AS date_key,
    COUNT(DISTINCT event_id) AS num_views
FROM `snowplow-cto-office.snowplow_hackathonPI.events_hackathon`
WHERE 1 = 1
  AND event_name = 'page_view'
GROUP BY 1
ORDER BY 1;

-- I use DISTINCT, because I have noticed that multiple rows can share an event_id.
SELECT
  event_name,
  COUNT(*) AS num_rows,
  COUNT(DISTINCT event_id) AS num_events,
  ROUND(COUNT(DISTINCT event_id) / COUNT(*) * 100, 2) AS events_to_rows_ratio_percent
FROM
  `snowplow-cto-office.snowplow_hackathonPI.events_hackathon`
GROUP BY 1
ORDER BY 1;

-- However, this is not very common, especially for the 'page_view' event.
-- I decided to take a look at such cases for 'page_view'.
WITH duplicated_ids AS (
SELECT
  *,
  COUNT(*) OVER (PARTITION BY event_id) AS num_rows_per_event
FROM
  `snowplow-cto-office.snowplow_hackathonPI.events_hackathon`
WHERE 1 = 1
  AND DATE(derived_tstamp) = '2021-09-15'
  AND event_name = 'page_view'
)
SELECT *
FROM duplicated_ids
WHERE num_rows_per_event > 1
ORDER BY event_id, derived_tstamp
LIMIT 500;

-- Sometimes the difference is in "network_userid", but at times it
-- looks like it is just in the timestamps.
-- It would be interesting to compare the rows precisely and
-- research why the system behaves this way,
-- but for now I will just keep the duplicated IDs in mind

-- II. How many users are active day by day?

SELECT
    DATE(derived_tstamp) AS date_key,
    COUNT(DISTINCT user_id) AS num_distinct_users
FROM `snowplow-cto-office.snowplow_hackathonPI.events_hackathon`
GROUP BY 1
ORDER BY 1;

-- III. How many sessions?

SELECT
    DATE(derived_tstamp) AS date_key,
    COUNT(DISTINCT domain_sessionid) AS num_distinct_sessions
FROM `snowplow-cto-office.snowplow_hackathonPI.events_hackathon`
GROUP BY 1
ORDER BY 1;

-- There are 2 columns referencing sessions:
-- 1) domain_sessionid; 275,752 unique values
-- 2) domain_sessionidx; 3,447 unique values
--
-- I needed to deduce, which one corresponds to user sessions.
-- It turned out that indices from "domain_sessionidx" can span the whole month
SELECT
    domain_sessionidx,
    MIN(derived_tstamp) AS first_event_at,
    MAX(derived_tstamp) AS last_event_at
FROM `snowplow-cto-office.snowplow_hackathonPI.events_hackathon`
GROUP BY 1
ORDER BY 1
LIMIT 50;
-- Thus, I need to look at "domain_sessionid".
-- I also checked what sessions based on "domain_sessionid"
-- look like in terms of events to be sure:
SELECT
  domain_sessionidx, user_id,
  event_name, derived_tstamp,
  domain_sessionid
FROM `snowplow-cto-office.snowplow_hackathonPI.events_hackathon`
WHERE 1 = 1
  AND DATE(derived_tstamp) = '2021-09-15'
ORDER BY 1, 5, 4
LIMIT 500;
-- A single domain_sessionid unites a set of consecutive user actions.

-- During the research I have also found out
-- that there are some sessions with multiple users:
-- 274,867 single-user sessions
-- 848 sessions with 2 users
-- 31 sessions with 3 users
-- 6 sessions with 4 users
WITH multiple_users_sessions AS (
SELECT
  domain_sessionid, COUNT(DISTINCT user_id) AS num_users,
  COUNT(DISTINCT event_id) AS num_events,
  TIMESTAMP_DIFF(MAX(derived_tstamp), MIN(derived_tstamp), MINUTE) AS ses_len_min
FROM `snowplow-cto-office.snowplow_hackathonPI.events_hackathon`
GROUP BY domain_sessionid
HAVING COUNT(DISTINCT user_id) > 1
ORDER BY 1
)

SELECT num_users, COUNT(DISTINCT domain_sessionid) AS num_sessions
FROM multiple_users_sessions
GROUP BY 1
ORDER BY 1;

SELECT
  domain_sessionidx, user_id, event_name,
  derived_tstamp,
  domain_sessionid
FROM `snowplow-cto-office.snowplow_hackathonPI.events_hackathon`
WHERE 1 = 1
  AND domain_sessionid IN (
    SELECT domain_sessionid FROM multiple_users_sessions
  )
ORDER BY 5, 4;

-- I would double-check if this behavior is normal

-- IV. What is the time spent per page_url (hashed)?

-- Unfortunately, there is no event explicitly indicating
-- that a page has been closed after being viewed.
-- Likewise, there is no event indicating the end of a user session.
-- (Events only include
-- "page_view", "change_form", "focus_form", "link_click", "submit_form")
--
-- For example, we could calculate the time spent by a user viewing a page
-- provided that they always open another page afterwards (or end session) by
-- computing the difference between consecutive events' timestamps, but
-- sometimes sessions consist of a single event only.
-- (E.g. domain_sessionid = 0013ae14-be30-4989-8f09-2edc943e8f48)
-- Also, there are 18 users, who only have one session of one event on their record:
SELECT
  user_id,
  COUNT(DISTINCT domain_sessionid) AS num_sessions,
  COUNT(DISTINCT event_id) AS num_events,
  ARRAY_AGG(event_name) AS event_names,
  ARRAY_AGG(hashed_page_url) AS page_urls
FROM `snowplow-cto-office.snowplow_hackathonPI.events_hackathon`
GROUP BY 1
HAVING COUNT(DISTINCT event_id) = 1
ORDER BY 1;
-- It is not possible to determine the time these users spent viewing the pages
-- unless additional information is provided

-- I would like to check the number of distinct URLs in the dataset
SELECT COUNT(DISTINCT hashed_page_url) -- The result is 164,273 unique addresses.
FROM `snowplow-cto-office.snowplow_hackathonPI.events_hackathon`

-- I would like to discuss whether we are interested in the time spent
-- per each of these URLs? If so, what is our final goal?

-- I would also like to see how frequently each URL is mentioned in the dataset
WITH events_per_url AS (
SELECT hashed_page_url, COUNT(DISTINCT event_id) AS num_events
FROM `snowplow-cto-office.snowplow_hackathonPI.events_hackathon`
GROUP BY 1
)
SELECT
  MIN(num_events) AS min_events_per_url, -- 1
  CAST(AVG(num_events) AS INT) AS avg_events_per_url, -- 53
  MAX(num_events) AS max_events_per_url -- 64,911
FROM events_per_url;

SELECT
    PERCENTILE_CONT(num_events, 0.25) OVER() AS q1_events_per_url, -- 2
    PERCENTILE_CONT(num_events, 0.5) OVER() AS median_events_per_url, -- 5
    PERCENTILE_CONT(num_events, 0.75) OVER() AS q3_events_per_url, -- 16
    PERCENTILE_CONT(num_events, 0.9) OVER() AS point_nine_events_per_url -- 72
FROM events_per_url
LIMIT 1;

-- We can see that most of the URLs were opened incomparably fewer times
-- than a relatively small set of the ones mainly used.
-- I will visualize this in Data Studio

-- V. How many customers are active day by day?

SELECT
  DATE(derived_tstamp) AS date_key,
  COUNT(DISTINCT hashed_customer_name) AS num_customers
FROM `snowplow-cto-office.snowplow_hackathonPI.events_hackathon`
GROUP BY 1
ORDER BY 1;


-- VI. Whats the activity for different user roles (user types)?

-- There is only one user_type in the dataset - "Admin"
SELECT DISTINCT user_type
FROM `snowplow-cto-office.snowplow_hackathonPI.events_hackathon`;

SELECT
  user_type, event, event_name,
  COUNT(DISTINCT event_id) AS num_events
FROM `snowplow-cto-office.snowplow_hackathonPI.events_hackathon`
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3;


