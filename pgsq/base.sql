    -- finds out how many jobs are running per queue, so that we know if it's full
        WITH running_jobs_per_queue AS (
          SELECT
            username,
            count(1) AS running_jobs from task
          WHERE (status = 'running' OR status = 'queued') -- running or queued
          AND created_at > NOW() - INTERVAL '6 HOURS' -- ignore jobs running past 6 hours ago
          group by 1
        ),
        -- find out queues that are full
        full_queues AS (
          select
            R.username
          from running_jobs_per_queue R
          left join taskslot Q ON R.username = Q.username
          where R.running_jobs >= CASE WHEN Q.slots IS NOT NULL THEN Q.slots ELSE 3 END
        )
        select *
        from jobs
        where status IN ('created', 'failed')
          and username NOT IN ( select username from full_queues )
          and retry_time <= now()
        order by id asc
        for update skip locked
        limit 1;


-- stats

SELECT username,
    sum(case when status = 'created' then 1 else 0 end) as created,
    sum(case when status = 'running' then 1 else 0 en d) as running,
    sum(case when status = 'success' then 1 else 0 end) as success,
    sum(case when status = 'failed' then 1 else 0 end) as failed,
    count(*) as total,
    max(age(start_time, created_at)) as max_delay
    from task
    where created_at > now() - interval '1 day'
    group by username;

-- trend per day

SELECT created_at::date, count(1) as count, rpad('#', cast(count(1) / 100 as int), '#')
    FROM task
WHERE created_at > now() - interval '1 day'
GROUP BY created_at::date
ORDER BY 1 desc;


-- trend per hour

CREATE FUNCTION convert_tz(
        thetime timestamp without time zone,
        from_tz character varying,
        to_tz character varying
    ) RETURNS timestamp without time zone as $$
    BEGIN RETURN thetime at time zone from_tz at time zone to_tz ; END
$$ language plpgsql;

SELECT date_trunc('hour', convert_tz(created_at, 'UTC', 'Asia/Kuala_Lumpur')),
    count(1),
    rpad('#', cast(count(1) / 10 as int), '#')
FROM task
WHERE created_at  >= now() - interval '1 day'
GROUP BY 1;
