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
          left join user_queues Q ON R.username = Q.username
          where R.running_jobs >= CASE WHEN Q.num_slots IS NOT NULL THEN Q.num_slots ELSE 3 END
        )
        select *
        from jobs
        where status IN ('created', 'failed')
          and username NOT IN ( select username from full_queues )
        '''
        sql_dedicated = 'and username = %s'
        sql_end = '''
          and retry_time <= now()
        order by id asc
        for update skip locked
        limit 1;
