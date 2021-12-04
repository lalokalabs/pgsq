
import os
import sys
import time
import logging
import datetime
import importlib

import click
import logzero

from pebble import ProcessPool
from human_id import generate_id
from peewee import (
    fn,
    SQL,
    JOIN,
    Case,
    Model,
    AutoField,
    CharField,
    IntegerField,
    DateTimeField,
)
from playhouse.postgres_ext import PostgresqlExtDatabase,  BinaryJSONField

log_format = '%(color)s[%(levelname)1.1s %(asctime)s pgsq:%(funcName)s:%(lineno)d]%(end_color)s %(message)s'
formatter = logzero.LogFormatter(fmt=log_format)
logzero.setup_default_logger(formatter=formatter)
logger = logzero.logger
logzero.logfile("logs.txt")

def get_database():
    db = PostgresqlExtDatabase('pwq', user='pwq', host="localhost", password="abc123")
    return db

db = get_database()

class BaseModel(Model):
    class Meta:
        database = db

class Task(BaseModel):
    id = AutoField()
    name = CharField()
    username = CharField()
    func = CharField()
    status = CharField()
    args = BinaryJSONField(default="{}")
    kwargs = BinaryJSONField(default="{}")
    start_time = DateTimeField(null=True)
    end_time = DateTimeField(null=True)
    retry_time = DateTimeField(null=True)
    created_at = DateTimeField()
    result = CharField(default="")

class TaskSlot(BaseModel):
    username = CharField()
    slots = IntegerField()

def add_task(username, func, *args, **kwargs):
    name = kwargs.pop("name", generate_id())
    task = Task(name=name, username=username, func=func)
    task.args = args
    task.kwargs = kwargs
    now = datetime.datetime.utcnow()
    task.created_at = now
    task.retry_time = now
    task.status = kwargs.pop("status", "created")
    task.save()
    return task

def get_next_task():
    six_hours = SQL("INTERVAL '6 hours'")
    running_jobs_per_queue = (Task
            .select(Task.username, fn.Count(1).alias("running_jobs"))
            .where(Task.status.in_(["running", "queued"]))
            .where(Task.created_at > fn.Now() - six_hours)
            .group_by(Task.username)
            .cte('running_jobs_per_queue', columns=("username", "running_jobs")))

    full_queues = (running_jobs_per_queue
            .select_from([running_jobs_per_queue.c.username])
            .join(TaskSlot, JOIN.LEFT_OUTER, on=(running_jobs_per_queue.c.username == TaskSlot.username))
            .where(running_jobs_per_queue.c.running_jobs >= Case(None, [((TaskSlot.slots != None), TaskSlot.slots)], 3))
            .cte("full_queues", columns=("username",)))

    query = (Task
            .select()
            .where(Task.status.in_(["created", "failed"]))
            .where(Task.username.not_in(full_queues.select(full_queues.c.username)))
            .where(Task.retry_time <= fn.Now())
            .order_by(Task.id)
            .for_update("for update skip locked")
            .limit(1)
            .with_cte(running_jobs_per_queue, full_queues))
    return query.get()

def update_task(task, current_status, **kwargs):
    Task = type(task)
    status = kwargs["status"]
    retry_time = task.retry_time
    if status == "failed":
        one_sec = datetime.timedelta(seconds=1)
        retry_time = task.retry_time + ((task.retry_time - task.created_at + one_sec) * 2)
        logger.info(f"{task.name} failed, current_status: {current_status} created_at: {task.created_at} delayed to {retry_time}")

    query = Task.update(retry_time=retry_time, **kwargs).where(Task.id==task.id, current_status==current_status)
    return query.execute()

def do_task(task):
    pid = os.getpid()
    logger.info(f"{task.id} processing [{task.name}] pid:{pid} {task.username}")

    try:
        module, func = task.func.rsplit('.', 1)
        importlib.invalidate_caches()
        m = importlib.import_module(module)
        f = getattr(m, func)
    except (ValueError, ImportError, AttributeError) as e:
        result = (e, False)
        logger.error(f"Failed getting function {task.func} {task.name}")
        return result

    try:
        logger.info(f"Executing {f} {task.name} pid:{pid}")
        res = f(*task.args, **task.kwargs)
        result = (res, True)
    except Exception as e:
        logger.error(f"user: {task.username} task: {task.name} error: {e}")
        result = (e, False)
        update_task(task, current_status="running", status="failed")
        return result

    logger.info(f"DONE with {task.name}, updating status success pid:{pid}")
    affected = update_task(task, current_status="running", status="success", result=result)
    if affected:
        logger.info(f"{task.name} status success pid:{pid}")

    return result

def do_task_runner(task):
    db = get_database()
    with db.bind_ctx([Task, TaskSlot]):
        return do_task(task)

def task_done(future, task):
    ret = False
    try:
        result, ret = future.result()  # blocks until results are ready
    except TimeoutError as error:
        logger.error(f"task: {task.name} timeout, took longer than {error.args[1]} seconds")
        ret = False
        result = error
    except Exception as error:
        logger.error(f"task: {task.name} failed error: {error.traceback}")
        ret = False
        result = error
    else:
        logger.info(f"task: {task.name} done result: {result}")

    status = "success" if ret else "failed"
    affected = update_task(task, current_status="running", status=status, result=result)
    logger.info(f"task: {task.name} updated to {status} affected: {affected}")

def process_task(pool):
    while True:
        with db.atomic() as transaction:
            try:
                task = get_next_task()
            except Exception as e:
                #print("ERROR: ", e)
                time.sleep(3)
                continue
            else:
                res = pool.schedule(do_task_runner, (task,), timeout=20)
                print(task)
                affected = update_task(task, current_status=task.status, status="running")
                logger.info(f"Task {task.name} username: {task.username} scheduled {affected}")
                def _task_done_wrapper(future, task=task):
                    task_done(future, task)
                res.add_done_callback(_task_done_wrapper)

@click.group()
def cli():
    pass

@cli.command()
@db.atomic()
def initdb():
    db.create_tables([Task, TaskSlot])


@cli.command()
@click.option("--num", default=1, help="Number of workers")
def workers(num=1):
    with ProcessPool(max_workers=num, max_tasks=10) as pool:
        try:
            print("Running")
            process_task(pool)
        except KeyboardInterrupt:
            print("Exiting ...")
            pool.close()
            pool.join()
        except Exception as e:
            print("ERROR:", e)
            time.sleep(5)
            sys.exit(1)

def run():
    cli()

