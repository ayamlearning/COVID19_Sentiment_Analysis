from crontab import CronTab

cron = CronTab(user='root')
job = cron.new(command='python twitter_streaming_pipeline.py')
job.minute.on(0)
job.hour.on(12)

cron.write()
