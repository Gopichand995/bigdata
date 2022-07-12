select * from celery_taskmeta 
where date_done BETWEEN '2022-04-06 10:40:40' and '2022-04-06 10:40:45'
and lower(status) = 'success'