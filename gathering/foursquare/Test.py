import datetime
from time import sleep

last_connect_time = datetime.datetime.utcnow()
sleep(10)
diff = datetime.datetime.utcnow() - last_connect_time
res = diff < datetime.timedelta(0, 9)
print(diff, res)