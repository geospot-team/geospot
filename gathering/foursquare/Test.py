import datetime

current_day = datetime.datetime.now()
current_week_monday = current_day - datetime.timedelta(days=current_day.weekday())
print(current_week_monday.weekday() - 1)
def get_week_suffix():
    current_day = datetime.datetime.now()
    current_week_monday = current_day - datetime.timedelta(days=current_day.weekday())
    return current_week_monday.strftime("%Y-%m-%d")