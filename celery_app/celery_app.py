from celery import Celery
from api.helper import add_new_report
from celery_app.report_task import report_task
celery = Celery('celery_app', broker='redis://localhost:6379/0')

@celery.task
def process_report(report_id):
    add_new_report(report_id)
    report_task(report_id)
    