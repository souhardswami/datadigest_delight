from flask import Flask, request, jsonify, send_file, Response, make_response
from api.helper import get_next_report_id, add_new_report, get_file_path
from celery_app.celery_app import process_report

from celery_app import  celery_app

app = Flask(__name__)


@app.route('/trigger_report', methods=['GET'])
def trigger_report():
    report_id = get_next_report_id()
    celery_app.process_report.delay(report_id )
    return jsonify({"report_id": report_id})

@app.route('/get_report/<int:report_id>', methods=['GET'])
def get_report(report_id):
    path_of_file, progress = get_file_path(report_id)

    if progress != 100:
        return jsonify({"status": 'RUNNING', "Progress": progress})
        
    return send_file(
        'reports/'+ path_of_file,
        mimetype='text/csv',
        as_attachment=True,
        download_name='report.csv'
    )
    
    

if __name__ == '__main__':
    app.run(debug=True)
