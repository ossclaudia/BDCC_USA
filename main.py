from flask import Flask, request, jsonify, redirect, render_template
from google.cloud import bigquery
import flask
import concurrent.futures
from google.appengine.api import wrap_wsgi_app
from google.appengine.ext import blobstore
from google.appengine.ext import ndb
import requests 
from gcf import update_statistics 

app = Flask(__name__)
app.wsgi_app = wrap_wsgi_app(app.wsgi_app, use_deferred=True)

client = bigquery.Client(location="us")

PROJECT_ID = "projetousa"
DATASET_ID = "MIMIC"
TABLE_REF = f"{PROJECT_ID}.{DATASET_ID}"
TABLE_PATIENTS = "PATIENTS"
TABLE_ADMISSIONS = "ADMISSIONS"
TABLE_QUESTIONS = "QUESTIONS"
TABLE_ANSWERS = "ANSWERS"
TABLE_EVENTS = 'LAB_EVENTS'
TABLE_INPUT = 'INPUT_EVENTS'

@app.route('/')
def homepage():
    return render_template("base.html")


@app.route("/results")
def results():
    query = f"""
    SELECT subject_id, 
        gender, 
        dob
    FROM `{TABLE_REF}.{TABLE_PATIENTS}`
    LIMIT 30
    """
    query_job = client.query(query)
    results = query_job.result()

    return render_template("patients.html", results=results)


# get paciente
@app.route('/rest/user/<int:subject_id>', methods=['GET'])
def get_patient(subject_id):
 
    query = f"""
        SELECT
            subject_id,
            gender,
            dob,
            image_url
        FROM `projetousa.MIMIC.{TABLE_PATIENTS}`
        WHERE subject_id = @subject_id
        """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("subject_id", "INT64", subject_id)
        ]
    )

    query_job = client.query(query, job_config=job_config)

    try:
        results = query_job.result(timeout=30)
    except concurrent.futures.TimeoutError:
        return flask.render_template("timeout.html", job_id=query_job.job_id)

    return flask.render_template("patient.html", results=results)


#adicionar um paciente
@app.route('/rest/user', methods=['POST'])
def create_patient():
    data = request.get_json()
 
    query = f"""
    INSERT INTO `{TABLE_REF}.{TABLE_PATIENTS}` (SUBJECT_ID, GENDER, DOB)
    VALUES (@subject_id, @gender, @dob)
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("subject_id", "INT64", data["subject_id"]),
            bigquery.ScalarQueryParameter("gender", "STRING", data["gender"]),
            bigquery.ScalarQueryParameter("dob", "TIMESTAMP", data["dob"])
        ]
    )
    query_job = client.query(query, job_config=job_config)
    query_job.result()

    return jsonify({"message": "Patient created successfully!"}), 201


#atualizar informacoes do paciente
@app.route('/rest/user/<int:subject_id>', methods=['PUT'])
def update_patient(subject_id):
    data = request.get_json()

    query = f"""
    UPDATE `{TABLE_REF}.{TABLE_PATIENTS}`
    SET GENDER = @gender, DOB = @dob
    WHERE SUBJECT_ID = @subject_id
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("gender", "STRING", data["gender"]),
            bigquery.ScalarQueryParameter("dob", "TIMESTAMP", data["dob"]),
            bigquery.ScalarQueryParameter("subject_id", "INT64", subject_id)
        ]
    )

    query_job = client.query(query, job_config=job_config)
    query_job.result()

    return jsonify({"message": f"Pacient {subject_id} updated successfully!"})


#remover paciente
DELETED_USER_ID = -1

@app.route('/rest/user/<int:subject_id>', methods=['DELETE'])
def delete_patient(subject_id):
    update_admissions_query = f"""
    UPDATE `{TABLE_REF}.{TABLE_ADMISSIONS}`
    SET SUBJECT_ID = @deleted_user_id
    WHERE SUBJECT_ID = @subject_id
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("deleted_user_id", "INT64", DELETED_USER_ID),
            bigquery.ScalarQueryParameter("subject_id", "INT64", subject_id),
        ]
    )
    client.query(update_admissions_query, job_config=job_config).result()

    update_progress_query = f"""
    UPDATE `{TABLE_REF}.{TABLE_INPUT}`
    SET SUBJECT_ID = @deleted_user_id
    WHERE SUBJECT_ID = @subject_id
    """
    client.query(update_progress_query, job_config=job_config).result()

    update_labevents_query = f"""
    UPDATE `{TABLE_REF}.{TABLE_EVENTS}`
    SET SUBJECT_ID = @deleted_user_id
    WHERE SUBJECT_ID = @subject_id
    """
    client.query(update_labevents_query, job_config=job_config).result()

    delete_query = f"""
    DELETE FROM `{TABLE_REF}.{TABLE_PATIENTS}`
    WHERE SUBJECT_ID = @subject_id
    """
    client.query(delete_query, job_config=job_config).result()

    return jsonify({"message": f"Patient {subject_id} deleted successfully!"})

#---------------------------------------------------------------------------------------------------

# Create Admission

@app.route('/rest/admissions', methods=['POST'])
def create_admission():
    data = request.get_json()
 
    query = f"""
    INSERT INTO `{TABLE_REF}.{TABLE_ADMISSIONS}` (SUBJECT_ID, HADM_ID, ADMITTIME, ADMISSION_LOCATION)
    VALUES (@subject_id, @hadm_id, @admittime, @admission_location)
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("subject_id", "INT64", data["subject_id"]),
            bigquery.ScalarQueryParameter("hadm_id", "INT64", data["hadm_id"]),
            bigquery.ScalarQueryParameter("admittime", "TIMESTAMP", data["admittime"]),
            bigquery.ScalarQueryParameter("admission_location", "STRING", data["admission_location"])
        ]
    )
    query_job = client.query(query, job_config=job_config)
    query_job.result()

    return jsonify({"message": "Admission created successfully!"}), 201

# Update Medical Event 

@app.route('/rest/admissions/<int:hadm_id>', methods=['PUT'])
def update_admission(hadm_id):
    data = request.get_json()

    dischtime = data.get("dischtime")
    deathtime = data.get("deathtime")

    if deathtime:
        dischtime = deathtime

    death_param = deathtime if deathtime else None

    query = f"""
    UPDATE `{TABLE_REF}.{TABLE_ADMISSIONS}`
    SET 
        DISCHTIME = @dischtime,
        DEATHTIME = @deathtime
    WHERE HADM_ID = @hadm_id
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("dischtime", "TIMESTAMP", dischtime),
            bigquery.ScalarQueryParameter("deathtime", "TIMESTAMP", death_param),
            bigquery.ScalarQueryParameter("hadm_id", "INT64", hadm_id)
        ]
    )

    query_job = client.query(query, job_config=job_config)
    query_job.result()

    return jsonify({"message": f"Admission {hadm_id} updated successfully!"})


#---------------------------------------------------------------------------------------------------

# Create a question 

@app.route('/rest/questions', methods=['POST'])
def create_question():
    data = request.get_json()
 
    query = f"""
    INSERT INTO `{TABLE_REF}.{TABLE_QUESTIONS}` (Message, ID, Patient_ID)
    VALUES (@message, @id, @patient_id)
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("message", "STRING", data["message"]),
            bigquery.ScalarQueryParameter("id", "INT64", data["id"]),
            bigquery.ScalarQueryParameter("patient_id", "INT64", data["patient_id"])
        ]
    )
    query_job = client.query(query, job_config=job_config)
    query_job.result()

    return jsonify({"message": "Question created successfully!"}), 201

# Reply to question

@app.route('/rest/answers', methods=['POST'])
def create_answer():
    data = request.get_json()
 
    query = f"""
    INSERT INTO `{TABLE_REF}.{TABLE_ANSWERS}` (Message, Replying_To, Unit_ID)
    VALUES (@message, @replying_to, @unit_id)
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("message", "STRING", data["message"]),
            bigquery.ScalarQueryParameter("replying_to", "INT64", data["replying_to"]),
            bigquery.ScalarQueryParameter("unit_id", "STRING", data["unit_id"])
        ]
    )
    query_job = client.query(query, job_config=job_config)
    query_job.result()

    return jsonify({"message": "Question answered successfully!"}), 201

# List the questions

@app.route('/questions')
def questions():
    query_job = client.query(
        f"""
        SELECT * FROM `projetousa.MIMIC.QUESTIONS` AS questions 
        LEFT JOIN `projetousa.MIMIC.ANSWERS` as answers 
        ON questions.ID = answers.Replying_To
        """
    )

    try:
        results = query_job.result(timeout=30)
    except concurrent.futures.TimeoutError:
        return flask.render_template("timeout.html", job_id=query_job.job_id)

    return flask.render_template("questions.html", results=results)


#---------------------------------------------------------------------------------------------------
# BLOBSTORAGE

# This datastore model keeps track of uploaded photos.
class PhotoUpload(ndb.Model):
    blob_key = ndb.BlobKeyProperty()


# [START gae_blobstore_handler_flask]
class PhotoUploadHandler(blobstore.BlobstoreUploadHandler):
    def post(self):
        upload = self.get_uploads(request.environ)[0]
        photo = PhotoUpload(blob_key=upload.key())
        photo.put()

        return redirect("/view_photo/%s" % upload.key())


class ViewPhotoHandler(blobstore.BlobstoreDownloadHandler):
    def get(self, photo_key):
        if not blobstore.get(photo_key):
            return "Photo key not found", 404
        else:
            headers = self.send_blob(request.environ, photo_key)

            # Prevent Flask from setting a default content-type.
            # GAE sets it to a guessed type if the header is not set.
            headers["Content-Type"] = None
            return "", headers


@app.route("/view_photo/<photo_key>")
def view_photo(photo_key):
    """View photo given a key."""
    return ViewPhotoHandler().get(photo_key)


@app.route("/upload_photo", methods=["POST"])
def upload_photo():
    """Upload handler called by blobstore when a blob is uploaded in the test."""
    return PhotoUploadHandler().post()


# [END gae_blobstore_handler_flask]


@app.route("/media")
def upload():
    """Create the HTML form to upload a file."""
    upload_url = blobstore.create_upload_url("/upload_photo")

    response = """
  <html><body>
  <form action="{}" method="POST" enctype="multipart/form-data">
    Upload File: <input type="file" name="file"><br>
    <input type="submit" name="submit" value="Submit Now">
  </form>
  </body></html>""".format(
        upload_url
    )

    return response


#---------------------------------------------------------------------------------------------------

#PROGRESS

@app.route('/rest/progress/interventions', methods=['GET'])
def get_interventions():
   
    query = f"""
    SELECT starttime, 
        endtime, 
        itemid, 
        amount, 
        amountuom, 
        rate, 
        rateuom
    FROM `{TABLE_REF}.{TABLE_INPUT}`
    ORDER BY starttime DESC
    LIMIT 15
    """

    query_job = client.query(query)
    results_input = query_job.result()

    return render_template("interventions.html", results=results_input)


@app.route('/rest/progress/labs', methods=['GET'])
def get_lab_results():
    
    query = f"""
    SELECT charttime, 
        itemid, 
        value, 
        valuenum, 
        valueuom
    FROM `{TABLE_REF}.{TABLE_EVENTS}`
    ORDER BY charttime DESC
    LIMIT 15
    """

    query_job = client.query(query)
    results_lab = query_job.result()

    return render_template("labs.html", results=results_lab)


@app.route('/rest/progress/<int:subject_id>', methods=['GET'])
def get_patient_progress(subject_id):
    query = f"""
    SELECT starttime, 
        endtime, 
        itemid, 
        amount, 
        amountuom, 
        rate, 
        rateuom
    FROM `{TABLE_REF}.{TABLE_INPUT}`
    WHERE subject_id = @subject_id
    ORDER BY starttime DESC
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("subject_id", "INT64", subject_id)]
    )

    query_job = client.query(query, job_config=job_config)
    results_input = query_job.result()

    query = f"""
    SELECT charttime, 
        itemid, 
        value, 
        valuenum, 
        valueuom
    FROM `{TABLE_REF}.{TABLE_EVENTS}`
    WHERE subject_id = @subject_id
    ORDER BY charttime DESC
    """

    query_job = client.query(query, job_config=job_config)
    results_lab = query_job.result()

    return render_template("progress.html", results_input=results_input, results_lab=results_lab)


#---------------------------------------------------------------------------------------------------
# PATIENTS WITH LONGEST WAITING TIMES

@app.route('/rest/patients/longest_waiting', methods=['GET'])
def get_longest_waiting_patients():
    query = f"""
    SELECT SUBJECT_ID, HADM_ID, ADMITTIME, 
    TIMESTAMP_DIFF(COALESCE(DISCHTIME, CURRENT_TIMESTAMP()), ADMITTIME, HOUR) AS WAITING_HOURS

    FROM `{TABLE_REF}.{TABLE_ADMISSIONS}`
    ORDER BY WAITING_HOURS DESC
    LIMIT 10
    """
    query_job = client.query(query)
    results = query_job.result()

    return render_template("longest_waiting.html", results=results)

#---------------------------------------------------------------------------------------------------
# GOOGLE CLOUD FUNCTION 

CLOUD_FUNCTION_URL = "https://us-west4-projetousa.cloudfunctions.net/update_statistics"

@app.route("/update_stats")
def update_stats():
    response = requests.get(CLOUD_FUNCTION_URL)
    return jsonify(response.json())

if __name__ == '__main__':
    app.run(debug=True)