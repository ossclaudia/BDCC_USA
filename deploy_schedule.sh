#gcloud config set project projetousa

gcloud scheduler jobs create http update-statistics-job \
    --schedule "*/10 * * * *" \
    --uri "https://us-west4-projetousa.cloudfunctions.net/update_statistics" \
    --http-method=GET \
    --time-zone="Europe/Lisbon" \
    --description="Updates admissions statistics every 10 minutes"