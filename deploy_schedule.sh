#gcloud config set project barbara2-451412

gcloud scheduler jobs create http update-statistics-job \
    --schedule "*/10 * * * *" \
    --uri "https://europe-southwest1-barbara2-451412.cloudfunctions.net/update_statistics" \
    --http-method=GET \
    --time-zone="Europe/Lisbon" \
    --description="Updates admissions statistics every 10 minutes"