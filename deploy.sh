gcloud functions deploy update_statistics \
    --runtime python311 \
    --trigger-http \
    --allow-unauthenticated \
    --region=europe-southwest1 \
    --entry-point=update_statistics \
    --source=.


