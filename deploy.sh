gcloud functions deploy update_statistics \
    --runtime python311 \
    --trigger-http \
    --allow-unauthenticated \
    --region=us-west4 \
    --entry-point=update_statistics \
    --source=.


