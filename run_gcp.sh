# Copyright 2021 Google LLC
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

gcloud dataflow flex-template run "twitter-beam-`date +%Y%m%d-%H%M%S`" \
    --template-file-gcs-location "$TEMPLATE_PATH" \
    --parameters input="gs://$GCP_BUCKET/input/logs.txt" \
    --parameters output="gs://$GCP_BUCKET/output/rejects.txt" \
    --parameters bqproject=$GCP_PROJECT \
    --parameters bqdataset=$BQ_DATASET \
    --parameters temp_location="gs://$GCP_BUCKET/tmp/" \
    --region "$REGION" \
    --project $GCP_PROJECT
