# Initialize Project



## Run Project

```
docker-compose up --build -d
```

Visit [Airflow from localhost](localhost:8183)


## Load Variabke

1. Visit Admin > Variable [Link](http://localhost:8183/variable/list/)
2. Import `variables.json` to Airflow

docker container run -it -d -v /tmp:/tmp -v /var/run/docker.sock:/var/run/docker.sock -v /var/lib/docker/containers:/var/lib/docker/containers:ro -e ACCOUNT_UUID={7f9f4e4b-535f-453d-bfbe-30dc00012b46}  -e RUNNER_UUID={44ff6f69-7c10-54f0-b571-a40932da6896} -e RUNTIME_PREREQUISITES_ENABLED=true -e OAUTH_CLIENT_ID=VSxQMnDpZL13Yq4oZkS6M9hotZoPCiDJ -e OAUTH_CLIENT_SECRET=ATOAFXDlYOW-HSneZCpmEXR2B8ekoscDIFFwBV0zSK8O7vSZeReWYgTs5JLAg4x_x00j7DBA6628 -e WORKING_DIRECTORY=/tmp --name runner-44ff6f69-7c10-54f0-b571-a40932da6896 docker-public.packages.atlassian.com/sox/atlassian/bitbucket-pipelines-runner:1