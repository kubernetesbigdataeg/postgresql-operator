podman build . --tag docker.io/kubernetesbigdataeg/postgres:15.0.0-1
podman login docker.io
podman push docker.io/kubernetesbigdataeg/postgres:15.0.0-1