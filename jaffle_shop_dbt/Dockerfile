<<<<<<< HEAD
ARG DBT_VERSION=0.19.2
=======
ARG DBT_VERSION=0.19.0
>>>>>>> 51b6e8ddc305cdbd235a7efbf8a7bec7bac0074c
FROM fishtownanalytics/dbt:${DBT_VERSION}

# ARG DBT_VERSION
# RUN set -ex \
#     && pip install dbt-clickhouse==${DBT_VERSION}

ENV DBT_PROFILES_DIR=.

# COPY profiles.yml profiles.yml    

ENTRYPOINT ["tail", "-f", "/dev/null"]