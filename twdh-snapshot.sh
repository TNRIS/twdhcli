#!/bin/bash

export APIKEY=`cat ~/my-token-prod.txt`
export URL=https://txwaterdatahub.org
export DEST=~/twdh-snapshot

if [ -d ${DEST} ]; then
  echo "Found TWDH snapshot directory ${DEST}"
else
  mkdir ${DEST}
  echo "TWDH snapshot directory not found, created ${DEST}"
fi

export SNAPSHOT=`date '+%Y-%m-%dT%T.%3N'`

echo "Creating snapshot in ${DEST}/twdh-${SNAPSHOT}"
mkdir ${DEST}/twdh-${SNAPSHOT}


echo "Creating package_search JSON version of datasets.json ..."
curl "https://txwaterdatahub.org/api/action/package_search?fq=type:dataset&rows=1000&include_private=true&include_drafts=true&include_deleted=true" --silent -A "twdhcli/0.1" -H "Authorization: ${APIKEY}" | python -m json.tool > ${DEST}/twdh-${SNAPSHOT}/datasets.json
echo "Creating package_search JSON version of applications.json ..."
curl "https://txwaterdatahub.org/api/action/package_search?fq=type:application&rows=1000&include_private=true&include_drafts=true&include_deleted=true" --silent -A "twdhcli/0.1" -H "Authorization: ${APIKEY}" | python -m json.tool > ${DEST}/twdh-${SNAPSHOT}/applications.json

echo "Creating JSONL files using ckanapi dump"
for i in datasets groups organizations users; 
  do
    echo "... ${i} ..."
    ckanapi dump $i \
      --apikey=$APIKEY \
      --all \
      -O ${DEST}/twdh-${SNAPSHOT}/$i.jsonl \
      -r $URL
  done;
echo "... done!"
