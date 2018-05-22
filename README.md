# getree

## Install

```
$ sudo apt install python3-dateutil python3-yaml
$ pip install flask
$ git clone http://github.com/dvolk/getree
$ cd getree
$ cp getree.yaml.example getree.yaml
edit getree.yaml
$ sqlite3 getree.sqlite
CREATE TABLE queue (sample_guid, run_uuid, status, elephantwalkurl, reference, distance, quality, epoch_added);
CREATE TABLE complete (sample_guid, run_uuid, elephantwalkurl, reference, distance, quality, epoch_added, epoch_start, epoch_end, neighbours, tree);
$ FLASK_APP=main.py flask run -h 0.0.0.0
```
## API

`<>` parameters are mandatory. Query parameters (`?...`) are optional. Default query parameters are shown below.

### `/neighbours/<guid>?reference=R00000039&distance=20&quality=0.08`

return json array of neighbours from database or add to run queue if not in database

### `/tree/<guid>?reference=R00000039&distance=20&quality=0.08`

return tree from database or add to run queue if not in database
  
### `/queue`

get json array of queue
  
### `/complete`

get json array of complete runs
  
### `/status`

human-readable status page

### `/new_run?guid=<guid>&reference=R00000039&distance=20&quality=0.08`

Same as `/tree` but the guid is a query parameter
