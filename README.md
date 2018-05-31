# arboreta

## Install

```
$ sudo apt install python3-dateutil python3-yaml python3-matplotlib
$ pip install flask
$ git clone http://github.com/dvolk/arboreta
$ cd arboreta
$ cp arboreta.yaml.example arboreta.yaml
edit arboreta.yaml
$ sqlite3 arboreta.sqlite
CREATE TABLE queue (sample_guid, run_uuid, status, elephantwalkurl, reference, distance, quality, epoch_added);
CREATE TABLE complete (sample_guid, run_uuid, elephantwalkurl, reference, distance, quality, epoch_added, epoch_start, epoch_end, neighbours, tree);
CREATE TABLE neighbours (uuid primary key, samples, distance integer, reference, quality, elephantwalkurl, epoch_add, neighbours, neighbours_count integer);
$ FLASK_APP=main.py flask run -h 0.0.0.0
```
## API

`<>` parameters are mandatory. Query parameters (`?...`) are optional. Default query parameters are shown below.

### `/status`

human-readable status page

### `/neighbours/<guid>?reference=R00000039&distance=20&quality=0.80`

return json array of neighbours from database or add to run queue if not in database

### `/ndgraph/<guid>?reference=R00000039&quality=0.80`

return json array of pairs of (distance,neighbours)

### `/ndgraph2/<guid>?reference=R00000039&quality=0.80`

return json array of two arrays - distances and neighbours

### `/ndgraph3/<guid>?reference=R00000039&quality=0.80&cutoff=4`

return json array of two arrays - distances and neighbours

keeps getting neighbours until `cutoff` of distances have the same number of neighbours

### `/ndgraph.svg/<guid>?reference=R00000039&quality=0.80&cutoff=(no default)`

return svg plot of distance-neighbours points

### `/tree/<guid>?reference=R00000039&distance=20&quality=0.80`

return tree from database or add to run queue if not in database

### `/new_run?guid=<guid>&reference=R00000039&distance=20&quality=0.80`

Same as `/tree` but the guid is a query parameter

### `/queue`

get json array of queue
  
### `/complete`

get json array of complete runs
