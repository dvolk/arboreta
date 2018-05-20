# getree

## API

### `/neighbours/<guid>[?reference=R00000039&distance=20&quality=0.08]`

return json array of neighbours from database or add to run queue if not in database

### `/tree/<guid>[?reference=R00000039&distance=20&quality=0.08]`

return tree from database or add to run queue if not in database
  
### `/queue`

get json array of queue
  
### `/complete`

get json array of complete runs
  
### `/status`

human-readable status page
