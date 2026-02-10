#!/bin/bash


# -------------------------------------------------
# DISK DEFINITIONS
# Format:
#   diskID:pool:dataset:keyfile
# -------------------------------------------------

DISKS=(
  "usb1:usb-backup:backup:/mnt/Apps/usb-backup/dataset_usb-backup_keys.json"
  "usb2:usb-backup2:backup:/mnt/Apps/usb-backup/dataset_usb-backup2_keys.json"
)

# -------------------------------------------------
# SOURCES PER DISK
# Format:
#   SOURCES_diskID=(
#   "/path/to/directory"
#   "/path/to/directory2"
#   "/path/to/directory3"
#   "..."
#   )
# -------------------------------------------------

SOURCES_usb1=(

  "/mnt/Storage/Videos"
  "/mnt/Storage/Documents"
)

SOURCES_usb2=(

  "/mnt/Storage/Videos"
  "/mnt/Storage/Documents"
)

# -------------------------------------------------
# Main Code
# -------------------------------------------------
if [ $# -ne 1 ]; then
    echo "Usage: {backup|backup-verbose|no-export|import-only|export-only|import-export|auto-start}"
    exit 1
fi

disk=""
pool=""
dataset=""
key=""
MODE="$1"

main() {

local backup="false"
local import="false"
local export="false"
local verbose="false"

console_bip "1"

case "$MODE" in
    backup)
    backup="true"
    import="true"
    export="true"
	  ;;
    backup-verbose)
    backup="true"
    import="true"
    export="true"
    verbose="true"
	  ;;
    import-export)
    import="true"
    export="true"
    ;;
    no-export)
    backup="true"
    import="true"
    ;;
    import-only)
    import="true"    
    ;;
    export-only)
    export="true"    
    ;;
    auto-start)
    echo "Code in Preparation"
    exit 0
    ;;
    *)
        echo "No valid argument. Stoping"
        exit 1
        ;;
esac

echo "$(date '+%Y-%m-%d %H:%M:%S') [Info] Starting USB Backup script" >&2

if [[ "$import" == "true" ]]; then
  echo "Mounting disk(s)" 
  console_bip "1"

  for i in "${!DISKS[@]}"; do
    get_disk_info "$i"
    pool_import "$pool"
    pool_unlock "$disk" "$pool" "$dataset" "$key"
  done
fi

if [[ "$backup" == "true" ]]; then
  for i in "${!DISKS[@]}"; do
    get_disk_info "$i"
    current_array="SOURCES_$disk"
    echo ""
    echo "Starting Backup on $pool ($disk)."
    echo ""
    if [[ ! -d "/mnt/$pool" || $(mount | grep -c "/mnt/$pool") -eq 0 ]]; then
      echo "$(date '+%Y-%m-%d %H:%M:%S') [Warning] Pool $pool ($disk) is not mounted. Skipped !" >&2
      echo ""
      continue
    fi  
    console_bip "5"
    eval "ARRAY=(\"\${${current_array}[@]}\")"
    for SRC in "${ARRAY[@]}"; do
      mkdir -p "/mnt/$pool/$dataset/${SRC#/mnt/}"
      if [[ "$verbose" == "true" ]]; then
        backup "$SRC" "/mnt/$pool/$dataset/${SRC#/mnt/}" "true"
        else
        backup "$SRC" "/mnt/$pool/$dataset/${SRC#/mnt/}" "false"
      fi
    done
    echo ""
    ZFS_scrub $disk $pool $dataset
  done
fi

if [[ "$export" == "true" ]]; then
  echo ""
  echo "Unmounting disk(s)"
  console_bip "2"
  for i in "${!DISKS[@]}"; do
    get_disk_info "$i"
    pool_export "$pool"
  done
	else
	echo "Pool left mounted."
fi

echo "$(date '+%Y-%m-%d %H:%M:%S') [Info] Finished USB Backup script" >&2
console_bip "8"

exit 0
}


# -------------------------------------------------
# functions
# -------------------------------------------------

# Parse disk info
get_disk_info() {
  local index="$1"

  if [ -z "${DISKS[$index]}" ]; then
    echo "DISKS[$index] is empty or not set"
    return 1
  fi

  IFS=":" read -r disk pool dataset key <<< "${DISKS[$index]}"

  return 0
}

console_bip() {
  local bip="${1:-1}"
  local bip_sleep="${2:-0.2}"

  [[ "$EUID" -ne 0 ]] && return 0 #Bip is available only in root mode...

  for ((i=1; i<=bip; i++)); do
    echo -e "\a" > /dev/console
    sleep $bip_sleep
  done
}

# -------------------
# Pool Import

#Import function
pool_import() {
    local pool_name="$1"
    local state job_status pool job_id found_pool imported_pool

    job_id=$(midclt call pool.import_find)
    
    while true; do
      job_status=$(midclt call core.get_jobs '[["id", "=", '"$job_id"']]' | jq -r '.[0]')
      state=$(echo $job_status | jq -r '.state')
      echo -n "."
      if [ "$state" == "SUCCESS" ]; then
          echo ""
          echo "Finished scan"
          break
      elif [ "$state" == "FAILED" ]; then
          echo ""
          echo "Failed Scan"
          return 1
      fi
      sleep 1
    done  
    echo ""

    pools=$(echo $job_status | jq -r '.result')

    echo "$pools" | jq -c '.[]' | while read -r pool; do
        name=$(echo "$pool" | jq -r '.name')
        guid=$(echo "$pool" | jq -r '.guid')
            import_job_id=$(midclt call pool.import_pool "{\"guid\": \"$guid\"}")

            # Poll until the import job is complete
            while true; do
                echo -n "."
                import_job_status=$(midclt call core.get_jobs '[["id", "=", '"$import_job_id"']]' | jq -r '.[0]')
                import_state=$(echo $import_job_status | jq -r '.state')
                if [ "$import_state" == "SUCCESS" ]; then
                      echo ""
                      echo "$name imported"
                    break
                elif [ "$import_state" == "FAILED" ]; then
                      echo ""
                      echo "Failed import"
                    return 1
                fi
                sleep 1
            done
    done

    if midclt call pool.query | jq -e --arg name "$pool_name" \
        '.[] | select(.name == $name)' >/dev/null; then
        echo "Successfully imported pool: $pool_name"
        return 0
    else
        echo "Pool with name $pool_name not found or not imported"
        return 1
    fi
    echo ""

}


# -------------------
# Pool Export

#Export function
pool_export() {
    local pool_name="$1"
    local pool_id export_job_id export_job_status

    if midclt call pool.query | jq -e --arg name "$pool_name" \
        '.[] | select(.name == $name)' >/dev/null; then
        echo "Mounted pool: $pool_name"
    else
        echo "Pool with name $pool_name not found or not imported"
        return 1
    fi


    echo "Starting export"  
    pool_id=$(midclt call pool.query '[["name","=","'"$pool_name"'"]]' | jq -r '.[0].id')
    export_job_id=$(midclt call pool.export "$pool_id")

    while true; do
        export_job_status=$(midclt call core.get_jobs "[[\"id\", \"=\", ${export_job_id}]]" | jq -r '.[0]')
        export_state=$(echo $export_job_status | jq -r '.state')
        echo -n "."

        if [ "$export_state" == "SUCCESS" ]; then
      echo ""
            echo "Export job succeeded."
            break
        elif [ "$export_state" == "FAILED" ]; then
            echo ""
      echo "Export job failed"
            return 1
        fi
        sleep 1
    done

    # After export, double-check if the pool is no longer available
    if midclt call pool.query | jq -e --arg name "$pool_name" \
        '.[] | select(.name == $name)' >/dev/null; then
        echo "Pool still mounted"
    else
        echo "Pool with name $pool_name successfully exported"
        return 1
    fi

}

# -------------------
# Backup

#Pool Unlock
pool_unlock() {
  local DISK="$1"
  local POOL_NAME="$2"
  local DATASET="$3"
  local POOL_KEY="$4"

  local MOUNT_POINT="/mnt/$POOL_NAME"
  local MAX_RETRIES=10
  local SLEEP_SEC=2
  local RETRIES=0
    
    echo ""
    if midclt call pool.query | jq -e --arg name "$POOL_NAME" \
        '.[] | select(.name == $name)' >/dev/null; then
        echo "Unlocking pool: $POOL_NAME"
    else
        echo "$POOL_NAME not found or not imported"
        return 1
    fi



  #  Check if pool is mounted
  if [[ -d "$MOUNT_POINT" && $(mount | grep -c "$MOUNT_POINT") -gt 0 ]]; then
    echo "Pool $POOL_NAME is already mounted."
    return 0
  fi

  #  Check if pool is unlocked
  local LOCKED_STATUS
  LOCKED_STATUS=$(midclt call pool.dataset.query \
    | jq -r ".[] | select(.pool==\"$POOL_NAME\") | .locked")

  if [[ $(echo "$LOCKED_STATUS" | grep -v false | wc -l) -eq 0 ]]; then
    echo "Pool $POOL_NAME is already unlocked."
    return 0
  fi

  #  Unlock pool
  jq -c 'to_entries[] | {datasets: [{name: .key, passphrase: .value}]}' "$POOL_KEY" |
  while read -r DATA; do
    local NAME
    NAME=$(jq -r '.datasets[0].name' <<< "$DATA")
    midclt call pool.dataset.unlock "$NAME" "$DATA" >/dev/null 2>&1
  done

  # Wait until unlocked
  until {
    LOCKED_STATUS=$(midclt call pool.dataset.query \
      | jq -r ".[] | select(.pool==\"$POOL_NAME\") | .locked")
    [[ $(echo "$LOCKED_STATUS" | grep -v false | wc -l) -eq 0 ]]
  }; do
    RETRIES=$((RETRIES + 1))
    if (( RETRIES > MAX_RETRIES )); then
      echo "$(date '+%Y-%m-%d %H:%M:%S') [ERROR] Pool $POOL_NAME is still locked after $((MAX_RETRIES * SLEEP_SEC)) seconds." >&2
      return 1
    fi
    echo "Waiting for pool to unlock... ($RETRIES/$MAX_RETRIES)"
    sleep "$SLEEP_SEC"
  done

  echo "Pool $POOL_NAME is unlocked."
  return 0
}

#Backup Script
backup() {
    local SRC="$1"
    local DEST="$2"
    local VERBOSE="$3"

    # Ensure script is run as root
    if [ "$(id -u)" -ne 0 ]; then
      echo ""
      echo "$(date '+%Y-%m-%d %H:%M:%S') [ERROR] This script must be run as root" >&2
      echo ""
      return 1
    fi

    # Build base rsync options
    local RSYNC_OPTS=(-a --delete --stats)

    if [[ "$VERBOSE" == "true" ]]; then
      # Verbose mode with progress
      RSYNC_OPTS+=(-v --progress)
      echo "running rsync "${RSYNC_OPTS[@]}" "$SRC/" "$DEST/""
      rsync "${RSYNC_OPTS[@]}" "$SRC/" "$DEST/"
    else
      # Quiet mode, only show final added size
      BACKUP_STATS=$(rsync "${RSYNC_OPTS[@]}" "$SRC/" "$DEST/" 2>/dev/null)

      # Total added/updated size
      BACKUP_SIZE=$(awk -F': ' '/Total transferred file size/ {print $2}' <<< "$BACKUP_STATS")
      BACKUP_SIZE=${BACKUP_SIZE:-0}  # Default to 0 if empty
      HR_SIZE=$(numfmt --to=iec --format="%.2f" <<< "${BACKUP_SIZE//,/}")

      # Total deleted files
      DELETED_FILES=$(awk -F': ' '/Number of deleted files/ {print $2}' <<< "$BACKUP_STATS")
      DELETED_FILES=${DELETED_FILES:-0}

      # Log the backup summary
      echo "[$(date '+%H:%M')] $SRC completed. Data added: $HR_SIZE, $DELETED_FILES files deleted" >&2


    fi

    return 0
}

ZFS_scrub() {

  local scrub_delay="27" #in days
  local scrub_file="/mnt/$pool/$dataset/.last_scrub"

  local now=$(date +%F)

  # Default: force scrub if file doesn't exist
  local diff_days=999

  if [ -f "$scrub_file" ]; then
    local last_scrub=$(cat "$scrub_file")
    # Calculate difference in days
    diff_days=$(( ( $(date -d "$now" +%s) - $(date -d "$last_scrub" +%s) ) / 86400 ))
  fi

  if [ "$diff_days" -ge $scrub_delay ]; then
    echo "$(date '+%Y-%m-%d %H:%M:%S') [WARNING] Starting to scrub $pool ($disk)" >&2
    midclt call pool.scrub.scrub $pool "START" >/dev/null 2>&1
    sleep 20
    local dots=0
    while true; do
        status=$(zpool status "$pool" | grep -i "scrub in progress")
        if [ -z "$status" ]; then
            echo -e "\n[$disk] Scrub finished on pool '$pool'"
            break
        else
            # Print rotating dots
            dots=$(( (dots % 3) + 1 ))
            line=""
            for ((i=0;i<dots;i++)); do
                line="${line}."
            done
            echo -ne "\r[$disk] Scrub running on $pool $line   "
            sleep 60  # check every 60 seconds
        fi
    done
    echo "$now" > "$scrub_file"
    echo "[$(date '+%H:%M')] Finished to scrub $pool ($disk)" >&2
  else
    echo "[$disk] Pool '$pool', dataset '$dataset' scrubed $diff_days days ago. Skipping."
  fi

return 0
}


# -------------------------------------------------
main 
