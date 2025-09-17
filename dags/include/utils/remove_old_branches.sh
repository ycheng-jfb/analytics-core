#!/usr/bin/env bash

usage() {
    USAGE="Usage:
    $0 [-d|--days [days]] [-o|--output [output_dir]] [-e|--exclude [exclusion_list]]
    [-ef|--excludefile /path/to/exclusion_list] [-r|--remove] [-n|--no-merged]
Options:
    -d  | --days        : branches with the latest commit older than this parameter will be removed
    -o  | --output      : location to write output tsv report and log file. \
The tsv report contains removed branches, last commit date, and branch owners
    -e  | --exclude     : list of branches to exclude from process
    -ef | --excludefile : path to file containing list of branches to exclude; \
-e and -ef are mutually exclusive
    -r  | --remove      : provide this flag to actually remove the branches from remote; \
the script will do a dryrun without this flag
    -n  | --no-merged   : include branches not merged to master; \
by default only merged branches are considered"
    echo "$USAGE"
    exit 1
}

# check if item ($1) exists in array ($2)
array_contains() {
    target=$1
    shift
    arr=("$@")
    for item in "${arr[@]}"
    do
        if [[ "$target" = "$item" ]]
        then
            echo 0
            return
        fi
    done
    echo 1
}

# function to list and remove branches older than $INTERVAL
remove_old_branches() {
    # $1 - $DRYRUN, $2 - $MERGED
    if [[ $2 = false ]]
    then
        >&2 echo "--no-merged flag provided, process will list/remove unmerged branches too"
    fi
    if [[ $1 = true ]]
    then
        >&2 echo "Dryrun, see tsv report for branches older than $DAYS days"
    else
        >&2 echo "Not a dryrun! Removing branches older than $DAYS days"
    fi
    declare GITREFS
    if [[ $2 = true ]]
    then
        GITREFS=$(git for-each-ref refs/remotes --merged=master --sort='authordate')
    else
        GITREFS=$(git for-each-ref refs/remotes --sort='authordate')
    fi
    while read -r commit _ ref
    do
        REF_NAME=$(git rev-parse --abbrev-ref "$ref")
        BRANCH_NAME="${REF_NAME/origin\//}"
        # branch owner -- first commit author
        BRANCH_OWNER=$(git log --pretty=%aN --reverse "$REF_NAME" | head -1)
        if [[ $(array_contains "$BRANCH_NAME" "${EXCLUDE_LIST[@]}") -eq 0 ]]
        then
            >&2 echo "excluding $BRANCH_NAME"
        else
            LAST_COMMIT_DT=$(git log -1 --pretty=%cd --date=format:%s "$commit")
            if [[ $((CURRENT-LAST_COMMIT_DT)) -ge ${INTERVAL} ]]
            then
                echo -e "$BRANCH_NAME\t$BRANCH_OWNER\t$(date -r "$LAST_COMMIT_DT" '+%Y-%m-%d')"
                if [[ $1 != true ]]
                then
                    >&2 echo "removing $BRANCH_NAME"
                    # remove branch in remote
                    git push origin :"$BRANCH_NAME" 1>&2
                else
                    >&2 echo "dryrunning $BRANCH_NAME"
                fi
            fi
        fi
    done < <(echo -e "${GITREFS}")
}

EXCLUDE_LIST=("develop") # array of branches to exclude
DAYS=365 # default number of days to check
OUTPUT_DIR="$HOME/Documents/" # default output report dir
DRYRUN=true # by default the script will only list branches older than $DAYS
MERGED=true # by default the script will only list merged branches older than $DAYS

# exit if both -e and -ef are provided
[[ $(array_contains '-e' "$@") -eq 0 || $(array_contains '--exclude' "$@") -eq 0 ]] && \
[[ $(array_contains '-ef' "$@") -eq 0 || $(array_contains '--excludefile' "$@") -eq 0 ]] && usage

# parse commandline args
while [[ "$#" -gt 0 ]]
do
    case $1 in
        -d|--days) [[ $2 -gt 0 ]] && DAYS=$2; shift ;;
        -o|--output) [[ -d $2 ]] && OUTPUT_DIR=$2; shift ;;
        -e|--exclude)
            while [[ $2 != -* && $# -gt 0 ]]
            do
                EXCLUDE_LIST+=( "$2" )
                shift
            done
            ;;
        -ef|--excludefile)
            if [[ ! -f $2 ]]
            then
                >&2 echo "Invalid exclusion file! $2"
                exit 1
            fi
            while IFS= read -r line
            do
                EXCLUDE_LIST+=("$line")
            done < "$2"
            shift
            ;;
        -r|--remove) DRYRUN=false ;;
        -n|--no-merged) MERGED=false ;;
        *)
            usage
    esac
    shift
done

# constants
SECONDS_PER_DAY=86400
CURRENT=$(date +%s)
INTERVAL=$(( DAYS * SECONDS_PER_DAY ))

OUTPUT_DIR="$(echo $OUTPUT_DIR | sed 's:/*$::')"

OUTFILE="$OUTPUT_DIR/git_branch_cleanup_$(date +%Y%m%d).tsv"
LOGFILE="$OUTPUT_DIR/git_branch_cleanup_$(date +%Y%m%d).log"
echo "Writing out to $OUTFILE, log to $LOGFILE"
echo -e "branch\tbranch_owner\tlast_commit_date" > "$OUTFILE"
remove_old_branches $DRYRUN $MERGED 2>"$LOGFILE" 1>>"$OUTFILE"
