#!/bin/zsh

# Set the working directory
cd "/home/shoaib/Desktop/workspace/python_automation_scripts_modernization" || exit

# Activate the virtual environment
source "venv/bin/activate"

# Initialize default values
mode="offline"
date_check=""
python_args=""
arglist=()

# Loop through all arguments to extract mode and date_check
while [[ $# -gt 0 ]]
do
    case "$1" in
        --mode)
            if [[ -n "$2" ]]; then
                mode="$2"
                shift 2
            else
                echo "Error: Missing value for --mode"
                exit 1
            fi
            ;;
        --date_check)
            if [[ -n "$2" ]]; then
                 -="$2"
                shift 2
            else
                echo "Error: Missing value for --date_check"
                exit 1
            fi
            ;;
        *)
            # Collect all remaining arguments as social_id
            arglist+=("$1")
            shift
            ;;
    esac
done

# Prepare the arguments to be passed based on optional parameters
python_args="--mode $mode"
if [[ -n "$date_check" ]]; then
    python_args="$python_args --date_check $date_check"
fi

# Loop through all remaining social_id arguments and process each
for social_id in "${arglist[@]}"
do
    echo "Processing social_id: $social_id"
    python "mention_created_based_on_social_id.py" "$social_id" $python_args
done

# Deactivate the virtual environment
deactivate





cd "/home/shoaib/Desktop/workspace/python_automation_scripts_modernization" || exit

# Activate the virtual environment
source "venv/bin/activate"
