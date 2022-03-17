import os
from config import aws_config

# Get Sys arguments
# action = sys.argv[1]
# if action != 's':
    # csv_file = sys.argv[2]                  # Input CSV Path + CSV File as a Parameter

# Start boto3 session
profile = input("Environment : ")
action = input("Action to do in this run. Daily Sync (s)/list(l)/Grant(g)/Revoke(r) : ")
if action != 's':
    csv_file = input("Provide CSV File Path + CSV File Name : ")

# AWS Environment Configuration
if profile == 'stg':
    aws = aws_config.Staging()
    ### account to role, type and level mapping
    # Stg Environment
    lf_mapping = {
    }

if profile == 'prd':
    aws = aws_config.Production()
    ### account to role, type and level mapping
    #PROD Environment
    lf_mapping = {
    }
