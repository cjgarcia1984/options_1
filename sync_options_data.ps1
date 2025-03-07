# PowerShell script to sync database file
$source = "\\192.168.50.100\options_data\"  # Adjust the source path
$destination = "C:\options_data\"

# Ensure the destination directory exists
if (-Not (Test-Path -Path $destination)) {
    New-Item -ItemType Directory -Path $destination
}

# Execute Robocopy to sync the file
Robocopy $source $destination * /Z /W:5 /R:5
