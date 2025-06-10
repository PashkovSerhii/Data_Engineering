
Write-Host "Starting AdTech Database Setup" -ForegroundColor Cyan

# Check Docker
if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    Write-Host "Docker is not installed. Please install Docker first." -ForegroundColor Red
    exit 1
}

if (-not (Get-Command docker-compose -ErrorAction SilentlyContinue)) {
    Write-Host "Docker Compose is not installed." -ForegroundColor Red
    exit 1
}

# Create directories
$directories = @("sql", "data/raw", "data/processed", "screenshots")
foreach ($dir in $directories) {
    if (-not (Test-Path $dir)) {
        New-Item -ItemType Directory -Path $dir | Out-Null
    }
}

Write-Host "Required directories created" -ForegroundColor Green

# Start Docker containers
Write-Host "Starting MySQL container..." -ForegroundColor Cyan
docker-compose up -d mysql

## Wait for MySQL to start
#Write-Host "Waiting for MySQL to start (30 seconds)..." -ForegroundColor Cyan
#Start-Sleep -Seconds 30
#
## Check connection
#Write-Host "Checking MySQL connection..." -ForegroundColor Cyan
#$result = docker exec adtech_mysql mysql -uroot -ppassword -e "SELECT 'MySQL is running!' as status;" 2>&1
#
#if ($LASTEXITCODE -eq 0) {
#    Write-Host "MySQL started successfully!" -ForegroundColor Green
#    Write-Host "phpMyAdmin available at http://localhost:8080" -ForegroundColor Green
#    Write-Host "MySQL available at localhost:3306" -ForegroundColor Green
#} else {
#    Write-Host "Error connecting to MySQL" -ForegroundColor Red
#    Write-Host $result -ForegroundColor Red
#    exit 1
#}
#
#Write-Host "Setup completed successfully!" -ForegroundColor Green


# Стартуємо MySQL OK, чекаємо...
docker-compose up -d mysql
Start-Sleep -Seconds 30
# Перевіряємо MySQL
$result = docker exec adtech_mysql mysql -uroot -ppassword -e "SELECT 'MySQL is running!' as status;" 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-Host "ERROR: MySQL is not started" -ForegroundColor Red
    exit 1
}

Write-Host "MySQL started. Now running the Python data transform..." -ForegroundColor Cyan

# Стартуємо python-контейнер, одразу зі скриптом та requirements.txt
docker-compose up -d adtech_python

# (За потреби, можна запустити adtech_python не в detached режимі, щоб бачити лог)
Write-Host "Python transform script executed inside Docker. Check logs with:" -ForegroundColor Yellow
Write-Host "docker logs adtech_python" -ForegroundColor Yellow

Write-Host "Setup completed!"


# ===============================================
# Показати перші 10 записів з кожної таблиці
# ===============================================

Write-Host "`nShowing first 10 records from each table:" -ForegroundColor Cyan

Write-Host "`nAdvertisers table:" -ForegroundColor Yellow
docker exec adtech_mysql mysql -u adtech_user -padtech_pass --table -e "USE adtech_db; SELECT * FROM advertisers LIMIT 10;"

Write-Host "`nCampaigns table:" -ForegroundColor Yellow
docker exec adtech_mysql mysql -u adtech_user -padtech_pass --table -e "USE adtech_db; SELECT * FROM campaigns LIMIT 10;"

Write-Host "`nUsers table:" -ForegroundColor Yellow
docker exec adtech_mysql mysql -u adtech_user -padtech_pass --table -e "USE adtech_db; SELECT * FROM users LIMIT 10;"

Write-Host "`nUser_interests table:" -ForegroundColor Yellow
docker exec adtech_mysql mysql -u adtech_user -padtech_pass --table -e "USE adtech_db; SELECT * FROM user_interests LIMIT 10;"

Write-Host "`nAd_events table:" -ForegroundColor Yellow
docker exec adtech_mysql mysql -u adtech_user -padtech_pass --table -e "USE adtech_db; SELECT * FROM ad_events LIMIT 10;"

Write-Host "`nClicks table:" -ForegroundColor Yellow
docker exec adtech_mysql mysql -u adtech_user -padtech_pass --table -e "USE adtech_db; SELECT * FROM clicks LIMIT 10;"

Write-Host "`nSetup completed successfully!" -ForegroundColor Green

# =============================================
# Running Python analytics script and retrieving results
# =============================================

Write-Host "`nRunning analytics Python script homework2.py" -ForegroundColor Cyan

# If using the python container:
docker-compose run --rm adtech_python python homework2.py

# Wait a bit to ensure the file was created (or use a proper check)
Start-Sleep -Seconds 3

# Copy report.xlsx from the container to host
if ($pyContainer) {
    docker cp "/app/report.xlsx" "./report.xlsx"
    Write-Host "File report.xlsx copied to the host" -ForegroundColor Green
} else {
    Write-Host "adtech_python container not found! Make sure the volume is configured or the file is copied to /output!" -ForegroundColor Red
}

# If results are stored via volume (e.g. ./output:/app/output), just notify!
# Write-Host "File report.xlsx is available at ./output/report.xlsx" -ForegroundColor Green

# =============================================
# Output a quick preview from Excel in PowerShell
# (for example: show 10 rows from the first sheet of report.xlsx)
# =============================================

# Check file existence
if (Test-Path "./report.xlsx") {
    Write-Host "`nFirst 10 rows from the 'top_campaigns_by_ctr' sheet:" -ForegroundColor Cyan
    python -c @"
import pandas as pd
report = pd.read_excel('report.xlsx', sheet_name='top_campaigns_by_ctr')
print(report.head(10).to_string(index=False))
"@
    Write-Host "`nOK: Report is generated! See all data in ./report.xlsx (you can open it in Excel)." -ForegroundColor Green
} else {
    Write-Host "report.xlsx not found. Check execution logs!" -ForegroundColor Red
}

Write-Host "`nYou can also find results files in ./results_*.csv, ./results_*.json or in your output folders!" -ForegroundColor Cyan

Write-Host "`nAll steps completed!" -ForegroundColor Green