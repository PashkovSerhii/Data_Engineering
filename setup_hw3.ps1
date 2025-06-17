Write-Host "---- Starting AdTech Analytics Environment Setup ----" -ForegroundColor Cyan

# Check Docker
if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    Write-Host "Docker is not installed. Please install Docker first." -ForegroundColor Red
    exit 1
}
if (-not (Get-Command docker-compose -ErrorAction SilentlyContinue)) {
    Write-Host "Docker Compose is not installed." -ForegroundColor Red
    exit 1
}

# Create required directories
$directories = @("sql", "data/raw", "data/processed", "screenshots", "output")
foreach ($dir in $directories) {
    if (-not (Test-Path $dir)) {
        New-Item -ItemType Directory -Path $dir | Out-Null
    }
}
Write-Host "Required directories created." -ForegroundColor Green

# Start containers (MySQL, MongoDB, mongo-express, phpMyAdmin)
Write-Host "Starting docker-compose stack (MySQL, MongoDB, Admin UIs)..." -ForegroundColor Cyan
docker-compose up -d mysql mongo mongo-express phpmyadmin

# Додаткова затримка для нормального старту сервісів
Start-Sleep -Seconds 35

# Перевірка MySQL
$result = docker exec adtech_mysql mysql -uroot -ppassword -e "SELECT 'MySQL is running!' as status;" 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-Host "ERROR: MySQL is not started" -ForegroundColor Red
    exit 1
} else {
    Write-Host "MySQL started successfully!" -ForegroundColor Green
}

# Перевірка готовності MongoDB через TCP-порт
Start-Sleep -Seconds 10  # невелика дод. пауза після початкової
function Test-Mongo-Port {
    try {
        $tcp = New-Object System.Net.Sockets.TcpClient("localhost", 27017)
        $tcp.Close()
        return $true
    } catch {
        return $false
    }
}

if (Test-Mongo-Port) {
    Write-Host "MongoDB is up and accepting TCP connections on port 27017!" -ForegroundColor Green
} else {
    Write-Host "ERROR: MongoDB port 27017 is not open" -ForegroundColor Red
    exit 1
}

# Перевірка mongo-express та phpmyadmin у браузері (тільки повідомлення)
Write-Host "Mongo Express should be available at http://localhost:8081" -ForegroundColor Yellow
Write-Host "phpMyAdmin should be available at http://localhost:8080" -ForegroundColor Yellow

## ETL: MySQL load
#Write-Host "Running transforms_data.py for MySQL load..." -ForegroundColor Cyan
#docker-compose run --rm adtech_python python transforms_data.py
#
## Preview MySQL data (example: first 5 campaigns)
#Write-Host "`nFirst 5 records from campaigns table:" -ForegroundColor Yellow
#docker exec adtech_mysql mysql -u adtech_user -padtech_pass --table -e "USE adtech_db; SELECT * FROM campaigns LIMIT 5;"

# Load engagement data into MongoDB
Write-Host "Running load_to_mongodb.py for MongoDB user engagement load..." -ForegroundColor Cyan
docker-compose run --rm adtech_python python load_to_mongodb.py

# Run analytics/report queries on MongoDB
Write-Host "Running analyze_mongo.py for MongoDB analytics results..." -ForegroundColor Cyan
docker-compose run --rm adtech_python python analyze_mongo.py

Write-Host "`nAll ETL and analytics jobs complete." -ForegroundColor Green
Write-Host "`nAdmin UIs: " -ForegroundColor Yellow
Write-Host " - Mongo Express: http://localhost:8081" -ForegroundColor Yellow
Write-Host " - phpMyAdmin:    http://localhost:8080" -ForegroundColor Yellow

Write-Host "`nResults can be found (if workflow succeeded): output/  or current working directory (results as CSV/JSON)." -ForegroundColor Green
Write-Host "`nYou can find screenshots and intermediate files in the 'screenshots' and 'output' folders." -ForegroundColor Green

Write-Host "---- Setup HW3 completed! ----" -ForegroundColor Cyan