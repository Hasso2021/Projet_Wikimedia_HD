# Crée les topics Kafka Wikimedia (Windows PowerShell)
# Prérequis : docker compose up -d (Kafka healthy)

$Bootstrap = "localhost:9092"

$Topics = @(
    @{ Name = "wm.recentchange.raw"; Partitions = 3 },
    @{ Name = "wm.bot.events";      Partitions = 3 },
    @{ Name = "wm.page.edits";      Partitions = 3 },
    @{ Name = "wm.errors";          Partitions = 1 }
)

Write-Host "`n=== Creating Kafka topics ===`n" -ForegroundColor Cyan

foreach ($t in $Topics) {
    docker exec kafka kafka-topics `
        --create `
        --if-not-exists `
        --topic $t.Name `
        --bootstrap-server $Bootstrap `
        --partitions $t.Partitions `
        --replication-factor 1

    if ($LASTEXITCODE -eq 0) {
        Write-Host "[OK]   $($t.Name) ($($t.Partitions) partitions)" -ForegroundColor Green
    } else {
        Write-Host "[FAIL] $($t.Name)" -ForegroundColor Red
    }
}

Write-Host "`n--- Topic list ---"
docker exec kafka kafka-topics --list --bootstrap-server $Bootstrap

Write-Host "`n=== Done ===`n" -ForegroundColor Cyan
