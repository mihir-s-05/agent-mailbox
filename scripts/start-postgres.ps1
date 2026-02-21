$ErrorActionPreference = "Stop"

$container = "agent-mailbox-postgres"
$exists = docker ps -a --format "{{.Names}}" | Where-Object { $_ -eq $container }
if (-not $exists) {
  docker run -d --name $container `
    -e POSTGRES_PASSWORD=postgres `
    -e POSTGRES_USER=postgres `
    -e POSTGRES_DB=agent_mailbox `
    -p 5432:5432 `
    postgres:16 | Out-Null
  Write-Host "Started new postgres container: $container"
} else {
  docker start $container | Out-Null
  Write-Host "Started existing postgres container: $container"
}

