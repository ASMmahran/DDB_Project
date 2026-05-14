# test_cluster.ps1
# Automated test for the 3-node DDB cluster.
# Run this AFTER starting all three nodes.

$node1 = "http://localhost:8081"
$node2 = "http://localhost:8082"
$node3 = "http://localhost:8083"

function Post($url, $body) {
    try {
        $json = $body | ConvertTo-Json -Depth 5
        $r = Invoke-WebRequest -Uri $url -Method POST `
             -ContentType "application/json" -Body $json -ErrorAction Stop
        return $r.Content | ConvertFrom-Json
    } catch {
        return @{ error = $_.Exception.Message }
    }
}

function Status($url) {
    try {
        $r = Invoke-WebRequest -Uri "$url/node/status" -ErrorAction Stop
        return $r.Content | ConvertFrom-Json
    } catch { return @{ role = "offline" } }
}

function PrintSection($title) {
    Write-Host ""
    Write-Host ("=" * 55) -ForegroundColor Cyan
    Write-Host "  $title" -ForegroundColor Cyan
    Write-Host ("=" * 55) -ForegroundColor Cyan
}

function PrintResult($label, $obj) {
    $json = $obj | ConvertTo-Json -Depth 5
    if ($obj.error) {
        Write-Host "  [FAIL] $label" -ForegroundColor Red
        Write-Host "         $json"
    } else {
        Write-Host "  [ OK ] $label" -ForegroundColor Green
        Write-Host "         $json"
    }
}

# ── 0. Cluster status ─────────────────────────────────────────────────────────
PrintSection "0. Cluster Status"
@($node1, $node2, $node3) | ForEach-Object {
    $s = Status $_
    $color = if ($s.role -eq "Master") { "Green" } elseif ($s.role -eq "Slave") { "Cyan" } else { "Red" }
    Write-Host "  Node $($s.node_id)  role=$($s.role)  leader=$($s.leader_id)  term=$($s.term)" -ForegroundColor $color
}

# ── 1. Create database on Master ──────────────────────────────────────────────
PrintSection "1. Create Database  (→ Master)"
$r = Post "$node1/db/create" @{ db = "school" }
PrintResult "Create DB 'school'" $r

# ── 2. Create table on Master ─────────────────────────────────────────────────
PrintSection "2. Create Table  (→ Master)"
$r = Post "$node1/table/create" @{
    db         = "school"
    table      = "students"
    attributes = @("name","age","email")
}
PrintResult "Create table 'students'" $r

# ── 3. Insert via Master ──────────────────────────────────────────────────────
PrintSection "3. Insert Records  (→ Master)"
$r = Post "$node1/query/insert" @{
    db = "school"; table = "students"
    record = @{ name = "Alice"; age = "20"; email = "alice@test.com" }
}
PrintResult "Insert Alice (via master)" $r

# ── 4. Insert via Slave (write-forwarding) ────────────────────────────────────
PrintSection "4. Insert via Slave  (auto-forwarded to Master)"
Start-Sleep -Seconds 1   # give replication a moment
$r = Post "$node2/query/insert" @{
    db = "school"; table = "students"
    record = @{ name = "Bob"; age = "22"; email = "bob@test.com" }
}
PrintResult "Insert Bob (via slave node2)" $r

# ── 5. Select from both slaves ────────────────────────────────────────────────
PrintSection "5. Select from Slaves  (tests replication)"
Start-Sleep -Seconds 1
$r = Post "$node2/query/select" @{ db = "school"; table = "students"; query = @{} }
PrintResult "Select all (node2)" $r

$r = Post "$node3/query/select" @{ db = "school"; table = "students"; query = @{} }
PrintResult "Select all (node3)" $r

# ── 6. Update via Slave ───────────────────────────────────────────────────────
PrintSection "6. Update via Slave"
$r = Post "$node2/query/update" @{
    db = "school"; table = "students"
    query  = @{ name = "Alice" }
    update = @{ age  = "21" }
}
PrintResult "Update Alice age→21 (via node2)" $r

# ── 7. Delete via Master ──────────────────────────────────────────────────────
PrintSection "7. Delete via Master"
$r = Post "$node1/query/delete" @{
    db = "school"; table = "students"
    query = @{ name = "Bob" }
}
PrintResult "Delete Bob (via master)" $r

# ── 8. Verify count ───────────────────────────────────────────────────────────
PrintSection "8. Final Select — should show 1 row (Alice)"
Start-Sleep -Seconds 1
$r = Post "$node3/query/select" @{ db = "school"; table = "students"; query = @{} }
PrintResult "Select all (node3)" $r

# ── 9. Slave cannot drop database ─────────────────────────────────────────────
PrintSection "9. Access Control — Slave cannot Drop DB"
$r = Post "$node2/db/drop" @{ db = "school" }
if ($r.error) {
    Write-Host "  [ OK ] Correctly rejected: $($r.error)" -ForegroundColor Green
} else {
    Write-Host "  [FAIL] Slave was allowed to drop DB!" -ForegroundColor Red
}

# ── 10. Raw SQL ───────────────────────────────────────────────────────────────
PrintSection "10. Raw SQL"
$r = Post "$node1/query/raw" @{ db = "school"; sql = "SELECT * FROM students" }
PrintResult "Raw SELECT (master)" $r

# ── 11. Drop database cleanup ─────────────────────────────────────────────────
PrintSection "11. Cleanup — Drop Database  (Master only)"
$r = Post "$node1/db/drop" @{ db = "school" }
PrintResult "Drop DB 'school'" $r

Write-Host ""
Write-Host "All tests complete." -ForegroundColor Cyan
