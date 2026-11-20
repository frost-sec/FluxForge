param(
    [Parameter(Mandatory, Position=0)] [string]$OutputDirectory,
    [string]$BuilderImage = 'fluxforge-zygote-builder:dev',
    [string]$PlotImage = 'fluxforge-benchmark-plot:dev'
)
$ErrorActionPreference = 'Stop'

$destination = $ExecutionContext.SessionState.Path.GetUnresolvedProviderPathFromPSPath($OutputDirectory)
$measurementFile = Join-Path $destination 'measurements.json'
if (Test-Path -LiteralPath $measurementFile) { throw 'Choose a new output directory for each benchmark run.' }

$imageId = & docker image inspect --format '{{.Id}}' $BuilderImage
if ($LASTEXITCODE -ne 0) { throw 'Build the Runner builder image before measuring.' }
& docker image inspect --format '{{.Id}}' $PlotImage | Out-Null
if ($LASTEXITCODE -ne 0) { throw 'Build the plotting image before measuring.' }

$measure = 'set -eu; mkdir /tmp/zygote-bench; c++ -O2 -std=c++20 -I/workspace/runner/include -I/build/generated /benchmark/measure.cpp /build/libff_worker_proto.a -lprotobuf -pthread -o /tmp/zygote-bench/measure; timeout 60s /tmp/zygote-bench/measure /tmp/zygote-bench'
$data = & docker run --rm --network none --user 65532:65532 --init --cap-drop ALL `
    --security-opt no-new-privileges:true --pids-limit 128 --memory 1g --cpus 2 --read-only `
    --tmpfs /tmp:rw,exec,nosuid,nodev,mode=1777 `
    --mount "type=bind,source=$PSScriptRoot,target=/benchmark,readonly" `
    -e "FF_BENCH_IMAGE_ID=$imageId" $BuilderImage bash -c $measure
if ($LASTEXITCODE -ne 0) { throw 'Zygote measurement failed.' }
$raw = $data -join "`n"
$raw | ConvertFrom-Json | Out-Null
New-Item -ItemType Directory -Path $destination -Force | Out-Null
[System.IO.File]::WriteAllText($measurementFile, $raw + "`n")

& docker run --rm --network none --mount "type=bind,source=$destination,target=/results" $PlotImage /results
if ($LASTEXITCODE -ne 0) { throw 'Plotting failed. Raw measurements are available in the output directory.' }
Write-Output $destination
