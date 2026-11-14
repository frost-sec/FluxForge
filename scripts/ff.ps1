param(
    [Parameter(Position=0)]
    [ValidateSet('tools','generate','build','up','down','test','integration','demo','verify','cli','status','logs','function-build')]
    [string]$Command = 'status',
    [Parameter(Position=1,ValueFromRemainingArguments=$true)]
    [string[]]$Arguments
)
$ErrorActionPreference = 'Stop'
$projectRoot = Split-Path -Parent $PSScriptRoot
function Invoke-Docker([string[]]$DockerArgs) {
    & docker @DockerArgs
    if ($LASTEXITCODE -ne 0) { throw "Docker command failed with exit code $LASTEXITCODE" }
}
function Tools {
    Invoke-Docker @('build','-f','deploy/Dockerfile.tools.go','-t','fluxforge-go-tools:dev','.')
    # The legacy developer image is built from the existing Ubuntu recipe.
    Invoke-Docker @('build','-f','deploy/Dockerfile.runner.dev','-t','fluxforge-runner:latest','.')
    Invoke-Docker @('build','-f','deploy/Dockerfile.tools.runner','-t','fluxforge-toolchain:dev','.')
}
function Generate {
    Invoke-Docker @('run','--rm','--mount',"type=bind,source=$projectRoot,target=/workspace",'fluxforge-go-tools:dev','bash','/workspace/scripts/codegen.sh')
    Invoke-Docker @('run','--rm','--mount',"type=bind,source=$projectRoot,target=/workspace",'fluxforge-toolchain:dev','bash','-c','mkdir -p /workspace/runner/include/proto && protoc -I/workspace/proto --cpp_out=/workspace/runner/include/proto --grpc_out=/workspace/runner/include/proto --plugin=protoc-gen-grpc=/usr/bin/grpc_cpp_plugin /workspace/proto/fluxforge_v1.proto')
}
Push-Location $projectRoot
try {
    switch ($Command) {
        'tools' { Tools }
        'generate' { Generate }
        'build' {
            Tools
            Generate
            Invoke-Docker @('build','-f','deploy/Dockerfile.runner','-t','fluxforge-runner-runtime:dev','.')
            Invoke-Docker @('build','-f','deploy/Dockerfile.function-sdk','-t','fluxforge-function-sdk:dev','.')
            Invoke-Docker @('build','-f','deploy/Dockerfile.control','--target','runtime','-t','fluxforge-control-runtime:dev','.')
            Invoke-Docker @('build','-f','deploy/Dockerfile.control','--target','ffctl','-t','fluxforge-cli:dev','.')
        }
        'up' {
            Invoke-Docker @('compose','-f','compose.yml','up','-d','postgres','minio','control-plane')
            $deployment = (& docker compose -f compose.yml config --format json | ConvertFrom-Json)
            if ($deployment.services.'control-plane'.environment.AUTOSCALING_ENABLED -eq 'true') { Invoke-Docker @('compose','-f','compose.yml','--profile','tools','run','--rm','--no-deps','ffctl','pool','resume') }
        }
        'down' {
            Invoke-Docker @('compose','-f','compose.yml','--profile','tools','run','--rm','--no-deps','ffctl','pool','shutdown','--timeout','45s')
            Invoke-Docker @('compose','-f','compose.yml','stop','control-plane')
            $deployment = (& docker compose -f compose.yml config --format json | ConvertFrom-Json)
            $ownedContainers = @(& docker ps -aq --filter 'label=io.fluxforge.managed=true' --filter "label=io.fluxforge.project=$($deployment.name)")
            if ($LASTEXITCODE -ne 0) { throw 'Cannot inspect owned runners' }
            foreach ($containerId in $ownedContainers) {
                if ($containerId -notmatch '^[a-f0-9]{12,64}$') { throw 'Unexpected Docker container ID' }
                Invoke-Docker @('stop','--time','5',$containerId)
                Invoke-Docker @('rm',$containerId)
            }
            Invoke-Docker @('compose','-f','compose.yml','down')
        }
        'test' {
            Invoke-Docker @('run','--rm','--mount',"type=bind,source=$projectRoot,target=/workspace",'--mount','type=volume,source=fluxforge-go-mod,target=/go/pkg/mod','--mount','type=volume,source=fluxforge-go-build,target=/root/.cache/go-build','fluxforge-go-tools:dev','bash','-c','bash /workspace/scripts/codegen.sh && go test -race ./...')
            Invoke-Docker @('run','--rm','--mount',"type=bind,source=$projectRoot,target=/workspace",'fluxforge-toolchain:dev','bash','-c','cmake -S /workspace/runner -B /workspace/runner/build -DFF_BUILD_AGENT=ON -DBUILD_TESTING=ON -DCMAKE_BUILD_TYPE=Debug && cmake --build /workspace/runner/build -j 4 && ctest --test-dir /workspace/runner/build --output-on-failure')
        }
        'demo' { Invoke-Docker @('compose','-f','compose.yml','--profile','tools','run','--rm','--no-deps','ffctl','demo') }
        'verify' { Invoke-Docker @('compose','-f','compose.yml','--profile','tools','run','--rm','--no-deps','ffctl','verify') }
        'integration' { Invoke-Docker @('compose','-f','compose.yml','--profile','tools','run','--rm','--no-deps','go-test') }
        'cli' { Invoke-Docker (@('compose','-f','compose.yml','--profile','tools','run','--rm','--no-deps','ffctl') + $Arguments) }
        'status' { Invoke-Docker @('compose','-f','compose.yml','ps') }
        'logs' { Invoke-Docker (@('compose','-f','compose.yml','logs','--tail','100') + $Arguments) }
        'function-build' {
            if ($Arguments.Count -ne 1) { throw 'Usage: ./scripts/ff.ps1 function-build path/to/function.cpp' }
            $sourcePath = (Resolve-Path -LiteralPath $Arguments[0]).Path
            $sourceDir = Split-Path -Parent $sourcePath
            $sourceName = Split-Path -Leaf $sourcePath
            Invoke-Docker @('run','--rm','--mount',"type=bind,source=$projectRoot,target=/workspace",'--mount',"type=bind,source=$sourceDir,target=/function",'-e',"FF_SOURCE=$sourceName",'fluxforge-toolchain:dev','bash','-c','g++ -std=c++20 -O2 -fPIC -shared -I/workspace/sdk/cpp/include "/function/$FF_SOURCE" -o /function/function.so')
        }
    }
} finally { Pop-Location }
